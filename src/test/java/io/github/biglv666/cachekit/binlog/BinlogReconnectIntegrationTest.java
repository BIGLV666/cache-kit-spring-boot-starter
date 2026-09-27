package io.github.biglv666.cachekit.binlog;

import com.baomidou.mybatisplus.autoconfigure.MybatisPlusAutoConfiguration;
import io.github.biglv666.cachekit.config.CacheKitAutoConfiguration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Testcontainers 端到端测试（自管容器，不依赖 CI 预起服务）：binlog 断连后的失效链路恢复。
 *
 * <p>两个场景补 {@link BinlogInvalidationIntegrationTest} 的盲区：
 * 1) 断线重连回放——MySQL 重启断连，断连窗口内/后的写入经 binlog 续传回放后仍失效成功；
 * 2) 位点被清理兜底——dump 连接被 KILL 后 PURGE 掉位点文件，重连报错秒断，
 *    BinlogLifecycle 达到阈值后重置为最新位点，失效监听自动恢复。</p>
 */
class BinlogReconnectIntegrationTest {

    @Configuration
    @MapperScan("io.github.biglv666.cachekit.binlog")
    static class MappersConfig {
    }

    private static MySQLContainer<?> mysql;
    private static GenericContainer<?> redis;

    @BeforeAll
    static void startContainers() {
        boolean dockerAvailable;
        try {
            dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            dockerAvailable = false;
        }
        Assumptions.assumeTrue(dockerAvailable, "无 Docker 环境，跳过 Testcontainers 端到端测试");

        // 镜像预拉取失败（离线环境）也应跳过而非失败
        try {
            mysql = new MySQLContainer<>("mysql:8.4")
                    .withDatabaseName("cachekit_test")
                    .withUsername("root")
                    .withPassword("root")
                    .withCommand("mysqld", "--log-bin=mysql-bin", "--binlog-format=ROW", "--server-id=1");
            mysql.start();
            redis = new GenericContainer<>("redis:7").withExposedPorts(6379);
            redis.start();
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "容器启动失败（镜像不可用？），跳过: " + e.getMessage());
        }
    }

    @org.junit.jupiter.api.BeforeEach
    void cleanStaleL2Entries() throws Exception {
        // 共享 Redis 容器：上个用例遗留的 user_bin:* L2 缓存会串场（DROP TABLE 不触发失效）
        redis.execInContainer("redis-cli", "FLUSHALL");
    }

    /** KILL 所有 Binlog Dump 线程，制造断连 */
    private static void killDumpThreads(JdbcTemplate jdbc) {
        jdbc.query("SELECT id FROM information_schema.processlist WHERE command LIKE 'Binlog Dump%'", rs -> {
            long threadId = rs.getLong(1);
            jdbc.execute("KILL " + threadId);
        });
    }

    /** KILL dump 连接并在重连（3s 间隔）前 PURGE 掉位点文件，使重连报 "Could not find first log file name" 秒断 */
    private static void killDumpThreadAndPurge(JdbcTemplate jdbc) {
        killDumpThreads(jdbc);
        jdbc.execute("FLUSH BINARY LOGS");
        jdbc.execute("PURGE BINARY LOGS BEFORE NOW()");
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        DataSourceAutoConfiguration.class,
                        JdbcTemplateAutoConfiguration.class,
                        MybatisPlusAutoConfiguration.class,
                        RedisAutoConfiguration.class,
                        AopAutoConfiguration.class,
                        CacheKitAutoConfiguration.class))
                .withUserConfiguration(MappersConfig.class)
                .withPropertyValues(
                        "spring.datasource.url=jdbc:mysql://" + mysql.getHost() + ":" + mysql.getMappedPort(3306)
                                + "/" + mysql.getDatabaseName(),
                        "spring.datasource.username=" + mysql.getUsername(),
                        "spring.datasource.password=" + mysql.getPassword(),
                        "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
                        "spring.data.redis.host=" + redis.getHost(),
                        "spring.data.redis.port=" + redis.getMappedPort(6379),
                        "cache-kit.l1.ttl=60s",
                        "cache-kit.l2.ttl=120s",
                        "cache-kit.l2.jitter=1ms",
                        "cache-kit.binlog.enabled=true",
                        "cache-kit.binlog.host=" + mysql.getHost(),
                        "cache-kit.binlog.port=" + mysql.getMappedPort(3306),
                        "cache-kit.binlog.database=" + mysql.getDatabaseName(),
                        "cache-kit.binlog.username=" + mysql.getUsername(),
                        "cache-kit.binlog.password=" + mysql.getPassword(),
                        "cache-kit.binlog.server-id=18366");
    }

    /**
     * 断线重连回放：KILL dump 连接制造断连，断连窗口内的写入在重连后经位点续传回放，
     * 必须触发失效。若重连不复读旧位点，该写入不会失效，L1（60s TTL）会一直返回旧值。
     */
    @Test
    void writesDuringDisconnectShouldBeReplayedAfterReconnect() throws Exception {
        runner().run(ctx -> {
            JdbcTemplate jdbc = ctx.getBean(JdbcTemplate.class);
            jdbc.execute("DROP TABLE IF EXISTS user_bin");
            jdbc.execute("CREATE TABLE user_bin (user_id BIGINT PRIMARY KEY, username VARCHAR(64))");
            jdbc.update("INSERT INTO user_bin VALUES (1, 'v0')");

            BinTestUserMapper mapper = ctx.getBean(BinTestUserMapper.class);
            assertThat(mapper.selectById(1L).getUsername()).isEqualTo("v0");

            // 断连窗口内的写入（重连间隔 3s，赶在下一次重连之前执行）
            killDumpThreads(jdbc);
            jdbc.update("UPDATE user_bin SET username='v1' WHERE user_id=1");

            long deadline = System.currentTimeMillis() + 30_000;
            String observed = "v0";
            while (System.currentTimeMillis() < deadline) {
                observed = mapper.selectById(1L).getUsername();
                if ("v1".equals(observed)) {
                    break;
                }
                Thread.sleep(500);
            }
            assertThat(observed).as("重连回放后必须读到新值").isEqualTo("v1");
        });
    }

    /**
     * 位点清理兜底：KILL dump 连接后 PURGE 位点文件 → 重连秒断循环 →
     * BinlogLifecycle 达到阈值（默认 5 轮 × 3s）重置最新位点 → 失效恢复。
     */
    @Test
    void purgedPositionShouldResetToLatestAndResumeInvalidation() throws Exception {
        runner().run(ctx -> {
            JdbcTemplate jdbc = ctx.getBean(JdbcTemplate.class);
            jdbc.execute("DROP TABLE IF EXISTS user_bin");
            jdbc.execute("CREATE TABLE user_bin (user_id BIGINT PRIMARY KEY, username VARCHAR(64))");
            jdbc.update("INSERT INTO user_bin VALUES (1, 'v0')");

            BinTestUserMapper mapper = ctx.getBean(BinTestUserMapper.class);
            assertThat(mapper.selectById(1L).getUsername()).isEqualTo("v0");

            // KILL binlog dump 线程制造断连，并在重连（3s 间隔）前 PURGE 掉位点文件，
            // 使重连报 "Could not find first log file name" 秒断
            killDumpThreadAndPurge(jdbc);

            long deadline = System.currentTimeMillis() + 45_000;
            String observed = "v0";
            jdbc.update("UPDATE user_bin SET username='v1' WHERE user_id=1");
            while (System.currentTimeMillis() < deadline) {
                observed = mapper.selectById(1L).getUsername();
                if ("v1".equals(observed)) {
                    break;
                }
                Thread.sleep(1_000);
            }
            assertThat(observed).as("位点重置后失效监听必须恢复").isEqualTo("v1");
        });
    }
}
