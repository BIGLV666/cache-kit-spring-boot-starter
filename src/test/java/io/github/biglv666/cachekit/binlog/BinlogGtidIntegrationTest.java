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
 * binlog GTID 模式（cache-kit.binlog.gtid-enabled）端到端：
 * 1) fail-fast——gtid-enabled 但宿主无数据源且未配置 gtid-set 时拒绝启动
 * （防止静默落到 connector 默认"从最早可用事件回放"的启动失效风暴）；
 * 2) 真实 MySQL（GTID 开启）下直写失效生效，且 client 的 GTID 集已被设为起点。
 */
class BinlogGtidIntegrationTest {

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
        Assumptions.assumeTrue(dockerAvailable, "无 Docker 环境，跳过 GTID 端到端测试");
        try {
            mysql = new MySQLContainer<>("mysql:8.4")
                    .withDatabaseName("cachekit_test")
                    .withUsername("root")
                    .withPassword("root")
                    .withCommand("mysqld", "--log-bin=mysql-bin", "--binlog-format=ROW",
                            "--server-id=1", "--gtid-mode=ON", "--enforce-gtid-consistency=ON");
            mysql.start();
            redis = new GenericContainer<>("redis:7").withExposedPorts(6379);
            redis.start();
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "容器启动失败（镜像不可用？），跳过: " + e.getMessage());
        }
    }

    @Test
    void gtidEnabledWithoutDataSourceOrGtidSetShouldFailFast() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CacheKitAutoConfiguration.class))
                .withPropertyValues(
                        "spring.data.redis.host=localhost",
                        "spring.data.redis.port=6379",
                        "cache-kit.binlog.enabled=true",
                        "cache-kit.binlog.gtid-enabled=true",
                        "cache-kit.binlog.host=localhost",
                        "cache-kit.binlog.port=3308",
                        "cache-kit.binlog.database=x",
                        "cache-kit.binlog.username=root",
                        "cache-kit.binlog.password=root")
                .run(ctx -> {
                    assertThat(ctx).hasFailed();
                    assertThat(ctx.getStartupFailure()).hasMessageContaining("gtid_executed");
                });
    }

    @Test
    void outOfBandWriteShouldBeInvalidatedInGtidMode() throws Exception {
        runner().run(ctx -> {
            JdbcTemplate jdbc = ctx.getBean(JdbcTemplate.class);
            jdbc.execute("DROP TABLE IF EXISTS user_bin");
            jdbc.execute("CREATE TABLE user_bin (user_id BIGINT PRIMARY KEY, username VARCHAR(64))");
            jdbc.update("INSERT INTO user_bin VALUES (1, 'v0')");

            BinTestUserMapper mapper = ctx.getBean(BinTestUserMapper.class);
            assertThat(mapper.selectById(1L).getUsername()).isEqualTo("v0");

            // GTID 集已以 @@global.gtid_executed 为起点设置（非"从最早可用回放"）
            com.github.shyiko.mysql.binlog.BinaryLogClient client =
                    ctx.getBean(com.github.shyiko.mysql.binlog.BinaryLogClient.class);
            assertThat(client.getGtidSet()).as("GTID 起点应已设置").isNotBlank();

            jdbc.update("UPDATE user_bin SET username='v1' WHERE user_id=1");
            long deadline = System.currentTimeMillis() + 15_000;
            String observed = "v0";
            while (System.currentTimeMillis() < deadline) {
                observed = mapper.selectById(1L).getUsername();
                if ("v1".equals(observed)) {
                    break;
                }
                Thread.sleep(300);
            }
            assertThat(observed).as("GTID 模式下直写失效必须生效").isEqualTo("v1");
        });
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
                        "cache-kit.binlog.gtid-enabled=true",
                        "cache-kit.binlog.host=" + mysql.getHost(),
                        "cache-kit.binlog.port=" + mysql.getMappedPort(3306),
                        "cache-kit.binlog.database=" + mysql.getDatabaseName(),
                        "cache-kit.binlog.username=" + mysql.getUsername(),
                        "cache-kit.binlog.password=" + mysql.getPassword(),
                        "cache-kit.binlog.server-id=18367");
    }
}
