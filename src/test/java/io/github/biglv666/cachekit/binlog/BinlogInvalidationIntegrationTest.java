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

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * binlog 直连失效端到端测试：真实 MySQL（binlog 开启、ROW 格式，容器 cache-kit-mysql:3307）。
 *
 * <p>关键场景：绕过应用直接改库（JdbcTemplate UPDATE，无 @CacheInvalidate、无广播），
 * binlog 监听必须把对应实体的 L1/L2 失效，让下一次读拿到新值——
 * 这补上了广播机制的唯一盲区（DBA 改库、其他服务写入）。</p>
 */
class BinlogInvalidationIntegrationTest {

    @Configuration
    @MapperScan("io.github.biglv666.cachekit.binlog")
    static class MappersConfig {
    }

    @BeforeAll
    static void requireMysql() {
        boolean reachable;
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", 3307), 500);
            reachable = true;
        } catch (Exception e) {
            reachable = false;
        }
        Assumptions.assumeTrue(reachable, "本地无带 binlog 的 MySQL(3307)，跳过测试");
    }

    @org.junit.jupiter.api.BeforeEach
    void cleanRedisL2Keys() {
        org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory factory =
                new org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory("localhost", 6379);
        factory.afterPropertiesSet();
        try {
            org.springframework.data.redis.core.StringRedisTemplate template =
                    new org.springframework.data.redis.core.StringRedisTemplate(factory);
            template.afterPropertiesSet();
            java.util.Set<String> keys = template.keys("user_bin:*");
            if (keys != null && !keys.isEmpty()) {
                template.delete(keys);
            }
        } finally {
            factory.destroy();
        }
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
                        "spring.datasource.url=jdbc:mysql://localhost:3307/cachekit_test",
                        "spring.datasource.username=root",
                        "spring.datasource.password=root",
                        "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
                        "spring.data.redis.host=localhost",
                        "spring.data.redis.port=6379",
                        "cache-kit.l1.ttl=60s",
                        "cache-kit.l2.ttl=120s",
                        "cache-kit.l2.jitter=1ms",
                        "cache-kit.binlog.enabled=true",
                        // 显式指定，验证属性直配路径
                        "cache-kit.binlog.host=localhost",
                        "cache-kit.binlog.port=3307",
                        "cache-kit.binlog.database=cachekit_test",
                        "cache-kit.binlog.username=root",
                        "cache-kit.binlog.password=root",
                        "cache-kit.binlog.server-id=18365");
    }

    @Test
    void outOfBandWriteShouldBeInvalidatedByBinlog() throws Exception {
        runner().run(ctx -> {
            JdbcTemplate jdbc = ctx.getBean(JdbcTemplate.class);
            jdbc.execute("DROP TABLE IF EXISTS user_bin");
            jdbc.execute("CREATE TABLE user_bin (user_id BIGINT PRIMARY KEY, username VARCHAR(64))");
            jdbc.update("INSERT INTO user_bin VALUES (1, 'v0')");

            BinTestUserMapper mapper = ctx.getBean(BinTestUserMapper.class);

            // 1) 首读：缓存 v0
            assertThat(mapper.selectById(1L).getUsername()).isEqualTo("v0");

            // 2) 绕过应用直接改库（无 @CacheInvalidate、无广播）——广播机制的盲区
            jdbc.update("UPDATE user_bin SET username='v1' WHERE user_id=1");

            // 3) binlog 行事件应触发失效：轮询等待事件到达 + 失效传播（通常 <1s）
            long deadline = System.currentTimeMillis() + 10_000;
            String observed;
            while (true) {
                observed = mapper.selectById(1L).getUsername();
                if ("v1".equals(observed) || System.currentTimeMillis() > deadline) {
                    break;
                }
                Thread.sleep(200);
            }
            assertThat(observed).as("binlog 失效后必须读到新值").isEqualTo("v1");
        });
    }

    /**
     * 性能：直写 → binlog 失效 → 下次读到新值 的端到端传播延迟分布。
     * 300 次直写，每次间隔 50ms；传播延迟含轮询粒度（5ms）。
     */
    @Test
    void invalidationPropagationLatencyBenchmark() throws Exception {
        runner().run(ctx -> {
            JdbcTemplate jdbc = ctx.getBean(JdbcTemplate.class);
            jdbc.execute("DROP TABLE IF EXISTS user_bin");
            jdbc.execute("CREATE TABLE user_bin (user_id BIGINT PRIMARY KEY, username VARCHAR(64))");
            jdbc.update("INSERT INTO user_bin VALUES (1, 'v0')");

            BinTestUserMapper mapper = ctx.getBean(BinTestUserMapper.class);
            assertThat(mapper.selectById(1L).getUsername()).isEqualTo("v0");

            int rounds = 300;
            long[] latencies = new long[rounds];
            int misses = 0;
            for (int i = 1; i <= rounds; i++) {
                String v = "v" + i;
                jdbc.update("UPDATE user_bin SET username='" + v + "' WHERE user_id=1");
                long t0 = System.nanoTime();
                // 5ms 粒度轮询，直到读到新值
                String observed;
                do {
                    Thread.sleep(5);
                    observed = mapper.selectById(1L).getUsername();
                } while (!v.equals(observed) && System.nanoTime() - t0 < 5_000_000_000L);
                latencies[i - 1] = System.nanoTime() - t0;
                if (!v.equals(observed)) {
                    misses++;
                }
                Thread.sleep(50);
            }

            long[] sorted = latencies.clone();
            Arrays.sort(sorted);
            System.out.printf("===== binlog 失效传播延迟（%d 次直写，轮询粒度 5ms） =====%n", rounds);
            System.out.printf("avg=%.1fms p50=%.1fms p90=%.1fms p99=%.1fms max=%.1fms | 超时未失效 %d 次%n",
                    Arrays.stream(latencies).average().orElse(0) / 1e6,
                    sorted[(int) (rounds * 0.5)] / 1e6,
                    sorted[(int) (rounds * 0.9)] / 1e6,
                    sorted[(int) (rounds * 0.99)] / 1e6,
                    sorted[rounds - 1] / 1e6, misses);
            assertThat(misses).as("5s 内未失效的次数").isZero();
        });
    }
}
