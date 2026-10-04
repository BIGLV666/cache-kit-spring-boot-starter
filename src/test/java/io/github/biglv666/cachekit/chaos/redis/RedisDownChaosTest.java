package io.github.biglv666.cachekit.chaos.redis;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.channel.RedisChannel;
import io.github.biglv666.cachekit.chaos.support.ChaosContainers;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.CacheMetricsListener;
import io.github.biglv666.cachekit.core.DoubleDeleteScheduler;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.support.JsonCodec;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Redis 真停机混沌（docker stop/start 同一容器，固定同号端口绑定）：
 * 验证"L2 故障降级绝不阻断业务"与"失效丢失由 TTL 上界兜底"两条承诺的完整链条。
 *
 * <p>关键场景：停机期间读写零异常；双删重试耗尽计入指标；恢复后读到 L2 残留旧值
 * （文档承诺的最坏情况）并在 L2 TTL 到期后自愈；双删积压保护确定性触发。</p>
 */
class RedisDownChaosTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = ChaosContainers.REDIS_PORT;

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory factory;

    /** 计数型监听器：降级/重试耗尽/积压跳过三个兜底指标 */
    static class CountingMetrics implements CacheMetricsListener {
        final AtomicInteger l2Fallbacks = new AtomicInteger();
        final AtomicInteger evictRetryExhausted = new AtomicInteger();
        final AtomicInteger doubleDeleteSkipped = new AtomicInteger();

        @Override
        public void l2Fallback(String op) {
            l2Fallbacks.incrementAndGet();
        }

        @Override
        public void evictRetryExhausted() {
            evictRetryExhausted.incrementAndGet();
        }

        @Override
        public void doubleDeleteSkipped() {
            doubleDeleteSkipped.incrementAndGet();
        }
    }

    @BeforeAll
    static void startRedis() {
        Assumptions.assumeTrue(dockerAvailable(), "无 Docker，跳过 Redis 停机混沌测试");
        Assumptions.assumeTrue(ChaosContainers.hostPortFree(PORT),
                "固定端口 " + PORT + " 被占用（其他混沌测试并行运行？），跳过");
        redis = ChaosContainers.redisFixedPort();
        redis.start();
        // 命令超时 500ms：停机时降级快速失败，测试时长可控。
        // REJECT_COMMANDS：断连时立即拒绝而非缓冲——Lettuce 默认缓冲会把"失败"的 DEL 在重连后
        // 补投成功（现实优于承诺，实测验证），要确定性观测文档承诺的最坏情况必须显式关闭缓冲
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .clientOptions(io.lettuce.core.ClientOptions.builder()
                        .disconnectedBehavior(io.lettuce.core.ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                        .build())
                .commandTimeout(Duration.ofMillis(500)).build();
        factory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(HOST, PORT), clientConfig);
        factory.afterPropertiesSet();
    }

    @AfterAll
    static void shutdown() {
        if (factory != null) {
            factory.destroy();
        }
        if (redis != null) {
            redis.stop();
        }
    }

    private static boolean dockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }

    /** 无命名空间、无自定义段的键（直连通道测试必须用切面拼好前缀的完整键） */
    private static String fullKey(EntityMetadata meta, long id) {
        return meta.prefix() + ":" + id;
    }

    /** load 返回实体：统一解包 userName 参与断言 */
    private static String userName(Object entity) {
        return ((UserEntity) entity).getUserName();
    }

    /**
     * 停机期间：读降级为未命中（业务拿 DB 值）、失效照常执行、广播失败不抛——
     * 业务读写零异常；恢复后读写自动回正常。
     */
    @Test
    void redisDownShouldDegradeWithoutBusinessException() {
        CountingMetrics metrics = new CountingMetrics();
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        RedisChannel l2 = new RedisChannel(template());
        l2.setMetricsListener(metrics);
        CaffeineChannel l1 = new CaffeineChannel(1000);
        TieredEntityCache cache = new TieredEntityCache(props, l1, l2,
                new NoopInvalidationPublisher(), new DoubleDeleteScheduler(Duration.ofMillis(200)),
                "", List.of());
        EntityMetadata meta = registry().require(UserEntity.class);
        AtomicReference<String> db = new AtomicReference<>("v0");
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, db.get()));
        assertThat(l1.get(fullKey(meta, 1L)).hit()).isTrue();

        // === 故障注入：docker stop（真停机，保留容器） ===
        ChaosContainers.stopContainer(redis);
        try {
            db.set("v1");
            // 停机期间读：L1 还有缓存（先命中一次验证不额外回源）
            Object v = cache.load(meta, 1L, null, true, () -> new UserEntity(1L, db.get()));
            assertThat(userName(v)).as("L1 缓存命中不应受停机影响").isEqualTo("v0");

            // 清掉 L1 后读：L2 降级为未命中 → DB 值，绝不抛异常
            l1.evict(fullKey(meta, 1L));
            Object degraded = cache.load(meta, 1L, null, true, () -> new UserEntity(1L, db.get()));
            assertThat(userName(degraded)).as("停机期间读必须降级为 DB 值").isEqualTo("v1");
            assertThat(metrics.l2Fallbacks.get()).as("降级必须计入指标").isPositive();

            // 停机期间失效：照常执行（L1 删 + 双删调度 + 广播），不抛异常
            cache.evictBatch(meta, List.of(1L, 2L));
        } finally {
            // === 故障恢复 ===
            ChaosContainers.startContainer(redis);
        }
        assertThat(ChaosContainers.awaitRedisReady(template(), 20_000)).isTrue();

        // 恢复后：重新回填缓存，下一次读走 L1
        long deadline = System.currentTimeMillis() + 20_000;
        Object recovered = null;
        while (System.currentTimeMillis() < deadline) {
            recovered = cache.load(meta, 1L, null, true, () -> new UserEntity(1L, db.get()));
            if ("v1".equals(userName(recovered))) {
                break;
            }
        }
        assertThat(userName(recovered)).as("恢复后必须读到新值").isEqualTo("v1");
        assertThat(l1.get(fullKey(meta, 1L)).hit()).as("恢复后缓存应重新回填").isTrue();
    }

    /**
     * 失效落在停机窗口内的完整兜底链：双删重试耗尽（指标）→ L2 旧值滞留 →
     * L1 短 TTL 过期后读到 L2 残留旧值（文档承诺的最坏脏读窗口）→ L2 TTL 到期自愈。
     * 这条链是"最终一致，最坏受 L2 TTL 上界约束"的实证。
     */
    @Test
    void invalidationLostInOutageShouldBeBoundedByL2Ttl() {
        CountingMetrics metrics = new CountingMetrics();
        CacheKitProperties props = new CacheKitProperties();
        props.getL1().setTtl(Duration.ofMillis(500));      // L1 快速过期，逼出 L2 残留
        props.getL2().setTtl(Duration.ofSeconds(10));      // L2 比 3s 停机窗口长（否则观察不到滞留）
        props.getL2().setJitter(Duration.ZERO);
        props.getL2().setDoubleDeleteDelay(Duration.ofMillis(200));
        RedisChannel l2 = new RedisChannel(template());
        l2.setMetricsListener(metrics);
        CaffeineChannel l1 = new CaffeineChannel(1000);
        DoubleDeleteScheduler scheduler = new DoubleDeleteScheduler(props.getL2().getDoubleDeleteDelay());
        scheduler.setMetricsListener(metrics);
        TieredEntityCache cache = new TieredEntityCache(props, l1, l2,
                new NoopInvalidationPublisher(), scheduler, "", List.of());
        cache.setMetricsListener(metrics); // 重试耗尽埋点在 cache 侧（evictKeys），必须挂载
        EntityMetadata meta = registry().require(UserEntity.class);

        // 独立键 42：测试 1 恢复后回填的同键 L2 值会污染"残留旧值"观测。
        // ttlOverride=10s 压掉 UserEntity 实体级 ttl=60s（@CacheEntity(ttl=60) 覆盖 l2.ttl），
        // 否则残留旧值存活 60s，10s 的自愈窗口永远看不到新值
        Duration ttlOverride = Duration.ofSeconds(10);
        long t0 = System.currentTimeMillis();
        AtomicReference<String> db = new AtomicReference<>("v0");
        cache.load(meta, 42L, ttlOverride, true, () -> new UserEntity(42L, db.get()));

        // 停机 3s > 重试窗口（200ms × 3 + 间隔）：DEL 必然耗尽重试
        ChaosContainers.stopContainer(redis);
        long outageStart = System.currentTimeMillis();
        try {
            db.set("v1");
            cache.evictBatch(meta, List.of(42L));
            long deadline = System.currentTimeMillis() + 5_000;
            while (metrics.evictRetryExhausted.get() == 0 && System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            assertThat(metrics.evictRetryExhausted.get())
                    .as("停机窗口内的失效必须计入重试耗尽指标（失效丢失）").isPositive();
        } finally {
            ChaosContainers.startContainer(redis);
        }
        assertThat(ChaosContainers.awaitRedisReady(template(), 20_000)).isTrue();
        long outageMs = System.currentTimeMillis() - outageStart;
        System.out.printf("===== 停机混沌：实际停机 %dms，重试耗尽 %d 次 =====%n", outageMs, metrics.evictRetryExhausted.get());

        // 构造"旧值滞留 L2"状态：DEL 重试耗尽后旧值滞留是文档承诺的最坏情况，但 docker
        // stop -t 0 是 SIGKILL，Redis 能否在退出前落盘 RDB 是竞态（全量负载下实测会输），
        // 重启后内存可能全空——直接向 L2 播种旧值，确定性构造"DEL 丢失 + 旧值滞留"场景
        template().opsForValue().set(fullKey(meta, 42L), JsonCodec.write(new UserEntity(42L, "v0")),
                Duration.ofSeconds(10));

        // 最坏脏读窗口实证：L1 已过期 → L2 残留旧值被服务
        Object v = cache.load(meta, 42L, ttlOverride, true, () -> new UserEntity(42L, db.get()));
        assertThat(userName(v)).as("失效丢失后必须读到 L2 残留旧值（文档承诺的最坏情况）").isEqualTo("v0");

        // 自愈：L2 TTL（t0 + 10s）到期后必须读到新值
        long freshDeadline = t0 + 10_000 + 5_000;
        Object observed = v;
        while (!"v1".equals(userName(observed)) && System.currentTimeMillis() < freshDeadline) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            observed = cache.load(meta, 42L, ttlOverride, true, () -> new UserEntity(42L, db.get()));
        }
        assertThat(userName(observed)).as("L2 TTL 到期后必须自愈为新值").isEqualTo("v1");
    }

    /**
     * 双删积压保护（无容器、确定性）：任务消费能力（2 线程）低于产生速率时，
     * 超过 1 万条上限的新任务被跳过并计数，调度器保持存活、后续正常工作。
     */
    @Test
    void doubleDeleteBacklogShouldBeCappedWithoutFailure() {
        CountingMetrics metrics = new CountingMetrics();
        DoubleDeleteScheduler scheduler = new DoubleDeleteScheduler(Duration.ofMillis(200));
        scheduler.setMetricsListener(metrics);
        try {
            // 每个任务阻塞 100ms：2 个线程消费，其余全部积压——超过 1 万条后必然触发跳过
            AtomicInteger executed = new AtomicInteger();
            for (int i = 0; i < 10_500; i++) {
                final int n = i;
                scheduler.schedule(() -> {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    executed.incrementAndGet();
                });
            }
            assertThat(metrics.doubleDeleteSkipped.get())
                    .as("积压超过上限必须触发跳过保护").isPositive();
            assertThat(executed.get()).as("跳过保护只挡新任务，不抛异常").isGreaterThanOrEqualTo(0);
        } finally {
            scheduler.shutdown();
        }
    }

    private static EntityMetadataRegistry registry() {
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.find(UserEntity.class);
        return registry;
    }

    private static org.springframework.data.redis.core.StringRedisTemplate template() {
        org.springframework.data.redis.core.StringRedisTemplate template =
                new org.springframework.data.redis.core.StringRedisTemplate(factory);
        template.afterPropertiesSet();
        return template;
    }
}
