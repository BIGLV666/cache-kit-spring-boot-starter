package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.channel.RedisChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 失效广播多实例端到端（自管 Redis 容器）：一个实例 evictBatch，其余实例的 L1 必须
 * 在时限内全部清除（零丢失）。streams 模式 3 实例 × 500 键、pub/sub 模式 2 实例 × 200 键。
 *
 * <p>判定用各实例自己的 L1（CaffeineChannel），不查 L2——evictBatch 本身会删共享 L2，
 * 只有"广播送达且本地应用"才能解释 L1 的清除。</p>
 */
class InvalidationBroadcastMultiInstanceIntegrationTest {

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate template;
    private static EntityMetadataRegistry registry;
    private static EntityMetadata meta;
    private static final int STREAM_IDS = 500;
    private static final int PUBSUB_IDS = 200;

    @BeforeAll
    static void startRedis() {
        boolean dockerAvailable;
        try {
            dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            dockerAvailable = false;
        }
        Assumptions.assumeTrue(dockerAvailable, "无 Docker 环境，跳过多实例广播测试");
        try {
            redis = new GenericContainer<>("redis:7").withExposedPorts(6379);
            redis.start();
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Redis 容器启动失败，跳过: " + e.getMessage());
        }
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379)));
        factory.afterPropertiesSet();
        template = new StringRedisTemplate(factory);
        template.afterPropertiesSet();
        registry = new EntityMetadataRegistry();
        meta = registry.require(UserEntity.class);
    }

    @AfterAll
    static void shutdown() {
        if (factory != null) {
            factory.destroy();
        }
    }

    /** 一个缓存实例：独立 L1（Caffeine）+ 共享 L2（Redis） */
    private record Instance(TieredEntityCache cache, CaffeineChannel l1) {
    }

    private static Instance instance(InvalidationPublisher publisher) {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        CaffeineChannel l1 = new CaffeineChannel(10_000);
        TieredEntityCache cache = new TieredEntityCache(props, l1, new RedisChannel(template), publisher,
                new DoubleDeleteScheduler(Duration.ofMillis(20)), "", List.of());
        return new Instance(cache, l1);
    }

    private static List<Object> preload(Instance instance, int n) {
        List<Object> ids = new ArrayList<>(n);
        for (long id = 1; id <= n; id++) {
            ids.add(id);
        }
        instance.cache().loadBatch(meta, ids, true, null,
                missing -> {
                    List<Object> out = new ArrayList<>();
                    for (Object id : missing) {
                        out.add(new UserEntity((Long) id, "v0"));
                    }
                    return out;
                });
        return ids;
    }

    private static String key(long id) {
        return meta.prefix() + ":" + id;
    }

    /** 轮询目标实例的 L1 全部清除；返回实际清除数（时限结束时） */
    private static int awaitAllEvicted(List<Instance> targets, List<Object> ids, long deadlineMillis) {
        long deadline = System.currentTimeMillis() + deadlineMillis;
        int lastCleared = -1;
        while (true) {
            int cleared = 0;
            for (Instance target : targets) {
                for (Object idObj : ids) { long id = (Long) idObj;
                    if (!target.l1().get(key(id)).hit()) {
                        cleared++;
                    }
                }
            }
            lastCleared = cleared;
            if (cleared == ids.size() * targets.size() || System.currentTimeMillis() > deadline) {
                return lastCleared;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return lastCleared;
            }
        }
    }

    @Test
    void streamsModeShouldDeliverToAllInstancesWithoutLoss() throws Exception {
        int instanceCount = 3;
        List<Instance> all = new ArrayList<>();
        // A（索引 0）持有 Streams 发布器；三个实例都挂消费者（真实拓扑）
        all.add(instance(new StreamsInvalidationPublisher(template, "cache-kit:test-stream",
                new CacheKitProperties().getBroadcast().getStreamsMaxlen())));
        for (int i = 1; i < instanceCount; i++) {
            all.add(instance(new NoopInvalidationPublisher()));
        }
        List<BroadcastApplier> appliers = new ArrayList<>();
        List<StreamsInvalidationConsumer> consumers = new ArrayList<>();
        // 滞后 gauge 数据源：消费者节流回调
        java.util.concurrent.atomic.AtomicInteger lagCalls = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicLong lastLag = new java.util.concurrent.atomic.AtomicLong(-1);
        CacheMetricsListener lagProbe = new CacheMetricsListener() {
            @Override
            public void streamsLagSeconds(long seconds) {
                lagCalls.incrementAndGet();
                lastLag.set(seconds);
            }
        };
        for (Instance inst : all) {
            BroadcastApplier applier = new BroadcastApplier(inst.l1(), registry, "");
            appliers.add(applier);
            StreamsInvalidationConsumer consumer = new StreamsInvalidationConsumer(template, applier,
                    "cache-kit:test-stream");
            consumer.setMetricsListener(lagProbe);
            consumer.start();
            consumers.add(consumer);
        }
        try {
            for (Instance inst : all) {
                preload(inst, STREAM_IDS);
            }

            long t0 = System.currentTimeMillis();
            all.get(0).cache().evictBatch(meta, preloadIds(STREAM_IDS));
            int cleared = awaitAllEvicted(all.subList(1, all.size()), preloadIds(STREAM_IDS), 30_000);
            long elapsed = System.currentTimeMillis() - t0;

            int expected = STREAM_IDS * (instanceCount - 1);
            System.out.printf("===== streams 多实例广播：%d 实例 × %d 键，送达 %d/%d，耗时 %dms =====%n",
                    instanceCount, STREAM_IDS, cleared, expected, elapsed);
            assertThat(cleared).as("其余实例 L1 必须全部清除（零丢失）").isEqualTo(expected);

            // 消费追平后滞后应回落到 0（轮询等待节流回调出现）
            long lagDeadline = System.currentTimeMillis() + 10_000;
            while (lastLag.get() != 0 && System.currentTimeMillis() < lagDeadline) {
                Thread.sleep(200);
            }
            assertThat(lagCalls.get()).as("滞后量测应被周期触发").isPositive();
            assertThat(lastLag.get()).as("全部送达后消费组滞后应追平为 0").isEqualTo(0);
        } finally {
            consumers.forEach(StreamsInvalidationConsumer::stop);
        }
    }

    private static List<Object> preloadIds(int n) {
        List<Object> ids = new ArrayList<>(n);
        for (long id = 1; id <= n; id++) {
            ids.add(id);
        }
        return ids;
    }

    @Test
    void pubsubModeShouldDeliverToAllInstances() {
        RedisInvalidationPublisher publisher =
                new RedisInvalidationPublisher(template, "cache-kit:test-pubsub");
        Instance a = instance(publisher);
        Instance b = instance(new NoopInvalidationPublisher());

        List<org.springframework.data.redis.listener.RedisMessageListenerContainer> containers = new ArrayList<>();
        try {
            for (Instance inst : List.of(a, b)) {
                InvalidationSubscriber subscriber = new InvalidationSubscriber(inst.l1(), registry, "");
                org.springframework.data.redis.listener.RedisMessageListenerContainer container =
                        new org.springframework.data.redis.listener.RedisMessageListenerContainer();
                container.setConnectionFactory(factory);
                container.afterPropertiesSet();
                container.addMessageListener(subscriber,
                        new org.springframework.data.redis.listener.ChannelTopic("cache-kit:test-pubsub"));
                container.start();
                containers.add(container);
            }
            preload(a, PUBSUB_IDS);
            preload(b, PUBSUB_IDS);

            long t0 = System.currentTimeMillis();
            a.cache().evictBatch(meta, preloadIds(PUBSUB_IDS));
            int cleared = awaitAllEvicted(List.of(b), preloadIds(PUBSUB_IDS), 20_000);
            long elapsed = System.currentTimeMillis() - t0;

            System.out.printf("===== pub/sub 多实例广播：2 实例 × %d 键，送达 %d/%d，耗时 %dms =====%n",
                    PUBSUB_IDS, cleared, PUBSUB_IDS, elapsed);
            assertThat(cleared).as("另一实例 L1 必须全部清除").isEqualTo(PUBSUB_IDS);
        } finally {
            for (org.springframework.data.redis.listener.RedisMessageListenerContainer container : containers) {
                try {
                    container.destroy();
                } catch (Exception ignored) {
                    // 测试清理：销毁失败无需处理
                }
            }
        }
    }

    /**
     * sharded pub/sub 模式（Redis 7 SSUBSCRIBE/SSEND，Lettuce 原生）：发布端 SSEND、
     * 订阅端独立连接 SSUBSCRIBE，语义与 pub/sub 一致。单机 Redis 7 下验证端到端送达，
     * 且断言订阅器运行在 sharded 模式（未静默回退）。
     */
    @Test
    void shardedPubsubModeShouldDeliverToAllInstances() {
        String topic = "cache-kit:test-sharded-pubsub";
        ShardedInvalidationPublisher publisher = new ShardedInvalidationPublisher(factory, topic);
        Instance a = instance(publisher);
        Instance b = instance(new NoopInvalidationPublisher());

        List<ShardedInvalidationSubscriber> subscribers = new ArrayList<>();
        try {
            for (Instance inst : List.of(a, b)) {
                BroadcastApplier applier = new BroadcastApplier(inst.l1(), registry, "");
                ShardedInvalidationSubscriber subscriber = new ShardedInvalidationSubscriber(factory, applier, topic);
                subscriber.start();
                subscribers.add(subscriber);
            }
            // 若意外回退（Redis 不支持 SSUBSCRIBE），断言失败暴露回退而非静默降级
            assertThat(subscribers).allSatisfy(s ->
                    assertThat(s.isShardedActive()).as("Redis 7 下订阅器必须运行在 sharded 模式").isTrue());

            preload(a, PUBSUB_IDS);
            preload(b, PUBSUB_IDS);

            long t0 = System.currentTimeMillis();
            a.cache().evictBatch(meta, preloadIds(PUBSUB_IDS));
            int cleared = awaitAllEvicted(List.of(b), preloadIds(PUBSUB_IDS), 20_000);
            long elapsed = System.currentTimeMillis() - t0;

            System.out.printf("===== sharded pub/sub 多实例广播：2 实例 × %d 键，送达 %d/%d，耗时 %dms =====%n",
                    PUBSUB_IDS, cleared, PUBSUB_IDS, elapsed);
            assertThat(cleared).as("另一实例 L1 必须全部清除").isEqualTo(PUBSUB_IDS);
        } finally {
            subscribers.forEach(ShardedInvalidationSubscriber::stop);
        }
    }
}
