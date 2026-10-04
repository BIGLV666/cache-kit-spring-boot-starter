package io.github.biglv666.cachekit.chaos.broadcast;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.channel.CacheEntry;
import io.github.biglv666.cachekit.chaos.support.ChaosContainers;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.BroadcastApplier;
import io.github.biglv666.cachekit.core.DoubleDeleteScheduler;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.ShardedInvalidationPublisher;
import io.github.biglv666.cachekit.core.ShardedInvalidationSubscriber;
import io.github.biglv666.cachekit.core.StreamsInvalidationConsumer;
import io.github.biglv666.cachekit.core.StreamsInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
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
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 广播通道故障混沌（docker stop/start 真停机）：
 * ① pub/sub 停机窗口的失效丢失（fire-and-forget 语义）由 L1 TTL 上界兜底、恢复后自动重连续传；
 * ② sharded pub/sub 停机恢复后订阅自动重建、新失效照常送达（验证重订阅兜底链）；
 * ③ streams 消费者处理变慢时不丢消息（消费组补投，ACK 语义）。
 *
 * <p>pub/sub 的丢失窗口是文档承诺的语义（选型建议写明"可容忍 TTL 兜底才用默认 pub/sub"），
 * 这里验证的是丢失"有界"且"可自愈"，而不是不丢失。</p>
 */
class BroadcastOutageChaosTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 16381; // 本类独立固定端口（16379 留给停机类，避免 TIME_WAIT 竞争）

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate template;
    private static EntityMetadataRegistry registry;
    private static EntityMetadata meta;

    @BeforeAll
    static void startRedis() {
        Assumptions.assumeTrue(dockerAvailable(), "无 Docker，跳过广播故障混沌测试");
        Assumptions.assumeTrue(ChaosContainers.hostPortFree(PORT),
                "固定端口 " + PORT + " 被占用（其他混沌测试并行运行？），跳过");
        redis = ChaosContainers.redisFixedPort(PORT);
        redis.start();
        factory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(HOST, PORT));
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

    /** 目标实例：独立 L1 + short TTL（丢失窗口的兜底观测），共享 Redis */
    private record Instance(CaffeineChannel l1, TieredEntityCache cache) {
    }

    private static Instance instance(Duration l1Ttl) {
        CacheKitProperties props = new CacheKitProperties();
        props.getL1().setTtl(l1Ttl);
        props.getL2().setJitter(Duration.ZERO);
        CaffeineChannel l1 = new CaffeineChannel(10_000);
        TieredEntityCache cache = new TieredEntityCache(props, l1, null,
                new NoopInvalidationPublisher(), null, "", List.of());
        return new Instance(l1, cache);
    }

    private static void preload(Instance instance, long... ids) {
        for (long id : ids) {
            instance.cache().load(meta, id, null, true, () -> new UserEntity(id, "v0"));
        }
    }

    private static String fullKey(long id) {
        return meta.prefix() + ":" + id;
    }

    /** pub/sub 停机窗口：窗口内失效丢失（L1 脏值存活至 TTL 上界后自愈）+ 窗口后重连续传 */
    @Test
    void pubsubOutageWindowShouldBeBoundedByL1TtlThenReconnect() throws Exception {
        Instance a = instance(Duration.ofSeconds(30));
        Instance b = instance(Duration.ofSeconds(5)); // 目标实例 L1 TTL 5s：丢失窗口的兜底上界
        String topic = "chaos:pubsub-outage";
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        container.addMessageListener(
                new io.github.biglv666.cachekit.core.InvalidationSubscriber(b.l1(), registry, ""),
                new ChannelTopic(topic));
        container.afterPropertiesSet();
        container.start();
        try {
            preload(a, 1, 2);
            preload(b, 1, 2);
            io.github.biglv666.cachekit.core.RedisInvalidationPublisher publisher =
                    new io.github.biglv666.cachekit.core.RedisInvalidationPublisher(template, topic);

            // === 故障注入 ===
            ChaosContainers.stopContainer(redis);
            long outageStart = System.currentTimeMillis();
            try {
                // 停机窗口内失效：发布失败（fire-and-forget）、L2 DEL 失败——B 的 L1 不被通知
                a.cache().evictBatch(meta, List.of(1L));
            } finally {
                ChaosContainers.startContainer(redis);
            }
            assertThat(ChaosContainers.awaitRedisReady(template, 20_000)).isTrue();

            // 窗口内丢失的失效：B 仍供旧值（丢失窗口是 pub/sub 语义的一部分）
            Object staleEntity = b.cache().load(meta, 1L, null, true, () -> new UserEntity(1L, "v1"));
            String stale = staleEntity == null ? null : ((UserEntity) staleEntity).getUserName();
            System.out.printf("===== pub/sub 停机混沌：停机 %dms，窗口内失效丢失=%s（L1 TTL 5s 兜底） =====%n",
                    System.currentTimeMillis() - outageStart, "v0".equals(stale));
            assertThat(stale).as("停机窗口内的失效丢失（pub/sub fire-and-forget 语义）").isEqualTo("v0");

            // 兜底：L1 TTL 到期后必须读到新值（有界自愈）
            long freshDeadline = System.currentTimeMillis() + 10_000;
            Object observed = stale;
            while (!"v1".equals(observed) && System.currentTimeMillis() < freshDeadline) {
                Thread.sleep(300);
                Object e = b.cache().load(meta, 1L, null, true, () -> new UserEntity(1L, "v1"));
                observed = e == null ? null : ((UserEntity) e).getUserName();
            }
            assertThat(observed).as("丢失窗口必须由 L1 TTL 上界兜底自愈").isEqualTo("v1");

            // 重连：恢复后的新失效必须正常送达（订阅自动重连）
            a.cache().evictBatch(meta, List.of(2L));
            long reconnectDeadline = System.currentTimeMillis() + 20_000;
            boolean reconnected = false;
            while (System.currentTimeMillis() < reconnectDeadline) {
                if (!b.l1().get(fullKey(2)).hit()) {
                    reconnected = true;
                    break;
                }
                Thread.sleep(200);
            }
            assertThat(reconnected).as("恢复后订阅必须自动重连并送达新失效").isTrue();
        } finally {
            container.stop();
            container.destroy();
        }
    }

    /** sharded pub/sub 停机恢复：订阅自动重建（Lettuce 重连 + 周期重订阅兜底），新失效照常送达 */
    @Test
    void shardedSubscriberShouldResubscribeAfterRedisRestart() throws Exception {
        Instance b = instance(Duration.ofSeconds(30));
        String topic = "chaos:sharded-outage";
        ShardedInvalidationSubscriber subscriber =
                new ShardedInvalidationSubscriber(factory, new BroadcastApplier(b.l1(), registry, ""), topic);
        subscriber.start();
        try {
            assertThat(subscriber.isShardedActive()).isTrue();
            io.github.biglv666.cachekit.core.ShardedInvalidationPublisher publisher =
                    new ShardedInvalidationPublisher(factory, topic);
            preload(b, 3);

            // 基线：停机前送达正常
            publisher.publish(fullKey(3));
            long deadline = System.currentTimeMillis() + 10_000;
            while (b.l1().get(fullKey(3)).hit() && System.currentTimeMillis() < deadline) {
                Thread.sleep(100);
            }
            assertThat(b.l1().get(fullKey(3)).hit()).isFalse();
            preload(b, 3);

            // === 故障注入：停 2s（订阅连接被切断）后恢复 ===
            ChaosContainers.stopContainer(redis);
            Thread.sleep(2_000);
            ChaosContainers.startContainer(redis);
            assertThat(ChaosContainers.awaitRedisReady(template, 20_000)).isTrue();

            assertThat(subscriber.isShardedActive()).as("重连后订阅器必须仍处于 sharded 模式").isTrue();
            publisher.publish(fullKey(3));
            // Lettuce 自动重订阅即时生效；若依赖 30s 周期重订阅兜底也能送达，只慢不丢
            deadline = System.currentTimeMillis() + 40_000;
            boolean delivered = false;
            while (System.currentTimeMillis() < deadline) {
                if (!b.l1().get(fullKey(3)).hit()) {
                    delivered = true;
                    break;
                }
                Thread.sleep(300);
            }
            assertThat(delivered).as("停机恢复后 sharded 订阅必须自动重建并送达").isTrue();
        } finally {
            subscriber.stop();
        }
    }

    /** streams 慢消费者：处理变慢不丢消息（消费组待读 + ACK，全部补投） */
    @Test
    void streamsSlowConsumerShouldNotLoseInvalidations() throws Exception {
        Instance b = instance(Duration.ofSeconds(30));
        String stream = "chaos:streams-outage";
        // 慢消费者：每条失效处理 80ms（模拟业务高峰/长 GC），50 条 ≈ 4s 处理时长
        java.util.concurrent.atomic.AtomicInteger applied = new java.util.concurrent.atomic.AtomicInteger();
        BroadcastApplier slowApplier = new BroadcastApplier(b.l1(), registry, "") {
            @Override
            public boolean apply(String key) {
                boolean result = super.apply(key);
                applied.incrementAndGet();
                try {
                    Thread.sleep(80);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return result;
            }
        };
        StreamsInvalidationConsumer consumer = new StreamsInvalidationConsumer(template, slowApplier, stream);
        consumer.start();
        try {
            for (long id = 1; id <= 50; id++) {
                final long key = id;
                b.cache().load(meta, key, null, true, () -> new UserEntity(key, "v0"));
            }
            StreamsInvalidationPublisher publisher =
                    new StreamsInvalidationPublisher(template, stream,
                            new CacheKitProperties().getBroadcast().getStreamsMaxlen());
            // 管道化 50 条失效瞬间发出；消费者以 80ms/条 的速度追赶
            List<Object> ids = new java.util.ArrayList<>();
            for (long id = 1; id <= 50; id++) {
                ids.add(id);
            }
            b.cache().evictBatch(meta, ids);

            long deadline = System.currentTimeMillis() + 30_000;
            int cleared = 0;
            while (System.currentTimeMillis() < deadline) {
                cleared = 0;
                for (Object id : ids) {
                    if (!b.l1().get(fullKey((Long) id)).hit()) {
                        cleared++;
                    }
                }
                if (cleared == 50) {
                    break;
                }
                Thread.sleep(200);
            }
            System.out.printf("===== streams 慢消费者混沌：50 条失效，送达 %d/50，applied=%d =====%n",
                    cleared, applied.get());
            assertThat(cleared).as("慢消费者不得丢失效（消费组补投，零丢失）").isEqualTo(50);
        } finally {
            consumer.stop();
        }
    }
}
