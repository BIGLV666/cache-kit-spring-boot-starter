package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Streams 消费组自愈：
 * 1. 运行中组被删除（XGROUP DESTROY）后，消费者必须重建组并继续送达；
 * 2. Redis FLUSHALL（组与 Stream 同时消失）后必须自愈，且重建期间发布的失效消息
 *    仍能被送达（重建位点从 0-0 重放，失效 DEL 幂等）。
 *
 * <p>旧实现只在进入循环前 ensureGroup 一次，NOGROUP 后永远退避，streams 模式静默失效。</p>
 */
class StreamsGroupRecoveryIntegrationTest {

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate template;
    private static CaffeineChannel l1;
    private static BroadcastApplier applier;

    @BeforeAll
    static void startRedis() {
        boolean dockerAvailable;
        try {
            dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            dockerAvailable = false;
        }
        org.junit.jupiter.api.Assumptions.assumeTrue(dockerAvailable, "无 Docker 环境，跳过 streams 自愈测试");
        try {
            redis = new GenericContainer<>("redis:7").withExposedPorts(6379);
            redis.start();
        } catch (Exception e) {
            org.junit.jupiter.api.Assumptions.assumeTrue(false, "Redis 容器启动失败，跳过: " + e.getMessage());
        }
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379)));
        factory.afterPropertiesSet();
        template = new StringRedisTemplate(factory);
        template.afterPropertiesSet();
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.require(UserEntity.class);
        l1 = new CaffeineChannel(1_000);
        applier = new BroadcastApplier(l1, registry, "");
    }

    @AfterAll
    static void shutdown() {
        if (factory != null) {
            factory.destroy();
        }
    }

    @BeforeEach
    void flushAll() {
        template.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
    }

    @Test
    void groupDestroyedWhileRunningShouldBeRecreatedAndContinueDelivering() throws Exception {
        String topic = "cache-kit:test:streams-recovery:" + UUID.randomUUID();
        RecordingMetrics metrics = new RecordingMetrics();
        StreamsInvalidationConsumer consumer = new StreamsInvalidationConsumer(template, applier, topic);
        consumer.setMetricsListener(metrics);
        consumer.start();
        try {
            awaitGroup(topic, consumer.groupName(), 8_000);
            assertThat(consumer.isGroupReady()).isTrue();

            // 正常送达基线
            l1.put("user_entity:1", "{\"userId\":1,\"userName\":\"cached\"}", Duration.ofMinutes(1));
            new StreamsInvalidationPublisher(template, topic, 1_000).publish("user_entity:1");
            assertThat(awaitEvicted("user_entity:1", 5_000)).as("基线失效应送达").isTrue();

            // 运行中删除组：旧实现从此永久 NOGROUP
            template.opsForStream().destroyGroup(topic, consumer.groupName());

            l1.put("user_entity:2", "{\"userId\":2,\"userName\":\"cached\"}", Duration.ofMinutes(1));
            new StreamsInvalidationPublisher(template, topic, 1_000).publish("user_entity:2");
            assertThat(awaitEvicted("user_entity:2", 10_000))
                    .as("组重建后失效应继续送达").isTrue();
            assertThat(consumer.groupRecreatedCount()).isGreaterThanOrEqualTo(1);
            assertThat(metrics.recreated.get()).isGreaterThanOrEqualTo(1);
            assertThat(consumer.isGroupReady()).isTrue();
        } finally {
            consumer.stop();
            template.delete(topic);
        }
    }

    @Test
    void flushAllShouldBeRecoveredAndInvalidationPublishedDuringOutageDelivered() throws Exception {
        String topic = "cache-kit:test:streams-flush:" + UUID.randomUUID();
        StreamsInvalidationConsumer consumer = new StreamsInvalidationConsumer(template, applier, topic);
        consumer.start();
        try {
            awaitGroup(topic, consumer.groupName(), 8_000);
            l1.put("user_entity:1", "{\"userId\":1,\"userName\":\"cached\"}", Duration.ofMinutes(1));
            new StreamsInvalidationPublisher(template, topic, 1_000).publish("user_entity:1");
            assertThat(awaitEvicted("user_entity:1", 5_000)).isTrue();

            // 组与 Stream 同时消失（Redis 重启/FLUSHALL/主从切到空实例）
            template.execute((RedisCallback<Object>) connection -> {
                connection.serverCommands().flushAll();
                return null;
            });

            // 重建窗口内发布：重建位点为 0-0，消息仍应被送达
            l1.put("user_entity:3", "{\"userId\":3,\"userName\":\"cached\"}", Duration.ofMinutes(1));
            new StreamsInvalidationPublisher(template, topic, 1_000).publish("user_entity:3");
            assertThat(awaitEvicted("user_entity:3", 10_000))
                    .as("FLUSHALL 后重建组并补投窗口内消息").isTrue();
            assertThat(consumer.groupRecreatedCount()).isGreaterThanOrEqualTo(1);
        } finally {
            consumer.stop();
            template.delete(topic);
        }
    }

    /**
     * 压测场景：3 实例 × 500 键，第一轮正常送达；FLUSHALL 后所有实例的组同时丢失，
     * 仍需在时限内把第二轮 500 条失效全部送达（重建位点 0-0 重放保留的 Stream）。
     */
    @Test
    void bulkInvalidationShouldSurviveGroupLossOnAllInstances() throws Exception {
        String topic = "cache-kit:test:streams-bulk:" + UUID.randomUUID();
        int instanceCount = 3;
        int keyCount = 500;
        List<CaffeineChannel> l1Channels = new ArrayList<>();
        List<StreamsInvalidationConsumer> consumers = new ArrayList<>();
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.require(UserEntity.class);
        for (int i = 0; i < instanceCount; i++) {
            CaffeineChannel channel = new CaffeineChannel(10_000);
            l1Channels.add(channel);
            StreamsInvalidationConsumer consumer = new StreamsInvalidationConsumer(
                    template, new BroadcastApplier(channel, registry, ""), topic);
            consumers.add(consumer);
            consumer.start();
        }
        StreamsInvalidationPublisher publisher = new StreamsInvalidationPublisher(template, topic, 10_000);
        List<String> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add("user_entity:" + i);
        }
        try {
            for (StreamsInvalidationConsumer consumer : consumers) {
                awaitGroup(topic, consumer.groupName(), 8_000);
            }

            // 第一轮：正常送达基线
            fill(l1Channels, keys);
            publisher.publishAll(keys);
            assertThat(awaitAllEvicted(l1Channels, keys, 15_000))
                    .as("第一轮 500 条失效应送达全部实例").isTrue();

            // 组与 Stream 同时消失 → 所有实例自愈
            template.execute((RedisCallback<Object>) connection -> {
                connection.serverCommands().flushAll();
                return null;
            });
            fill(l1Channels, keys);
            publisher.publishAll(keys);
            assertThat(awaitAllEvicted(l1Channels, keys, 25_000))
                    .as("FLUSHALL 后 3 实例 × 500 条失效仍应全部送达").isTrue();
            for (StreamsInvalidationConsumer consumer : consumers) {
                assertThat(consumer.groupRecreatedCount()).isGreaterThanOrEqualTo(1);
            }
        } finally {
            for (StreamsInvalidationConsumer consumer : consumers) {
                consumer.stop();
            }
            template.delete(topic);
        }
    }

    private static void fill(List<CaffeineChannel> channels, List<String> keys) {
        for (CaffeineChannel channel : channels) {
            for (String key : keys) {
                channel.put(key, "{\"userId\":1,\"userName\":\"cached\"}", Duration.ofMinutes(1));
            }
        }
    }

    private static boolean awaitAllEvicted(List<CaffeineChannel> channels, List<String> keys, long timeoutMs)
            throws InterruptedException {
        return awaitCondition(() -> {
            for (CaffeineChannel channel : channels) {
                for (String key : keys) {
                    if (channel.get(key).hit()) {
                        return false;
                    }
                }
            }
            return true;
        }, timeoutMs);
    }

    static class RecordingMetrics implements CacheMetricsListener {
        final AtomicInteger recreated = new AtomicInteger();

        @Override
        public void streamsGroupRecreated() {
            recreated.incrementAndGet();
        }
    }

    private static void awaitGroup(String topic, String group, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (template.opsForStream().groups(topic).stream()
                        .anyMatch(g -> group.equals(g.groupName()))) {
                    return;
                }
            } catch (Exception ignored) {
                // Stream 尚不存在：继续等待
            }
            Thread.sleep(50);
        }
        throw new AssertionError("消费组未在超时内创建: " + group);
    }

    private static boolean awaitEvicted(String key, long timeoutMs) throws InterruptedException {
        return awaitCondition(() -> !l1.get(key).hit(), timeoutMs);
    }

    private static boolean awaitCondition(java.util.function.BooleanSupplier condition, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50);
        }
        return condition.getAsBoolean();
    }
}
