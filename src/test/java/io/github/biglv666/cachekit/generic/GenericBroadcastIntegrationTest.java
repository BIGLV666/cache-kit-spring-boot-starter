package io.github.biglv666.cachekit.generic;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.channel.RedisChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.BroadcastApplier;
import io.github.biglv666.cachekit.core.GenericKeyCodec;
import io.github.biglv666.cachekit.core.InvalidationSubscriber;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.RedisInvalidationPublisher;
import io.github.biglv666.cachekit.core.StreamsInvalidationConsumer;
import io.github.biglv666.cachekit.core.StreamsInvalidationPublisher;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.support.JsonCodec;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 通用缓存失效广播端到端：pub/sub 与 streams 两种模式都要把 tagged 消息
 * 路由到 {@link BroadcastApplier} 并清除本实例 L1。
 */
class GenericBroadcastIntegrationTest {

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate template;

    @BeforeAll
    static void startRedis() {
        boolean dockerAvailable;
        try {
            dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            dockerAvailable = false;
        }
        Assumptions.assumeTrue(dockerAvailable, "无 Docker 环境，跳过通用缓存广播集成测试");
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

    private static GenericCacheRegistry registry() {
        GenericCacheRegistry registry = new GenericCacheRegistry();
        registry.require(TestValue.class);
        return registry;
    }

    private static void put(CaffeineChannel l1, String key) {
        l1.put(key, JsonCodec.write(new TestValue("v", 1)), Duration.ofMinutes(1));
        assertThat(l1.get(key).hit()).isTrue();
    }

    private static void awaitEvicted(CaffeineChannel l1, String key, String mode) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (l1.get(key).hit() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertThat(l1.get(key).hit()).as(mode + " 广播应清除 L1").isFalse();
    }

    @Test
    void pubsubShouldEvictGenericL1() throws Exception {
        String key = GenericKeyCodec.SEGMENT + ":app-config:pubsub";
        CaffeineChannel l1 = new CaffeineChannel(1024);
        BroadcastApplier applier = new BroadcastApplier(l1, new EntityMetadataRegistry(), registry(), "");
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        container.addMessageListener(new InvalidationSubscriber(applier), new ChannelTopic("cache-kit:generic-test"));
        container.afterPropertiesSet();
        container.start();
        try {
            long deadline = System.currentTimeMillis() + 2_000;
            while (!container.isRunning() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            Thread.sleep(200);

            put(l1, key);
            new RedisInvalidationPublisher(template, "cache-kit:generic-test")
                    .publish(GenericKeyCodec.wrap(key));

            awaitEvicted(l1, key, "pub/subs");
        } finally {
            container.stop();
            container.destroy();
        }
    }

    @Test
    void streamsShouldEvictGenericL1() throws Exception {
        String key = GenericKeyCodec.SEGMENT + ":app-config:streams";
        String stream = "cache-kit:generic-stream-test";
        CaffeineChannel l1 = new CaffeineChannel(1024);
        BroadcastApplier applier = new BroadcastApplier(l1, new EntityMetadataRegistry(), registry(), "");
        StreamsInvalidationConsumer consumer = new StreamsInvalidationConsumer(template, applier, stream);
        consumer.start();
        try {
            long deadline = System.currentTimeMillis() + 5_000;
            while (!consumer.isGroupReady() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertThat(consumer.isGroupReady()).isTrue();

            put(l1, key);
            new StreamsInvalidationPublisher(template, stream, 1000)
                    .publish(GenericKeyCodec.wrap(key));

            awaitEvicted(l1, key, "streams");
        } finally {
            consumer.stop();
        }
    }

    @Test
    void sharedL2ShouldServeAnotherInstanceAndEvict() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        GenericCacheMetadata meta = new GenericCacheMetadata(TestValue.class, "app-config", null, true);
        var valueType = io.github.biglv666.cachekit.support.JsonCodec.constructType(TestValue.class);
        String key = "l2-" + UUID.randomUUID();

        CaffeineChannel l1A = new CaffeineChannel(1024);
        CaffeineChannel l1B = new CaffeineChannel(1024);
        CaffeineChannel l1C = new CaffeineChannel(1024);
        TieredValueCache a = new TieredValueCache(props, l1A, new RedisChannel(template),
                new NoopInvalidationPublisher(), null, "", List.of());
        TieredValueCache b = new TieredValueCache(props, l1B, new RedisChannel(template),
                new NoopInvalidationPublisher(), null, "", List.of());
        TieredValueCache c = new TieredValueCache(props, l1C, new RedisChannel(template),
                new NoopInvalidationPublisher(), null, "", List.of());

        AtomicInteger loads = new AtomicInteger();
        a.load(meta, key, valueType, null, true, () -> {
            loads.incrementAndGet();
            return new TestValue("l2", 1);
        });
        b.load(meta, key, valueType, null, true, () -> {
            loads.incrementAndGet();
            return new TestValue("miss", 2);
        });

        assertThat(loads.get()).as("第二个实例应命中共享 L2").isEqualTo(1);
        assertThat(b.peek(meta, key, valueType)).isEqualTo(new TestValue("l2", 1));

        a.evict(meta, key);
        assertThat(c.peek(meta, key, valueType)).as("evict 应删除共享 L2").isNull();
    }
}
