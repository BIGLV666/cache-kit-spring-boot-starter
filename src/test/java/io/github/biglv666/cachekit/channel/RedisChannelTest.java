package io.github.biglv666.cachekit.channel;

import io.github.biglv666.cachekit.core.InvalidationSubscriber;
import io.github.biglv666.cachekit.core.RedisInvalidationPublisher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Redis 通道与失效广播集成测试：本地无 Redis（127.0.0.1:6379）时自动跳过；
 * CI 环境提供 Redis service，会实际执行。
 */
class RedisChannelTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 6379;

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate template;

    @BeforeAll
    static void setUp() {
        assumeTrue(redisAvailable(), "本地无 Redis，跳过集成测试");
        factory = new LettuceConnectionFactory(HOST, PORT);
        factory.afterPropertiesSet();
        template = new StringRedisTemplate(factory);
        template.afterPropertiesSet();
    }

    @AfterAll
    static void tearDown() {
        if (factory != null) {
            factory.destroy();
        }
    }

    private static boolean redisAvailable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(HOST, PORT), 500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    void putGetEvictShouldWork() {
        RedisChannel channel = new RedisChannel(template);
        String key = "cache-kit-test:roundtrip";

        channel.evict(key);
        assertThat(channel.get(key)).isEqualTo(CacheEntry.miss());

        channel.put(key, "{\"v\":1}", Duration.ofSeconds(30));
        assertThat(channel.get(key)).isEqualTo(CacheEntry.of("{\"v\":1}"));

        channel.evict(key);
        assertThat(channel.get(key)).isEqualTo(CacheEntry.miss());
    }

    @Test
    void entryShouldExpireAfterTtl() throws InterruptedException {
        RedisChannel channel = new RedisChannel(template);
        String key = "cache-kit-test:ttl";

        channel.put(key, "v", Duration.ofMillis(200));
        assertThat(channel.get(key).hit()).isTrue();

        Thread.sleep(600);
        assertThat(channel.get(key)).isEqualTo(CacheEntry.miss());
    }

    @Test
    void broadcastShouldEvictRemoteL1() throws Exception {
        CaffeineChannel l1 = new CaffeineChannel(128);
        // 订阅器只清"已知实体前缀"的键：先解析实体元数据再广播其键
        io.github.biglv666.cachekit.metadata.EntityMetadataRegistry registry =
                new io.github.biglv666.cachekit.metadata.EntityMetadataRegistry();
        registry.find(io.github.biglv666.cachekit.binlog.BinTestUser.class);
        l1.put("user_bin:1", "v", Duration.ofSeconds(30));

        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        String topic = "cache-kit:test-invalidate";
        container.addMessageListener(new InvalidationSubscriber(l1, registry), new ChannelTopic(topic));
        container.afterPropertiesSet();
        container.start();
        try {
            new RedisInvalidationPublisher(template, topic).publish("user_bin:1");
            // pub/sub 异步，轮询等待
            long deadline = System.currentTimeMillis() + 3000;
            while (l1.get("user_bin:1").hit() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertThat(l1.get("user_bin:1")).isEqualTo(CacheEntry.miss());
        } finally {
            container.stop();
            container.destroy();
        }
    }
}
