package io.github.biglv666.cachekit.chaos.poison;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.channel.RedisChannel;
import io.github.biglv666.cachekit.channel.CacheEntry;
import io.github.biglv666.cachekit.chaos.support.ChaosContainers;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.BroadcastApplier;
import io.github.biglv666.cachekit.support.JsonCodec;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.StreamsInvalidationConsumer;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 恶意/异常输入混沌：能写入 Redis 的攻击者（内部人员、越权服务）与垃圾数据对组件的影响。
 *
 * <p>验证：① L2 毒值（结构漂移的垃圾 JSON）→ 按未命中处理、回源自愈、业务零异常；
 * ② NULL 占位哨兵被恶意写入 → 既定行为是"该键在 null-ttl 窗口内被服务为不存在"
 * （写 L2 的前提已是越权，危害有界且哨兵值可被判别，记录为已知边界而非缺陷）；
 * ③ 伪造广播：未知前缀键被忽略（防缓存 DoS），已知前缀键的失效被尊重（失效幂等无害）；
 * ④ 垃圾 stream 记录不中断消费。</p>
 */
class InputChaosTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 16382; // 本类独立固定端口

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate template;

    @BeforeAll
    static void startRedis() {
        Assumptions.assumeTrue(dockerAvailable(), "无 Docker，跳过输入投毒混沌测试");
        Assumptions.assumeTrue(ChaosContainers.hostPortFree(PORT), "固定端口被占用，跳过");
        redis = ChaosContainers.redisFixedPort(PORT);
        redis.start();
        factory = new LettuceConnectionFactory(new RedisStandaloneConfiguration(HOST, PORT));
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

    private static boolean dockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }

    private TieredEntityCache cache(CaffeineChannel l1) {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        return new TieredEntityCache(props, l1, new RedisChannel(template),
                new NoopInvalidationPublisher(), null, "", List.of());
    }

    private EntityMetadata meta() {
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.find(UserEntity.class);
        return registry.require(UserEntity.class);
    }

    /** L2 毒值（结构漂移的垃圾 JSON）：按未命中处理，回源自愈覆盖，业务零异常 */
    @Test
    void poisonedL2ValueShouldSelfHeal() {
        EntityMetadata meta = meta();
        CaffeineChannel l1 = new CaffeineChannel(100);
        TieredEntityCache cache = cache(l1);
        String fullKey = meta.prefix() + ":1"; // 直连操作必须用完整前缀键（切面组装后才入存储）

        // 1) 正常回填 L2
        assertThat(((UserEntity) cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "v0"))).getUserName())
                .isEqualTo("v0");
        assertThat(template.opsForValue().get(fullKey)).isNotBlank();

        // 2) 攻击者改写 L2 为垃圾 JSON（模拟结构漂移/篡改）
        template.opsForValue().set(fullKey, "{\"corrupted\":", Duration.ofMinutes(5));
        l1.evict(fullKey);

        // 3) 读路径：解码失败按未命中 → 回源 → putBoth 覆盖毒值，绝不抛异常、绝不返回坏对象
        Object v = cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "v1"));
        assertThat(v).as("毒值必须按未命中处理并回源")
                .extracting(e -> ((UserEntity) e).getUserName()).isEqualTo("v1");
        assertThat(template.opsForValue().get(fullKey))
                .as("回源成功必须覆盖毒值（自愈）")
                .isEqualTo(JsonCodec.write(new UserEntity(1L, "v1")));
    }

    /** NULL 占位哨兵被恶意写入：既定行为 = 该键在窗口内被服务为"确认不存在"（已知边界，非缺陷） */
    @Test
    void poisonedNullSentinelBehaviorIsDocumentedBoundary() {
        EntityMetadata meta = meta();
        CaffeineChannel l1 = new CaffeineChannel(100);
        TieredEntityCache cache = cache(l1);
        String fullKey = meta.prefix() + ":2";

        template.opsForValue().set(fullKey, JsonCodec.NULL_SENTINEL, Duration.ofSeconds(30));
        AtomicInteger loaderCalls = new AtomicInteger();

        Object v = cache.load(meta, 2L, null, true, () -> {
            loaderCalls.incrementAndGet();
            return new UserEntity(2L, "v0");
        });
        assertThat(v).as("哨兵占位被服务为 null（写 L2 的前提已是越权，危害有界）").isNull();
        assertThat(loaderCalls.get()).as("占位命中不回源（防穿透机制的副作用面）").isZero();

        // 失效（应用内写路径触发）后立即恢复可见——占位可被正常失效链清除
        cache.evictBatch(meta, List.of(2L));
        Object after = cache.load(meta, 2L, null, true, () -> {
            loaderCalls.incrementAndGet();
            return new UserEntity(2L, "v0");
        });
        assertThat(((UserEntity) after).getUserName()).isEqualTo("v0");
        assertThat(loaderCalls.get()).isEqualTo(1);
    }

    /** 伪造广播：未知前缀被忽略（防任意清缓存），已知前缀失效被尊重（幂等无害） */
    @Test
    void forgedBroadcastShouldNotClearUnknownKeys() throws Exception {
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.find(UserEntity.class);
        EntityMetadata meta = registry.require(UserEntity.class);
        CaffeineChannel l1 = new CaffeineChannel(100);
        l1.put(meta.prefix() + ":1", "j", Duration.ofSeconds(30));          // 已知前缀
        l1.put("attacker:secret", "j", Duration.ofSeconds(30));             // 未知前缀（攻击者的键）
        String topic = "chaos:forged-broadcast";
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        container.addMessageListener(new io.github.biglv666.cachekit.core.InvalidationSubscriber(l1, registry, ""),
                new ChannelTopic(topic));
        container.afterPropertiesSet();
        container.start();
        try {
            // 攻击者向 topic 伪造消息：清空任意键 + 空串 + 已知前缀
            template.convertAndSend(topic, "attacker:secret");
            template.convertAndSend(topic, "");
            template.convertAndSend(topic, meta.prefix() + ":1");
            Thread.sleep(800);

            assertThat(l1.get("attacker:secret").hit())
                    .as("未知前缀键必须被忽略（广播 DoS 防护边界）").isTrue();
            assertThat(l1.get(meta.prefix() + ":1").hit())
                    .as("已知前缀的失效被尊重（失效幂等，允许任意发布方触发）").isFalse();
        } finally {
            container.stop();
            container.destroy();
        }
    }

    /** 垃圾 stream 记录（空字段/巨大值/错误字段）不中断消费，后续有效记录照常送达 */
    @Test
    void junkStreamRecordsShouldNotBreakConsumer() throws Exception {
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.find(UserEntity.class);
        CaffeineChannel l1 = new CaffeineChannel(100);
        String stream = "chaos:junk-stream";
        StreamsInvalidationConsumer consumer =
                new StreamsInvalidationConsumer(template, new BroadcastApplier(l1, registry, ""), stream);
        consumer.start();
        try {
            // 垃圾记录：空值字段（XADD 不允许零字段）、错误字段名、1MB 大值
            template.opsForStream().add(stream, Map.of("k", ""));
            template.opsForStream().add(stream, Map.of("wrong", "shape"));
            byte[] junk = new byte[1024 * 1024];
            java.util.Arrays.fill(junk, (byte) 'x');
            template.opsForStream().add(stream, Map.of("k", new String(junk, java.nio.charset.StandardCharsets.UTF_8)));
            // 有效记录
            template.opsForStream().add(stream, Map.of("k", meta().prefix() + ":5"));
            l1.put(meta().prefix() + ":5", "j", Duration.ofSeconds(30));

            long deadline = System.currentTimeMillis() + 10_000;
            while (l1.get(meta().prefix() + ":5").hit() && System.currentTimeMillis() < deadline) {
                Thread.sleep(100);
            }
            assertThat(l1.get(meta().prefix() + ":5").hit())
                    .as("垃圾记录之后的合法失效必须照常送达（消费循环未中断）").isFalse();
        } finally {
            consumer.stop();
        }
    }
}
