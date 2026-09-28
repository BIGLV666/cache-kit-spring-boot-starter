package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CacheChannel;
import io.github.biglv666.cachekit.channel.CacheEntry;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.channel.RedisChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 读路径对比 benchmark（Testcontainers redis:7，单线程、非 JMH，量级参考）：
 * cache-kit（L1 命中 / L2 命中 / miss→回源含序列化）vs 裸 Redis GET vs
 * Spring Cache（RedisCacheManager）vs JetCache（RedisLettuceCache）。
 * 键在 1000 个间轮转，避免极端热键；每行 2000 次预热后计时。
 * 失效传播延迟见 InvalidationBroadcastMultiInstanceIntegrationTest 的多实例实测。
 */
class CacheBenchmarkIntegrationTest {

    private static final int KEYS = 1_000;
    private static final int WARMUP_OPS = 2_000;

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate template;
    private static EntityMetadata meta;

    @BeforeAll
    static void startRedis() {
        boolean dockerAvailable;
        try {
            dockerAvailable = DockerClientHolder.isAvailable();
        } catch (Throwable t) {
            dockerAvailable = false;
        }
        Assumptions.assumeTrue(dockerAvailable, "无 Docker 环境，跳过 benchmark");
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
        meta = new EntityMetadataRegistry().require(UserEntity.class);
    }

    @AfterAll
    static void shutdown() {
        if (factory != null) {
            factory.destroy();
        }
    }

    /** 恒定未命中的通道：隔离 L2 命中/回源路径 */
    private static CacheChannel alwaysMiss() {
        return new CacheChannel() {
            @Override
            public CacheEntry get(String key) {
                return CacheEntry.miss();
            }

            @Override
            public void put(String key, String json, Duration ttl) {
                // 故意不存：读路径恒 miss
            }

            @Override
            public void evict(String key) {
            }
        };
    }

    private static TieredEntityCache cache(CacheChannel l1, CacheChannel l2) {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        return new TieredEntityCache(props, l1, l2, new NoopInvalidationPublisher(),
                new DoubleDeleteScheduler(Duration.ofMillis(20)));
    }

    private static String json(long id) {
        return io.github.biglv666.cachekit.support.JsonCodec.write(new UserEntity(id, "v" + id));
    }

    private static String cacheKitKey(long id) {
        return meta.prefix() + ":" + id;
    }

    /** 单线程固定操作数测吞吐（先预热），返回 ops/sec */
    private static double bench(String name, int ops, Runnable op) {
        for (int i = 0; i < Math.min(WARMUP_OPS, ops); i++) {
            op.run();
        }
        long t0 = System.nanoTime();
        for (int i = 0; i < ops; i++) {
            op.run();
        }
        double opsPerSec = ops / ((System.nanoTime() - t0) / 1e9);
        System.out.printf("ROW|%-36s|%,14.0f%n", name, opsPerSec);
        return opsPerSec;
    }

    @Test
    void readPathComparison() {
        // 键轮转游标：避免 1000 轮内一直打同一个热键
        AtomicLong cursor = new AtomicLong();
        LongFunction<Long> nextId = base -> Math.abs(cursor.getAndIncrement() % KEYS) + 1;

        // 1) cache-kit L1 命中（真实 Caffeine L1，全部预热）
        TieredEntityCache l1Cache = cache(new CaffeineChannel(10_000), new RedisChannel(template));
        for (long id = 1; id <= KEYS; id++) {
            final long fid = id;
            l1Cache.load(meta, id, null, true, () -> new UserEntity(fid, "v" + fid));
        }
        bench("cache-kit L1 命中", 500_000, () ->
                l1Cache.load(meta, nextId.apply(0), null, true, () -> {
                    throw new AssertionError("不应回源");
                }));

        // 2) cache-kit L2 命中（L1 恒 miss：Redis GET + 反序列化；L2 由上一行的同键预热填充）
        TieredEntityCache l2Cache = cache(alwaysMiss(), new RedisChannel(template));
        assertThat(l2Cache.load(meta, 1L, null, true, () -> {
            throw new AssertionError("不应回源");
        })).as("L2 预校验应命中").isNotNull();
        bench("cache-kit L2 命中", 30_000, () ->
                l2Cache.load(meta, nextId.apply(0), null, true, () -> {
                    throw new AssertionError("不应回源");
                }));

        // 3) cache-kit miss→回源（L1/L2 恒 miss：single-flight + 回源 + 序列化）
        TieredEntityCache missCache = cache(alwaysMiss(), alwaysMiss());
        bench("cache-kit miss→回源(含序列化)", 100_000, () ->
                missCache.load(meta, nextId.apply(0), null, true, () -> new UserEntity(1L, "v1")));

        // 4) 裸 Redis GET（StringRedisTemplate）
        for (long id = 1; id <= KEYS; id++) {
            template.opsForValue().set("bench:raw:" + id, json(id), Duration.ofSeconds(120));
        }
        bench("裸 Redis GET", 30_000, () ->
                template.opsForValue().get("bench:raw:" + nextId.apply(0)));

        // 5) Spring Cache（RedisCacheManager，JSON 值序列化——生产常见配置）
        RedisCacheManager manager = RedisCacheManager.builder(factory)
                .cacheDefaults(RedisCacheConfiguration.defaultCacheConfig()
                        .entryTtl(Duration.ofSeconds(120))
                        .serializeValuesWith(org.springframework.data.redis.serializer.RedisSerializationContext
                                .SerializationPair.fromSerializer(
                                        new org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer())))
                .build();
        org.springframework.cache.Cache spring = manager.getCache("bench-spring");
        for (long id = 1; id <= KEYS; id++) {
            spring.put(id, new UserEntity(id, "v" + id));
        }
        bench("Spring Cache (RedisCacheManager)", 30_000, () ->
                spring.get(nextId.apply(0).intValue()));

        // 6) JetCache（RedisLettuceCache；装配失败仅跳过该行，不阻塞其余对比）
        try {
            RedisClient jetClient = RedisClient.create(
                    "redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
            // JetCache 走 byte[] 编解码：必须用 JetCacheCodec 建连（默认 String codec 会 EncoderException）
            StatefulRedisConnection<String, String> jetConn =
                    (StatefulRedisConnection<String, String>) jetClient.connect(
                            new com.alicp.jetcache.redis.lettuce.JetCacheCodec());
            com.alicp.jetcache.redis.lettuce.LettuceConnectionManager mgr =
                    com.alicp.jetcache.redis.lettuce.LettuceConnectionManager.defaultManager();
            mgr.init(jetClient, jetConn);
            com.alicp.jetcache.Cache<Long, String> jet =
                    com.alicp.jetcache.redis.lettuce.RedisLettuceCacheBuilder
                            .createRedisLettuceCacheBuilder()
                            .redisClient(jetClient)
                            .connection(jetConn)
                            .connectionManager(mgr)
                            .keyPrefix("bench:jet:")
                            .keyConvertor(o -> o == null ? null : o.toString())
                            // JetCache 内部把值包进 CacheValueHolder 再编码：必须用能序列化 holder 的编码器
                            .valueEncoder(com.alicp.jetcache.support.JavaValueEncoder.INSTANCE)
                            .valueDecoder(com.alicp.jetcache.support.JavaValueDecoder.INSTANCE)
                            .expireAfterWrite(120, TimeUnit.SECONDS)
                            .build();
            for (long id = 1; id <= KEYS; id++) {
                jet.put(id, json(id));
            }
            com.alicp.jetcache.CacheGetResult<String> probe = jet.GET(1L);
            System.out.println("JET PROBE: isSuccess=" + probe.isSuccess() + " message=" + probe.getMessage());
            if (!probe.isSuccess()) {
                throw new IllegalStateException("JetCache 预校验未命中: " + probe.getMessage());
            }
            bench("JetCache (RedisLettuceCache)", 30_000, () ->
                    jet.get(nextId.apply(0)));
        } catch (Throwable t) {
            System.out.println("ROW|JetCache (RedisLettuceCache)|N/A（装配失败: " + t + "）");
        }
    }

    /** Docker 可用性探测（嵌套类隔离，防无 Testcontainers 环境类加载失败） */
    private static final class DockerClientHolder {
        static boolean isAvailable() {
            return org.testcontainers.DockerClientFactory.instance().isDockerAvailable();
        }
    }
}
