package io.github.biglv666.cachekit.core;

import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.channel.RedisChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import io.github.biglv666.cachekit.support.JsonCodec;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;
import java.util.function.LongConsumer;

/**
 * 诊断用（非选型依据）：L1 命中路径逐段归因，单线程 µs 级中位数/均值。
 * 回答"cache-kit L1 命中为何比 JetCache 多级快"——把 key 拼接、Caffeine get、
 * Jackson 反序列化、JetCache 持有者/多级包装各段成本拆开对比。
 */
class CacheHitPathAttributionTest {

    private static final int KEYS = 1_000;
    private static final int WARMUP = 300_000;
    private static final int MEASURE = 2_000_000;

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory factory;
    private static EntityMetadataRegistry registry;
    private static EntityMetadata meta;
    private static TieredEntityCache l1Cache;
    private static com.alicp.jetcache.Cache<Long, String> jetLocal;
    private static com.alicp.jetcache.Cache<Long, String> jetMulti;
    private static RedisClient jetClient;

    @BeforeAll
    static void setup() {
        boolean dockerAvailable;
        try {
            dockerAvailable = org.testcontainers.DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            dockerAvailable = false;
        }
        Assumptions.assumeTrue(dockerAvailable, "无 Docker，跳过归因实验");
        redis = new GenericContainer<>("redis:7").withExposedPorts(6379);
        redis.start();
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379)));
        factory.afterPropertiesSet();
        registry = new EntityMetadataRegistry();
        meta = registry.require(UserEntity.class);

        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        l1Cache = new TieredEntityCache(props, new CaffeineChannel(10_000),
                new RedisChannel(template(factory)), new NoopInvalidationPublisher(),
                new DoubleDeleteScheduler(Duration.ofMillis(20)));
        for (long id = 1; id <= KEYS; id++) {
            final long fid = id;
            l1Cache.load(meta, id, null, true, () -> new UserEntity(fid, "v" + fid));
        }

        jetClient = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        StatefulRedisConnection<String, String> conn =
                (StatefulRedisConnection<String, String>) jetClient.connect(
                        new com.alicp.jetcache.redis.lettuce.JetCacheCodec());
        com.alicp.jetcache.redis.lettuce.LettuceConnectionManager mgr =
                com.alicp.jetcache.redis.lettuce.LettuceConnectionManager.defaultManager();
        mgr.init(jetClient, conn);
        jetLocal = CaffeineCacheBuilder.createCaffeineCacheBuilder()
                .limit(10_000).expireAfterWrite(300, TimeUnit.SECONDS)
                .keyConvertor(o -> o == null ? null : o.toString())
                .build();
        com.alicp.jetcache.Cache<Long, String> jetRemote =
                com.alicp.jetcache.redis.lettuce.RedisLettuceCacheBuilder.createRedisLettuceCacheBuilder()
                        .redisClient(jetClient).connection(conn).connectionManager(mgr)
                        .keyPrefix("attr:jet:").keyConvertor(o -> o == null ? null : o.toString())
                        .valueEncoder(com.alicp.jetcache.support.JavaValueEncoder.INSTANCE)
                        .valueDecoder(com.alicp.jetcache.support.JavaValueDecoder.INSTANCE)
                        .expireAfterWrite(300, TimeUnit.SECONDS).build();
        jetMulti = com.alicp.jetcache.MultiLevelCacheBuilder.createMultiLevelCacheBuilder()
                .addCache(jetLocal, jetRemote).build();
        for (long id = 1; id <= KEYS; id++) {
            jetMulti.put(id, "x" + id);
        }
    }

    private static StringRedisTemplate template(LettuceConnectionFactory factory) {
        StringRedisTemplate t = new StringRedisTemplate(factory);
        t.afterPropertiesSet();
        return t;
    }

    @AfterAll
    static void shutdown() {
        if (jetClient != null) {
            jetClient.shutdown();
        }
        if (factory != null) {
            factory.destroy();
        }
    }

    private static long[] measure(IntUnaryOperator idPicker, LongConsumer op) {
        for (int i = 0; i < WARMUP; i++) {
            op.accept(idPicker.applyAsInt(i % KEYS));
        }
        long[] lat = new long[MEASURE];
        for (int i = 0; i < MEASURE; i++) {
            long t0 = System.nanoTime();
            op.accept(idPicker.applyAsInt(i % KEYS));
            lat[i] = System.nanoTime() - t0;
        }
        Arrays.sort(lat);
        return lat;
    }

    private static void report(String name, long[] sorted) {
        long sum = 0;
        for (long v : sorted) {
            sum += v;
        }
        System.out.printf("ROW|%-44s|p50 %5d ns|p99 %7d ns|p999 %7d ns|mean %7.0f ns%n",
                name, sorted[sorted.length / 2], sorted[(int) (sorted.length * 0.99)],
                sorted[(int) (sorted.length * 0.999)], (double) sum / sorted.length);
    }

    @Test
    void hitPathAttribution() {
        IntUnaryOperator pick = i -> i + 1;
        String json = JsonCodec.write(new UserEntity(1L, "v1"));
        CaffeineChannel rawL1 = new CaffeineChannel(10_000);
        String[] keys = new String[KEYS + 1];
        for (int i = 1; i <= KEYS; i++) {
            keys[i] = "user:" + i;
            rawL1.put(keys[i], json(i), Duration.ofSeconds(300));
        }

        // A) 纯 Caffeine get（cache-kit L1 存储层原样，String→String）
        report("A 纯Caffeine get(String)", measure(pick, id -> {
            if (!rawL1.get(keys[(int) id]).hit()) {
                throw new AssertionError();
            }
        }));
        // B) Jackson 反序列化单独计价（cache-kit 命中路径的主要额外成本）
        report("B Jackson readValue(2字段实体)", measure(pick, id -> {
            if (JsonCodec.read(json, UserEntity.class) == null) {
                throw new AssertionError();
            }
        }));
        // C) cache-kit load 完整命中路径（key 拼接 + Caffeine + Jackson + 指标）
        report("C cache-kit load L1命中(全路径)", measure(pick, id ->
                l1Cache.load(meta, (long) id, null, true, () -> {
                    throw new AssertionError("不应回源");
                })));
        // D) JetCache 仅本地 get（持有者包装/解包）
        report("D JetCache 本地get", measure(pick, id -> {
            if (jetLocal.get((long) id) == null) {
                throw new AssertionError();
            }
        }));
        // E) JetCache 多级 get（本地命中）
        report("E JetCache 多级get(本地命中)", measure(pick, id -> {
            if (jetMulti.get((long) id) == null) {
                throw new AssertionError();
            }
        }));
    }

    private static String json(long id) {
        return JsonCodec.write(new UserEntity(id, "v" + id));
    }
}
