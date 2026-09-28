package io.github.biglv666.cachekit.core;

import com.alicp.jetcache.embedded.CaffeineCacheBuilder;
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
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntUnaryOperator;
import java.util.function.LongConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 压测对比（Testcontainers redis:7）：cache-kit vs Spring Cache（RedisCacheManager）vs JetCache。
 *
 * <p>三个场景：
 * 1) 并发读吞吐（16 线程 × 3s，1000 键轮转，ops/s + 延迟分位）；
 * 2) 缓存击穿（50 并发打同一冷键、模拟 50ms DB）：DB 回源次数与总耗时；
 * 3) 单键失效传播（100 轮，A 实例 evict → B 实例 L1 清除的延迟分位，pubsub/streams 两种通道）。</p>
 *
 * <p>数值受环境波动影响，量级与相对关系才是重点；复现：mvn test -Dtest=CacheStressComparisonIntegrationTest。</p>
 */
class CacheStressComparisonIntegrationTest {

    private static final int KEYS = 1_000;
    private static final int READ_THREADS = 16;
    private static final int READ_SECONDS = 3;
    private static final int STORM_THREADS = 50;
    private static final long FAKE_DB_MILLIS = 50;
    private static final int PROPAGATION_ROUNDS = 100;

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory sharedFactory;
    private static LettuceConnectionFactory pooledFactory;
    private static StringRedisTemplate template;
    private static StringRedisTemplate pooledTemplate;
    private static EntityMetadata meta;
    private static EntityMetadataRegistry registry;

    @BeforeAll
    static void startRedis() {
        boolean dockerAvailable;
        try {
            dockerAvailable = DockerClientHolder.isAvailable();
        } catch (Throwable t) {
            dockerAvailable = false;
        }
        Assumptions.assumeTrue(dockerAvailable, "无 Docker 环境，跳过压测对比");
        try {
            redis = new GenericContainer<>("redis:7").withExposedPorts(6379);
            redis.start();
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Redis 容器启动失败，跳过: " + e.getMessage());
        }
        var standalone = new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379));

        sharedFactory = new LettuceConnectionFactory(standalone);
        sharedFactory.afterPropertiesSet();
        template = new StringRedisTemplate(sharedFactory);
        template.afterPropertiesSet();

        // 生产典型配置：Lettuce 连接池（8 连接）
        GenericObjectPoolConfig<io.lettuce.core.api.StatefulConnection<?, ?>> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(8);
        pooledFactory = new LettuceConnectionFactory(standalone,
                LettucePoolingClientConfiguration.builder().poolConfig(poolConfig).build());
        pooledFactory.afterPropertiesSet();
        pooledTemplate = new StringRedisTemplate(pooledFactory);
        pooledTemplate.afterPropertiesSet();

        registry = new EntityMetadataRegistry();
        meta = registry.require(UserEntity.class);
    }

    @AfterAll
    static void shutdown() {
        if (pooledFactory != null) {
            pooledFactory.destroy();
        }
        if (sharedFactory != null) {
            sharedFactory.destroy();
        }
    }

    private static String keyOf(long id) {
        return meta.prefix() + ":" + id;
    }

    private static String json(long id) {
        return io.github.biglv666.cachekit.support.JsonCodec.write(new UserEntity(id, "v" + id));
    }

    private static TieredEntityCache cacheKit(CacheChannel l1) {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        return new TieredEntityCache(props, l1, new RedisChannel(template), new NoopInvalidationPublisher(),
                new DoubleDeleteScheduler(Duration.ofMillis(20)));
    }

    private static CacheChannel alwaysMiss() {
        return new CacheChannel() {
            @Override
            public CacheEntry get(String key) {
                return CacheEntry.miss();
            }

            @Override
            public void put(String key, String json, Duration ttl) {
            }

            @Override
            public void evict(String key) {
            }
        };
    }

    // ================= 场景 1：并发读吞吐 =================

    private record StressResult(long totalOps, double opsPerSec, long p50Us, long p99Us, long p999Us) {
    }

    private static StressResult runConcurrent(int threads, int seconds, IntUnaryOperator idPicker, LongConsumer op)
            throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<List<Long>> latencies = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicLong ops = new AtomicLong();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                List<Long> lat = new ArrayList<>(64_000);
                start.await();
                long cursor = tid;
                while (!stop.get()) {
                    long id = idPicker.applyAsInt((int) (cursor++ % KEYS));
                    long t0 = System.nanoTime();
                    op.accept(id);
                    lat.add(System.nanoTime() - t0);
                    ops.incrementAndGet();
                }
                latencies.add(lat);
                done.countDown();
                return null;
            });
        }
        start.countDown();
        long t0 = System.nanoTime();
        TimeUnit.SECONDS.sleep(seconds);
        stop.set(true);
        done.await();
        double wallSec = (System.nanoTime() - t0) / 1e9;
        pool.shutdownNow();

        int n = latencies.stream().mapToInt(List::size).sum();
        long[] all = new long[n];
        int idx = 0;
        for (List<Long> lat : latencies) {
            for (Long l : lat) {
                all[idx++] = l / 1_000; // µs
            }
        }
        Arrays.sort(all);
        return new StressResult(n, n / wallSec,
                all[(int) (n * 0.50)], all[(int) (n * 0.99)], all[(int) (n * 0.999)]);
    }

    private static void printStress(String name, StressResult r) {
        System.out.printf("ROW|%-34s|%12.0f ops/s|%8d µs|%8d µs|%8d µs%n",
                name, r.opsPerSec(), r.p50Us(), r.p99Us(), r.p999Us());
    }

    @Test
    void stressComparison() throws Exception {
        IntUnaryOperator pick = i -> i + 1;

        // ---- cache-kit 三级（L1 命中为主）；load 同时把值写进共享 L2，供"仅 L2"行使用 ----
        TieredEntityCache l1Cache = cacheKit(new CaffeineChannel(10_000));
        for (long id = 1; id <= KEYS; id++) {
            final long fid = id;
            l1Cache.load(meta, id, null, true, () -> new UserEntity(fid, "v" + fid));
        }
        // ---- cache-kit 仅 L2（L1 bypass）：远程读路径对照，键与上一步相同 ----
        TieredEntityCache l2Only = cacheKit(alwaysMiss());
        // ---- Spring Cache（连接池 8）----
        RedisCacheManager manager = RedisCacheManager.builder(pooledFactory)
                .cacheDefaults(RedisCacheConfiguration.defaultCacheConfig()
                        .entryTtl(Duration.ofSeconds(300))
                        .serializeValuesWith(org.springframework.data.redis.serializer.RedisSerializationContext
                                .SerializationPair.fromSerializer(
                                        new org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer())))
                .build();
        org.springframework.cache.Cache spring = manager.getCache("stress-spring");
        for (long id = 1; id <= KEYS; id++) {
            spring.put(id, new UserEntity(id, "v" + id));
        }
        // ---- 裸 Redis GET（连接池 8）----
        for (long id = 1; id <= KEYS; id++) {
            pooledTemplate.opsForValue().set("stress:raw:" + id, json(id), Duration.ofSeconds(300));
        }
        // ---- JetCache：仅远程 + 多级（Caffeine+Redis），实例在主线程构建、50 线程共享 ----
        RedisClient jetClient = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        StatefulRedisConnection<String, String> jetConn = (StatefulRedisConnection<String, String>)
                jetClient.connect(new com.alicp.jetcache.redis.lettuce.JetCacheCodec());
        com.alicp.jetcache.redis.lettuce.LettuceConnectionManager jetMgr =
                com.alicp.jetcache.redis.lettuce.LettuceConnectionManager.defaultManager();
        jetMgr.init(jetClient, jetConn);
        com.alicp.jetcache.Cache<Long, String> jetRemote = remoteJet(jetClient, jetConn, jetMgr, "stress:jet:", false);
        com.alicp.jetcache.Cache<Long, String> jetMulti = multiLevelJet(jetClient, jetConn, jetMgr, "stress:jetm:");
        for (long id = 1; id <= KEYS; id++) {
            jetRemote.put(id, json(id));
            jetMulti.put(id, json(id));
        }

        System.out.println("===== 场景1 并发读吞吐（16 线程 × 3s，1000 键轮转） =====");
        printStress("cache-kit 三级(L1 命中)", runConcurrent(READ_THREADS, READ_SECONDS, pick,
                id -> l1Cache.load(meta, id, null, true, () -> {
                    throw new AssertionError("不应回源");
                })));
        printStress("JetCache 多级(Caffeine+Redis)", runConcurrent(READ_THREADS, READ_SECONDS, pick,
                id -> jetMulti.get((long) id)));
        printStress("Spring Cache(仅Redis,池8)", runConcurrent(READ_THREADS, READ_SECONDS, pick,
                id -> spring.get(id)));
        printStress("JetCache 仅远程", runConcurrent(READ_THREADS, READ_SECONDS, pick,
                id -> jetRemote.get((long) id)));
        printStress("cache-kit 仅L2(单连接)", runConcurrent(READ_THREADS, READ_SECONDS, pick,
                id -> l2Only.load(meta, id, null, true, () -> {
                    throw new AssertionError("不应回源");
                })));
        printStress("裸 Redis GET(池8)", runConcurrent(READ_THREADS, READ_SECONDS, pick,
                id -> pooledTemplate.opsForValue().get("stress:raw:" + id)));

        // ================= 场景 2：缓存击穿 =================
        System.out.println("===== 场景2 缓存击穿（50 并发 × 同一冷键，模拟 50ms DB） =====");
        // 每行独立键空间与独立缓存实例（主线程构建、50 线程共享——single-flight/防穿透都是实例内语义）
        TieredEntityCache stormCache = cacheKit(new CaffeineChannel(10_000));
        storm("cache-kit single-flight", 901_000, loads ->
                stormCache.load(meta, 901_000, null, true, () -> fakeDb(loads).apply(901_000L)));
        storm("Spring Cache(默认)", 902_000, loads ->
                spring.get(902_000, () -> fakeDb(loads).apply(902_000)));
        RedisClient jetClient2 = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        StatefulRedisConnection<String, String> jetConn2 = (StatefulRedisConnection<String, String>)
                jetClient2.connect(new com.alicp.jetcache.redis.lettuce.JetCacheCodec());
        com.alicp.jetcache.Cache<Long, String> jetDefault =
                remoteJet(jetClient2, jetConn2, jetMgr, "storm:jetd:", false);
        storm("JetCache(默认)", 903_000, loads -> {
            try {
                jetDefault.computeIfAbsent(903_000L, k -> fakeJson(loads, k));
            } catch (com.alicp.jetcache.CacheInvokeException e) {
                throw new IllegalStateException(e);
            }
        });
        RedisClient jetClient3 = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        StatefulRedisConnection<String, String> jetConn3 = (StatefulRedisConnection<String, String>)
                jetClient3.connect(new com.alicp.jetcache.redis.lettuce.JetCacheCodec());
        com.alicp.jetcache.Cache<Long, String> jetProtect =
                remoteJet(jetClient3, jetConn3, jetMgr, "storm:jets:", true);
        storm("JetCache(penetrationProtect)", 904_000, loads -> {
            try {
                jetProtect.computeIfAbsent(904_000L, k -> fakeJson(loads, k));
            } catch (com.alicp.jetcache.CacheInvokeException e) {
                throw new IllegalStateException(e);
            }
        });
        jetClient3.shutdown();
        jetClient2.shutdown();

        // ================= 场景 3：单键失效传播 =================
        System.out.println("===== 场景3 单键失效传播（A evict → B L1 清除，100 轮） =====");
        propagation("cache-kit pubsub", 1_000, false);
        propagation("cache-kit streams", 2_000, true);
    }

    private static com.alicp.jetcache.Cache<Long, String> remoteJet(
            RedisClient client, StatefulRedisConnection<String, String> conn,
            com.alicp.jetcache.redis.lettuce.LettuceConnectionManager mgr, String prefix, boolean protect) {
        var builder = com.alicp.jetcache.redis.lettuce.RedisLettuceCacheBuilder.createRedisLettuceCacheBuilder()
                .redisClient(client)
                .connection(conn)
                .connectionManager(mgr)
                .keyPrefix(prefix)
                .keyConvertor(o -> o == null ? null : o.toString())
                .valueEncoder(com.alicp.jetcache.support.JavaValueEncoder.INSTANCE)
                .valueDecoder(com.alicp.jetcache.support.JavaValueDecoder.INSTANCE)
                .expireAfterWrite(300, TimeUnit.SECONDS);
        if (protect) {
            builder.getConfig().setCachePenetrationProtect(true);
        }
        return builder.build();
    }

    private static com.alicp.jetcache.Cache<Long, String> multiLevelJet(
            RedisClient client, StatefulRedisConnection<String, String> conn,
            com.alicp.jetcache.redis.lettuce.LettuceConnectionManager mgr, String prefix) {
        com.alicp.jetcache.Cache<Long, String> local = CaffeineCacheBuilder.createCaffeineCacheBuilder()
                .limit(10_000)
                .expireAfterWrite(300, TimeUnit.SECONDS)
                .keyConvertor(o -> o == null ? null : o.toString())
                .build();
        com.alicp.jetcache.Cache<Long, String> remote = remoteJet(client, conn, mgr, prefix, false);
        return com.alicp.jetcache.MultiLevelCacheBuilder.createMultiLevelCacheBuilder()
                .addCache(local, remote)
                .build();
    }

    /** 缓存击穿：50 并发打同一冷键，统计 DB 回源次数与总耗时。getOrLoad 执行一次"读缺失则回源" */
    private static void storm(String name, long keyId, java.util.function.Consumer<AtomicInteger> getOrLoad)
            throws Exception {
        AtomicInteger dbLoads = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(STORM_THREADS);
        ExecutorService pool = Executors.newFixedThreadPool(STORM_THREADS);
        for (int t = 0; t < STORM_THREADS; t++) {
            pool.submit(() -> {
                start.await();
                getOrLoad.accept(dbLoads);
                done.countDown();
                return null;
            });
        }
        long t0 = System.nanoTime();
        start.countDown();
        done.await();
        long wallMs = (System.nanoTime() - t0) / 1_000_000;
        pool.shutdownNow();
        System.out.printf("ROW|%-34s|DB 回源 %3d 次|%8d ms%n", name, dbLoads.get(), wallMs);
    }

    private static String fakeJson(AtomicInteger dbLoads, long id) {
        dbLoads.incrementAndGet();
        try {
            TimeUnit.MILLISECONDS.sleep(FAKE_DB_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return json(id);
    }

    private static java.util.function.LongFunction<UserEntity> fakeDb(AtomicInteger dbLoads) {
        return id -> {
            dbLoads.incrementAndGet();
            try {
                TimeUnit.MILLISECONDS.sleep(FAKE_DB_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new UserEntity(id, "v" + id);
        };
    }

    /** 单键失效传播：B 预载 L1 → A evict → 轮询 B L1 清除，收集延迟分位 */
    private static void propagation(String name, long idBase, boolean streams) throws Exception {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        CaffeineChannel bL1 = new CaffeineChannel(10_000);
        String streamKey = "stress:prop-stream:" + idBase;
        InvalidationPublisher publisher = streams
                ? new StreamsInvalidationPublisher(template, streamKey, props.getBroadcast().getStreamsMaxlen())
                : new RedisInvalidationPublisher(template, "stress:prop-pubsub");
        List<AutoCloseable> closables = new ArrayList<>();
        if (streams) {
            StreamsInvalidationConsumer consumer = new StreamsInvalidationConsumer(template,
                    new BroadcastApplier(bL1, registry, ""), streamKey);
            consumer.start();
            closables.add(consumer::stop);
        } else {
            org.springframework.data.redis.listener.RedisMessageListenerContainer container =
                    new org.springframework.data.redis.listener.RedisMessageListenerContainer();
            container.setConnectionFactory(sharedFactory);
            container.afterPropertiesSet();
            container.addMessageListener(new InvalidationSubscriber(bL1, registry, ""),
                    new org.springframework.data.redis.listener.ChannelTopic("stress:prop-pubsub"));
            container.start();
            closables.add(container::destroy);
        }
        // A 实例只负责 evict（DEL + 广播）；B 的 L1 由测试直接预载
        TieredEntityCache a = new TieredEntityCache(props, new CaffeineChannel(1024), new RedisChannel(template),
                publisher, new DoubleDeleteScheduler(Duration.ofMillis(20)));
        try {
            long[] lat = new long[PROPAGATION_ROUNDS];
            for (int i = 0; i < PROPAGATION_ROUNDS; i++) {
                long id = idBase + i;
                String key = keyOf(id);
                bL1.evict(key);
                bL1.put(key, json(id), Duration.ofSeconds(300));
                long t0 = System.nanoTime();
                a.evict(meta, id);
                while (bL1.get(key).hit() && System.nanoTime() - t0 < 5_000_000_000L) {
                    Thread.sleep(1);
                }
                lat[i] = (System.nanoTime() - t0) / 1_000;
                assertThat(bL1.get(key).hit()).as("失效必须送达").isFalse();
            }
            long[] sorted = lat.clone();
            Arrays.sort(sorted);
            System.out.printf("ROW|%-34s|p50 %6d µs|p99 %8d µs|max %8d µs%n",
                    name, sorted[(int) (PROPAGATION_ROUNDS * 0.5)],
                    sorted[(int) (PROPAGATION_ROUNDS * 0.99)], sorted[PROPAGATION_ROUNDS - 1]);
        } finally {
            for (AutoCloseable c : closables) {
                try {
                    c.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static final class DockerClientHolder {
        static boolean isAvailable() {
            return org.testcontainers.DockerClientFactory.instance().isDockerAvailable();
        }
    }
}
