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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;
import java.util.function.LongConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 压测对比（Testcontainers redis:7）：cache-kit vs Spring Cache（RedisCacheManager）vs JetCache。
 *
 * <p>三个场景：
 * 1) 并发读吞吐上限：线程数从 cores/2 扫到 4×cores（每档 0.3s 预热 + 2s 计时），
 *    取最优档位为"上限"；1000 键轮转，op = 一次读缓存调用；
 * 2) 缓存击穿（max(200, 4×cores) 并发）：单冷键回源次数；冷启动场景（100 个冷键 × 4×cores
 *    线程，每线程顺序请求全部键，模拟 20ms DB）统计 DB 总回源次数；
 * 3) 单键失效传播（100 轮，A evict → B L1 清除延迟分位，pubsub/streams）。</p>
 *
 * <p>数值受环境波动影响，量级与相对关系才是重点；复现：mvn test -Dtest=CacheStressComparisonIntegrationTest。</p>
 */
class CacheStressComparisonIntegrationTest {

    private static final int KEYS = 1_000;
    private static final double LEVEL_SECONDS = 2.0;
    private static final double LEVEL_WARMUP_SECONDS = 0.3;
    private static final long FAKE_DB_MILLIS = 50;
    private static final long COLD_START_DB_MILLIS = 20;
    private static final int COLD_START_KEYS = 100;
    private static final int PROPAGATION_ROUNDS = 100;
    /** 延迟采样比例：1/8，控制高吞吐档位的内存占用（分位数统计足够） */
    private static final int LATENCY_SAMPLE = 8;

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory sharedFactory;
    private static LettuceConnectionFactory pooledFactory;
    private static StringRedisTemplate template;
    private static StringRedisTemplate pooledTemplate;
    private static EntityMetadata meta;
    private static int cores;

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
        cores = Runtime.getRuntime().availableProcessors();
        var standalone = new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379));

        sharedFactory = new LettuceConnectionFactory(standalone);
        sharedFactory.afterPropertiesSet();
        template = new StringRedisTemplate(sharedFactory);
        template.afterPropertiesSet();

        // 生产典型配置：Lettuce 连接池（8 连接）
        GenericObjectPoolConfig<io.lettuce.core.api.StatefulConnection<?, ?>> poolConfig =
                new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(8);
        pooledFactory = new LettuceConnectionFactory(standalone,
                LettucePoolingClientConfiguration.builder().poolConfig(poolConfig).build());
        pooledFactory.afterPropertiesSet();
        pooledTemplate = new StringRedisTemplate(pooledFactory);
        pooledTemplate.afterPropertiesSet();

        EntityMetadataRegistry r = new EntityMetadataRegistry();
        meta = r.require(UserEntity.class);
        System.out.printf("===== 环境：%d 逻辑核，redis:7 容器 =====%n", cores);
    }

    /** 传播/订阅路径用的注册表：需已解析实体，广播键匹配才生效 */
    private static EntityMetadataRegistry registry() {
        EntityMetadataRegistry r = new EntityMetadataRegistry();
        r.require(UserEntity.class);
        return r;
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

    // ================= 场景 1：并发读吞吐上限扫描 =================

    private record StressResult(long totalOps, double opsPerSec, long p50Us, long p99Us, long p999Us) {
    }

    private static StressResult runConcurrent(int threads, IntUnaryOperator idPicker, LongConsumer op)
            throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<List<Long>> latencies = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicLong ops = new AtomicLong();
        long warmupNanos = (long) (LEVEL_WARMUP_SECONDS * 1e9);
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                List<Long> lat = new ArrayList<>(32_000);
                start.await();
                long cursor = tid;
                long begin = System.nanoTime();
                while (!stop.get()) {
                    long id = idPicker.applyAsInt((int) (cursor++ % KEYS));
                    long t0 = System.nanoTime();
                    op.accept(id);
                    long cost = System.nanoTime() - t0;
                    long count = ops.incrementAndGet();
                    if (count % LATENCY_SAMPLE == 0) {
                        lat.add(cost / 1_000);
                    }
                    if (System.nanoTime() - begin < warmupNanos) {
                        continue;
                    }
                }
                latencies.add(lat);
                done.countDown();
                return null;
            });
        }
        start.countDown();
        long windowNanos = (long) ((LEVEL_WARMUP_SECONDS + LEVEL_SECONDS) * 1e9);
        long t0 = System.nanoTime();
        while (System.nanoTime() - t0 < windowNanos) {
            Thread.sleep(50);
        }
        stop.set(true);
        done.await();
        pool.shutdownNow();

        int n = latencies.stream().mapToInt(List::size).sum();
        long[] all = new long[Math.max(1, n)];
        int idx = 0;
        for (List<Long> lat : latencies) {
            for (Long l : lat) {
                all[idx++] = l;
            }
        }
        if (idx < all.length) {
            all = Arrays.copyOf(all, Math.max(1, idx));
        }
        Arrays.sort(all);
        return new StressResult(ops.get(), ops.get() / LEVEL_SECONDS,
                percentile(all, 0.50), percentile(all, 0.99), percentile(all, 0.999));
    }

    private static long percentile(long[] sorted, double p) {
        if (sorted.length == 0) {
            return 0;
        }
        return sorted[(int) Math.min(sorted.length - 1, sorted.length * p)];
    }

    private static void printLevel(String name, int threads, StressResult r) {
        System.out.printf("ROW|%-34s|%4d thr|%12.0f ops/s|p50 %5d us|p99 %7d us%n",
                name, threads, r.opsPerSec(), r.p50Us(), r.p99Us());
    }

    /** 线程数从 cores/2 扫到 4×cores，取最优档位为上限 */
    private static StressResult sweep(String name, IntUnaryOperator pick, LongConsumer op) throws Exception {
        int[] levels = {Math.max(2, cores / 2), cores, cores * 2, cores * 4};
        StressResult best = null;
        int bestThreads = 0;
        for (int threads : levels) {
            StressResult r = runConcurrent(threads, pick, op);
            printLevel(name, threads, r);
            if (best == null || r.opsPerSec() > best.opsPerSec()) {
                best = r;
                bestThreads = threads;
            }
        }
        System.out.printf("CEIL|%-34s|上限 %12.0f ops/s @ %d thr (p99 %d us)%n",
                name, best.opsPerSec(), bestThreads, best.p99Us());
        return best;
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
        // ---- JetCache：仅远程 + 多级（Caffeine+Redis），实例在主线程构建、线程共享 ----
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

        System.out.println("===== 场景1 并发读吞吐上限扫描（thr 从 cores/2 到 4×cores，每档 0.3s 预热 + 2s 计时） =====");
        StressResult kitL1 = sweep("cache-kit 三级(L1 命中)", pick,
                id -> l1Cache.load(meta, id, null, true, () -> {
                    throw new AssertionError("不应回源");
                }));
        StressResult jetMultiRes = sweep("JetCache 多级(Caffeine+Redis)", pick,
                id -> {
                    if (jetMulti.get((long) id) == null) {
                        throw new AssertionError("应命中");
                    }
                });
        sweep("Spring Cache(仅Redis,池8)", pick, id -> spring.get(id));
        sweep("JetCache 仅远程", pick, id -> {
            if (jetRemote.get((long) id) == null) {
                throw new AssertionError("应命中");
            }
        });
        sweep("cache-kit 仅L2(单连接)", pick,
                id -> l2Only.load(meta, id, null, true, () -> {
                    throw new AssertionError("不应回源");
                }));
        sweep("裸 Redis GET(池8)", pick, id -> pooledTemplate.opsForValue().get("stress:raw:" + id));

        // ================= 场景 2：单冷键击穿 =================
        int stormThreads = Math.max(200, cores * 4);
        System.out.println("===== 场景2 单冷键击穿（" + stormThreads + " 并发 × 1 冷键，模拟 50ms DB） =====");
        // 每行独立键空间与独立缓存实例（主线程构建、线程共享——single-flight/防穿透都是实例内语义）
        TieredEntityCache stormCache = cacheKit(new CaffeineChannel(10_000));
        storm("cache-kit single-flight", stormThreads, 901_000, loads ->
                stormCache.load(meta, 901_000, null, true, () -> fakeDb(loads, 901_000L, FAKE_DB_MILLIS)));
        storm("Spring Cache(默认)", stormThreads, 902_000, loads ->
                spring.get(902_000, () -> fakeDb(loads, 902_000L, FAKE_DB_MILLIS)));
        RedisClient jetClient2 = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        StatefulRedisConnection<String, String> jetConn2 = (StatefulRedisConnection<String, String>)
                jetClient2.connect(new com.alicp.jetcache.redis.lettuce.JetCacheCodec());
        com.alicp.jetcache.Cache<Long, String> jetDefault =
                remoteJet(jetClient2, jetConn2, jetMgr, "storm:jetd:", false);
        storm("JetCache(默认)", stormThreads, 903_000, loads -> {
            try {
                jetDefault.computeIfAbsent(903_000L, k -> fakeJson(loads, k, FAKE_DB_MILLIS));
            } catch (com.alicp.jetcache.CacheInvokeException e) {
                throw new IllegalStateException(e);
            }
        });
        RedisClient jetClient3 = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        StatefulRedisConnection<String, String> jetConn3 = (StatefulRedisConnection<String, String>)
                jetClient3.connect(new com.alicp.jetcache.redis.lettuce.JetCacheCodec());
        com.alicp.jetcache.Cache<Long, String> jetProtect =
                remoteJet(jetClient3, jetConn3, jetMgr, "storm:jets:", true);
        storm("JetCache(penetrationProtect)", stormThreads, 904_000, loads -> {
            try {
                jetProtect.computeIfAbsent(904_000L, k -> fakeJson(loads, k, FAKE_DB_MILLIS));
            } catch (com.alicp.jetcache.CacheInvokeException e) {
                throw new IllegalStateException(e);
            }
        });
        jetClient3.shutdown();
        jetClient2.shutdown();

        // 冷启动回源放大：4×cores 并发各顺序请求 100 个冷键（模拟 20ms DB），统计 DB 总回源
        System.out.println("===== 场景2b 冷启动回源放大（" + cores * 4 + " 并发 × " + COLD_START_KEYS
                + " 冷键/线程，模拟 20ms DB） =====");
        int csThreads = cores * 4;
        TieredEntityCache csCache = cacheKit(new CaffeineChannel(10_000));
        stormMulti("cache-kit", csThreads, 920_000, COLD_START_KEYS, (key, loads) ->
                csCache.load(meta, key, null, true, () -> fakeDb(loads, key, COLD_START_DB_MILLIS)));
        stormMulti("Spring Cache(默认)", csThreads, 930_000, COLD_START_KEYS, (key, loads) ->
                spring.get(key, () -> fakeDb(loads, key, COLD_START_DB_MILLIS)));
        stormMulti("JetCache(默认)", csThreads, 940_000, COLD_START_KEYS, (key, loads) -> {
            try {
                jetDefault.computeIfAbsent(key, k -> fakeJson(loads, k, COLD_START_DB_MILLIS));
            } catch (com.alicp.jetcache.CacheInvokeException e) {
                throw new IllegalStateException(e);
            }
        });
        stormMulti("JetCache(penetrationProtect)", csThreads, 950_000, COLD_START_KEYS, (key, loads) -> {
            try {
                jetProtect.computeIfAbsent(key, k -> fakeJson(loads, k, COLD_START_DB_MILLIS));
            } catch (com.alicp.jetcache.CacheInvokeException e) {
                throw new IllegalStateException(e);
            }
        });

        // ================= 场景 3：单键失效传播 =================
        System.out.println("===== 场景3 单键失效传播（A evict → B L1 清除，100 轮） =====");
        propagation("cache-kit pubsub", 1_000, false);
        propagation("cache-kit streams", 2_000, true);

        assertThat(kitL1.totalOps()).isPositive();
        assertThat(jetMultiRes.totalOps()).isPositive();
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

    /** 单冷键击穿：threads 并发打同一键，统计 DB 回源次数与总耗时。getOrLoad 执行一次"读缺失则回源" */
    private static void storm(String name, int threads, long keyId,
                              Consumer<AtomicInteger> getOrLoad) throws Exception {
        AtomicInteger dbLoads = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
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
        System.out.printf("ROW|%-34s|DB 回源 %4d 次|%8d ms%n", name, dbLoads.get(), wallMs);
    }

    /** 冷启动回源放大：threads 并发各顺序请求 keyCount 个冷键，统计 DB 总回源次数与总耗时 */
    private static void stormMulti(String name, int threads, long keyBase, int keyCount,
                                   BiConsumer<Long, AtomicInteger> getOrLoadOne) throws Exception {
        AtomicInteger dbLoads = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                start.await();
                for (int k = 0; k < keyCount; k++) {
                    getOrLoadOne.accept(keyBase + k, dbLoads);
                }
                done.countDown();
                return null;
            });
        }
        long t0 = System.nanoTime();
        start.countDown();
        done.await();
        long wallMs = (System.nanoTime() - t0) / 1_000_000;
        pool.shutdownNow();
        System.out.printf("ROW|%-34s|DB 回源 %6d 次|%8d ms%n", name, dbLoads.get(), wallMs);
    }

    private static UserEntity fakeDb(AtomicInteger dbLoads, long id, long millis) {
        dbLoads.incrementAndGet();
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return new UserEntity(id, "v" + id);
    }

    private static String fakeJson(AtomicInteger dbLoads, long id, long millis) {
        dbLoads.incrementAndGet();
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return json(id);
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
                    new BroadcastApplier(bL1, registry(), ""), streamKey);
            consumer.start();
            closables.add(consumer::stop);
        } else {
            org.springframework.data.redis.listener.RedisMessageListenerContainer container =
                    new org.springframework.data.redis.listener.RedisMessageListenerContainer();
            container.setConnectionFactory(sharedFactory);
            container.afterPropertiesSet();
            container.addMessageListener(new InvalidationSubscriber(bL1, registry(), ""),
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
            System.out.printf("ROW|%-34s|p50 %6d us|p99 %8d us|max %8d us%n",
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
