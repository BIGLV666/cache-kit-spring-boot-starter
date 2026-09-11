package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CacheChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TieredEntityCacheTest {

    private InMemoryChannel l1;
    private InMemoryChannel l2;
    private RecordingPublisher publisher;
    private TieredEntityCache cache;
    private EntityMetadata meta;

    /** 记录发布内容的假广播器 */
    static class RecordingPublisher implements InvalidationPublisher {
        final List<String> published = new CopyOnWriteArrayList<>();

        @Override
        public void publish(String key) {
            published.add(key);
        }
    }

    @BeforeEach
    void setUp() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        props.getL2().setDoubleDeleteDelay(Duration.ofMillis(50));
        l1 = new InMemoryChannel();
        l2 = new InMemoryChannel();
        publisher = new RecordingPublisher();
        cache = new TieredEntityCache(props, l1, l2, publisher, new DoubleDeleteScheduler(props.getL2().getDoubleDeleteDelay()));
        meta = new EntityMetadataRegistry().require(UserEntity.class);
    }

    @AfterEach
    void tearDown() {
        // DoubleDeleteScheduler 的守护线程不阻塞 JVM 退出，无需显式关闭
    }

    @Test
    void loaderShouldExecuteOnceForRepeatedGets() {
        AtomicInteger loaderCount = new AtomicInteger();
        Object first = cache.load(meta, 1L, null, true, () -> {
            loaderCount.incrementAndGet();
            return new UserEntity(1L, "lv");
        });
        Object second = cache.load(meta, 1L, null, true, () -> {
            loaderCount.incrementAndGet();
            return new UserEntity(1L, "lv");
        });

        assertThat(loaderCount.get()).isEqualTo(1);
        assertThat(first).isEqualTo(second);
        assertThat(l1.store).containsKey("user_entity:1");
        assertThat(l2.store).containsKey("user_entity:1");
    }

    @Test
    void l2HitShouldBackfillL1WithoutLoader() {
        // 预置 L2，L1 为空：应直接从 L2 命中并回填 L1
        l2.store.put("user_entity:1", "{\"userId\":1,\"userName\":\"lv\"}");
        AtomicInteger loaderCount = new AtomicInteger();

        Object value = cache.load(meta, 1L, null, true, () -> {
            loaderCount.incrementAndGet();
            return null;
        });

        assertThat(loaderCount.get()).isZero();
        assertThat(value).isEqualTo(new UserEntity(1L, "lv"));
        assertThat(l1.store).containsKey("user_entity:1");
    }

    @Test
    void nullResultShouldBeCachedWhenCacheNullEnabled() {
        AtomicInteger loaderCount = new AtomicInteger();
        cache.load(meta, 404L, null, true, () -> {
            loaderCount.incrementAndGet();
            return null;
        });
        cache.load(meta, 404L, null, true, () -> {
            loaderCount.incrementAndGet();
            return null;
        });

        assertThat(loaderCount.get()).isEqualTo(1);
        assertThat(l1.store.get("user_entity:404")).isEqualTo("__cache_kit_null__");
        assertThat(l2.store.get("user_entity:404")).isEqualTo("__cache_kit_null__");
    }

    @Test
    void nullResultShouldNotBeCachedWhenCacheNullDisabled() {
        AtomicInteger loaderCount = new AtomicInteger();
        cache.load(meta, 404L, null, false, () -> {
            loaderCount.incrementAndGet();
            return null;
        });
        cache.load(meta, 404L, null, false, () -> {
            loaderCount.incrementAndGet();
            return null;
        });

        assertThat(loaderCount.get()).isEqualTo(2);
        assertThat(l1.store).doesNotContainKey("user_entity:404");
    }

    @Test
    void evictShouldForceReloadAndBroadcast() {
        AtomicInteger loaderCount = new AtomicInteger();
        cache.load(meta, 1L, null, true, () -> {
            loaderCount.incrementAndGet();
            return new UserEntity(1L, "lv");
        });

        cache.evict(meta, 1L);
        assertThat(l1.store).doesNotContainKey("user_entity:1");
        assertThat(l2.store).doesNotContainKey("user_entity:1");
        assertThat(publisher.published).contains("user_entity:1");

        cache.load(meta, 1L, null, true, () -> {
            loaderCount.incrementAndGet();
            return new UserEntity(1L, "lv");
        });
        assertThat(loaderCount.get()).isEqualTo(2);
    }

    @Test
    void concurrentLoadShouldCoalesceToSingleLoaderCall() throws Exception {
        AtomicInteger loaderCount = new AtomicInteger();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startBarrier = new CountDownLatch(threads);
        List<CompletableFuture<Object>> futures = new CopyOnWriteArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                startBarrier.countDown();
                try {
                    startBarrier.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
                return cache.load(meta, 9L, null, true, () -> {
                    loaderStarted.countDown();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                    }
                    loaderCount.incrementAndGet();
                    return new UserEntity(9L, "concurrent");
                });
            }, pool));
        }
        startBarrier.await(5, TimeUnit.SECONDS);
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
        pool.shutdownNow();

        assertThat(loaderCount.get()).isEqualTo(1);
        for (CompletableFuture<Object> f : futures) {
            assertThat(f.join()).isEqualTo(new UserEntity(9L, "concurrent"));
        }
    }

    @Test
    void bypassContextShouldSkipCache() {
        l1.store.put("user_entity:1", "{\"userId\":1,\"userName\":\"cached\"}");
        AtomicInteger loaderCount = new AtomicInteger();

        Object result = CacheKit.withDb(() -> cache.load(meta, 1L, null, true, () -> {
            loaderCount.incrementAndGet();
            return new UserEntity(1L, "fresh");
        }));

        assertThat(loaderCount.get()).isEqualTo(1);
        assertThat(result).isEqualTo(new UserEntity(1L, "fresh"));
        // 旁路只影响读路径，不触碰已有缓存
        assertThat(l1.store).containsKey("user_entity:1");
    }

    @Test
    void loaderFailureShouldNotBeCached() {
        AtomicInteger loaderCount = new AtomicInteger();
        for (int i = 0; i < 2; i++) {
            try {
                cache.load(meta, 500L, null, true, () -> {
                    loaderCount.incrementAndGet();
                    throw new IllegalStateException("db down");
                });
            } catch (RuntimeException ignored) {
                // 预期：异常向上抛
            }
        }
        assertThat(loaderCount.get()).isEqualTo(2);
    }

    @Test
    void redisDownShouldDegradeGracefully() {
        // L2 通道抛异常（Redis 宕机模拟）：读不失败、写不失败、失效不失败
        CacheChannel brokenL2 = new InMemoryChannel() {
            @Override
            public io.github.biglv666.cachekit.channel.CacheEntry get(String key) {
                throw new IllegalStateException("redis down");
            }

            @Override
            public void put(String key, String json, Duration ttl) {
                throw new IllegalStateException("redis down");
            }

            @Override
            public void evict(String key) {
                throw new IllegalStateException("redis down");
            }
        };
        TieredEntityCache degraded = new TieredEntityCache(new CacheKitProperties(), l1, brokenL2,
                publisher, new DoubleDeleteScheduler(Duration.ofMillis(50)));
        AtomicInteger loaderCount = new AtomicInteger();

        Object first = degraded.load(meta, 1L, null, true, () -> {
            loaderCount.incrementAndGet();
            return new UserEntity(1L, "lv");
        });
        // 第二次读：L2 故障不影响 L1 命中（L1 正常工作），不应抛异常
        Object second = degraded.load(meta, 1L, null, true, () -> {
            loaderCount.incrementAndGet();
            return new UserEntity(1L, "lv");
        });
        degraded.evict(meta, 1L);
        degraded.peekBatch(meta, List.of(1L, 2L));

        assertThat(first).isEqualTo(new UserEntity(1L, "lv"));
        assertThat(second).isEqualTo(new UserEntity(1L, "lv"));
        // L2 put 失败被吞掉后，L1 仍有值：第二次读命中 L1，无需回源
        assertThat(loaderCount.get()).as("L1 正常时第二次读命中 L1").isEqualTo(1);
    }

    @Test
    void serializationFailureShouldNotLoseData() {
        EntityMetadata badMeta = new EntityMetadataRegistry()
                .require(io.github.biglv666.cachekit.model.UnserializableEntity.class);
        AtomicInteger loaderCount = new AtomicInteger();

        Object first = cache.load(badMeta, 1L, null, true, () -> {
            loaderCount.incrementAndGet();
            return newUnserializable(1L);
        });
        System.out.println("[DBG] l1 after first load = " + l1.store + " , first=" + first);
        Object second = cache.load(badMeta, 1L, null, true, () -> {
            loaderCount.incrementAndGet();
            return newUnserializable(1L);
        });

        // 序列化失败不抛异常、不缓存，但业务结果必须原样返回（实体无 equals，按非空断言）
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(loaderCount.get()).as("不可序列化 → 不缓存，每次回源").isEqualTo(2);
        assertThat(l1.store).doesNotContainKey("unserializable_entity:1");
    }

    private io.github.biglv666.cachekit.model.UnserializableEntity newUnserializable(Long id) {
        io.github.biglv666.cachekit.model.UnserializableEntity e =
                new io.github.biglv666.cachekit.model.UnserializableEntity();
        e.setId(id);
        e.setSelf(e);   // 自引用：Jackson 无限递归，序列化必然失败
        return e;
    }

    @Test
    void peekBatchShouldReturnThreeStatesInOrder() {
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "lv"));       // 1 → 有值
        cache.load(meta, 2L, null, true, () -> null);                            // 2 → null 占位

        List<TieredEntityCache.CachePeek> peeks = cache.peekBatch(meta, List.of(1L, 2L, 3L));
        assertThat(peeks.get(0).state()).isEqualTo(TieredEntityCache.CachePeek.State.HIT);
        assertThat(peeks.get(1).state()).isEqualTo(TieredEntityCache.CachePeek.State.HIT_NULL);
        assertThat(peeks.get(2).state()).isEqualTo(TieredEntityCache.CachePeek.State.MISS);
    }

    @Test
    void evictBatchShouldCoalesceDoubleDelete() {
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "a"));
        cache.load(meta, 2L, null, true, () -> new UserEntity(2L, "b"));

        cache.evictBatch(meta, List.of(1L, 2L), true);

        assertThat(l1.store).doesNotContainKey("user_entity:1");
        assertThat(l1.store).doesNotContainKey("user_entity:2");
        assertThat(publisher.published).contains("user_entity:1", "user_entity:2");
    }

    @Test
    void concurrentBatchMissesShouldCoalescePerId() throws Exception {
        // 独立实例：干净缓存 + 独立通道
        InMemoryChannel l1b = new InMemoryChannel();
        InMemoryChannel l2b = new InMemoryChannel();
        TieredEntityCache batchCache = new TieredEntityCache(new CacheKitProperties(), l1b, l2b,
                new NoopInvalidationPublisher(), new DoubleDeleteScheduler(Duration.ofMillis(50)));
        EntityMetadata m = new EntityMetadataRegistry().require(UserEntity.class);

        // 每个 ID 的实际回源次数（跨所有批量调用）：single-flight 保证 == 1
        Map<Long, AtomicInteger> perIdLoads = new ConcurrentHashMap<>();
        java.util.function.Function<List<Object>, List<Object>> loader = missingIds -> {
            for (Object id : missingIds) {
                perIdLoads.computeIfAbsent((Long) id, k -> new AtomicInteger()).incrementAndGet();
            }
            sleepQuietly(50);
            return missingIds.stream()
                    .filter(id -> ((Long) id) == 1L)
                    .<Object>map(id -> new UserEntity(1L, "only"))
                    .toList();
        };

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(threads);
        List<CompletableFuture<Object[]>> futures = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                start.countDown();
                try {
                    start.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
                return batchCache.loadBatch(m, List.of(1L, 2L, 3L), true, null, loader);
            }, pool));
        }
        start.await(5, TimeUnit.SECONDS);
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(15, TimeUnit.SECONDS);
        pool.shutdownNow();

        // 核心保证：每个缺失 ID 至多回源一次（无论 8 个线程怎么并发到达）
        assertThat(perIdLoads.get(1L).get()).as("ID=1 回源次数").isEqualTo(1);
        assertThat(perIdLoads.get(2L).get()).as("ID=2 回源次数").isEqualTo(1);
        assertThat(perIdLoads.get(3L).get()).as("ID=3 回源次数").isEqualTo(1);
        // 所有线程拿到一致结果：只有 1 存在，2/3 为 null 占位
        for (CompletableFuture<Object[]> f : futures) {
            Object[] result = f.join();
            assertThat(result[0]).isEqualTo(new UserEntity(1L, "only"));
            assertThat(result[1]).isNull();
            assertThat(result[2]).isNull();
        }
    }

    @Test
    void namespaceAndCustomizerShouldPrefixKeys() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        props.setKeyNamespace("db1");
        InMemoryChannel l1c = new InMemoryChannel();
        TieredEntityCache scoped = new TieredEntityCache(props, l1c, new InMemoryChannel(),
                new NoopInvalidationPublisher(), new DoubleDeleteScheduler(Duration.ofMillis(50)),
                "db1", List.of(() -> "tenantA"));

        EntityMetadata m = new EntityMetadataRegistry().require(UserEntity.class);
        scoped.load(m, 1L, null, true, () -> new UserEntity(1L, "lv"));

        assertThat(l1c.store).containsKey("tenantA:db1:user_entity:1");
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }
}
