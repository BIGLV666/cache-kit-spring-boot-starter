package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CacheChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.exception.IdMisfireException;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        cache.evictBatch(meta, List.of(1L, 2L));

        assertThat(l1.store).doesNotContainKey("user_entity:1");
        assertThat(l1.store).doesNotContainKey("user_entity:2");
        assertThat(publisher.published).contains("user_entity:1", "user_entity:2");
    }

    @Test
    void batchEvictThenLoadMustReloadAfterBatch() {
        // 回归钉子：批量回源完成后 inflight future 必须清理——
        // 残留的已完成 future 会让 evict 后的 load 永远拿到旧值（历史真实缺陷，现有并发测试抓不住）
        AtomicInteger loadCount = new AtomicInteger();
        java.util.function.Function<List<Object>, List<Object>> loader = missing -> {
            loadCount.incrementAndGet();
            return missing.stream()
                    .map(id -> new UserEntity((Long) id, "v" + id))
                    .<Object>map(x -> x)
                    .toList();
        };

        cache.loadBatch(meta, List.of(1L), true, null, loader);
        cache.evictBatch(meta, List.of(1L));
        cache.loadBatch(meta, List.of(1L), true, null, loader);

        assertThat(loadCount.get()).as("evict 后批量读必须重新回源").isEqualTo(2);
    }

    @Test
    void strictBatchShouldRejectNonPrimaryKeyValues() {
        // 严格模式守卫：请求值与回源结果主键对不上（把手机号当 ID）→
        // 抛 IdMisfireException 携带原始结果，绝不返回空数据、不写错键占位
        java.util.function.Function<List<Object>, List<Object>> loader = missing ->
                List.<Object>of(new UserEntity(7L, "owner"));

        assertThatThrownBy(() -> cache.loadBatch(meta, List.of("13800001111"), true, null, loader, true))
                .isInstanceOf(IdMisfireException.class)
                .satisfies(e -> assertThat(((IdMisfireException) e).getLoadedEntities())
                        .containsExactly(new UserEntity(7L, "owner")));

        assertThat(l1.store).doesNotContainKey("user_entity:13800001111");
        assertThat(l2.store).doesNotContainKey("user_entity:13800001111");
    }

    @Test
    void strictBatchShouldRejectWhenResultIsEmpty() {
        // 严格守卫扩展到空结果（0.3.0）：请求值全部查不到时同样无法证明是主键集合，
        // 抛误判旁路而不是写错键 null 占位——占位键永远不会被该行后续 insert 的失效命中
        java.util.function.Function<List<Object>, List<Object>> loader = missing -> List.of();

        assertThatThrownBy(() -> cache.loadBatch(meta, List.of("13800001111"), true, null, loader, true))
                .isInstanceOf(IdMisfireException.class);

        assertThat(l1.store).doesNotContainKey("user_entity:13800001111");
        assertThat(l2.store).doesNotContainKey("user_entity:13800001111");
    }

    @Test
    void strictBatchShouldPassWhenValuesArePrimaryKeys() {
        java.util.function.Function<List<Object>, List<Object>> loader = missing ->
                missing.stream()
                        .map(id -> new UserEntity((Long) id, "v" + id))
                        .<Object>map(x -> x)
                        .toList();

        Object[] out = cache.loadBatch(meta, List.of(1L, 2L), true, null, loader, true);

        assertThat(out[0]).isEqualTo(new UserEntity(1L, "v1"));
        assertThat(out[1]).isEqualTo(new UserEntity(2L, "v2"));
        assertThat(l1.store).containsKey("user_entity:1");
        assertThat(l1.store).containsKey("user_entity:2");
    }

    @Test
    void customizerThrowingShouldNotBreakChain() {
        // CacheKeyCustomizer.segment() 抛异常：按"无段"降级，单条/批量/失效主流程全部照常
        //（历史上批量注册路径会被 SPI 异常打断并永久挂死后续读）
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        InMemoryChannel l1c = new InMemoryChannel();
        TieredEntityCache guarded = new TieredEntityCache(props, l1c, new InMemoryChannel(),
                new NoopInvalidationPublisher(), new DoubleDeleteScheduler(Duration.ofMillis(50)),
                "", List.<CacheKeyCustomizer>of(() -> {
                    throw new IllegalStateException("no tenant ctx");
                }));
        EntityMetadata m = new EntityMetadataRegistry().require(UserEntity.class);

        Object v = guarded.load(m, 1L, null, true, () -> new UserEntity(1L, "lv"));
        assertThat(v).isEqualTo(new UserEntity(1L, "lv"));
        assertThat(l1c.store).containsKey("user_entity:1");

        Object[] batch = guarded.loadBatch(m, List.of(2L, 3L), true, null,
                missing -> missing.stream()
                        .map(id -> new UserEntity((Long) id, "b"))
                        .<Object>map(x -> x)
                        .toList());
        assertThat(batch).hasSize(2);

        guarded.evictBatch(m, List.of(2L));
        AtomicInteger reload = new AtomicInteger();
        guarded.loadBatch(m, List.of(2L), true, null, missing -> {
            reload.incrementAndGet();
            return List.of(new UserEntity(2L, "b2"));
        });
        assertThat(reload.get()).as("失效后必须重新回源（inflight 不残留）").isEqualTo(1);
    }

    @Test
    void waitersShouldGetDistinctInstances() throws Exception {
        // single-flight 等待者必须各自 decode 出新实例，不能共享同一可变对象
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(threads);
        List<CompletableFuture<Object>> futures = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                start.countDown();
                try {
                    start.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
                return cache.load(meta, 9L, null, true, () -> {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                    }
                    return new UserEntity(9L, "shared");
                });
            }, pool));
        }
        start.await(5, TimeUnit.SECONDS);
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
        pool.shutdownNow();

        java.util.Set<Object> identities = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (CompletableFuture<Object> f : futures) {
            identities.add(f.join());
        }
        assertThat(identities).as("并发等待者应拿到各自独立实例").hasSize(threads);
    }

    @Test
    void l1TtlShouldNotExceedEntityTtlOverride() {
        // 实体级 ttl=2s < 全局 l1.ttl=30s：L1 回填 TTL 必须取较小值，防止"L2 已过期、L1 仍旧"倒装
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        Map<String, Duration> l1Ttls = new ConcurrentHashMap<>();
        CacheChannel recordingL1 = new InMemoryChannel() {
            @Override
            public void put(String key, String json, Duration ttl) {
                super.put(key, json, ttl);
                l1Ttls.put(key, ttl);
            }
        };
        TieredEntityCache ttlCache = new TieredEntityCache(props, recordingL1, new InMemoryChannel(),
                new NoopInvalidationPublisher(), new DoubleDeleteScheduler(Duration.ofMillis(50)));
        EntityMetadata ttlMeta = new EntityMetadataRegistry().require(
                io.github.biglv666.cachekit.model.TtlEntity.class);

        ttlCache.load(ttlMeta, 1L, null, true, () -> new io.github.biglv666.cachekit.model.TtlEntity(1L, "lv"));

        assertThat(l1Ttls.get("ttl_entity:1")).isEqualTo(Duration.ofSeconds(2));
    }

    @Test
    void nullIdShouldNotTouchCache() {
        // null 主键防 "前缀:null" 键与字面 "null" 主键冲突：load 拒绝、peek/batch 按未命中
        assertThatThrownBy(() -> cache.load(meta, null, null, true, () -> new UserEntity(1L, "x")))
                .isInstanceOf(CacheKitException.class);
        assertThat(cache.peek(meta, null).state()).isEqualTo(TieredEntityCache.CachePeek.State.MISS);
        assertThat(cache.peekBatch(meta, java.util.Arrays.asList(1L, null)).get(1).state())
                .isEqualTo(TieredEntityCache.CachePeek.State.MISS);

        Object[] out = cache.loadBatch(meta, java.util.Arrays.asList(1L, null), true, null,
                missing -> missing.stream()
                        .map(id -> new UserEntity((Long) id, "v"))
                        .<Object>map(x -> x)
                        .toList());
        assertThat(out[0]).isNotNull();
        assertThat(out[1]).isNull();
        assertThat(l1.store).doesNotContainKey("user_entity:null");
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

    @Test
    void evictBatchShouldDeduplicateKeys() {
        // binlog UPDATE 双镜像主键相同 / 调用方传重复 ID：DEL + 广播去重，命令数减半；
        // 双删调度器传 null，避免第二次删除干扰计数
        TieredEntityCache noDoubleDelete = new TieredEntityCache(new CacheKitProperties(), l1, l2,
                publisher, null);

        noDoubleDelete.evictBatch(meta, List.of(1L, 1L, 2L));

        assertThat(publisher.published.stream().filter("user_entity:1"::equals).count())
                .as("重复 ID 只广播一次").isEqualTo(1);
        assertThat(publisher.published.stream().filter("user_entity:2"::equals).count()).isEqualTo(1);
    }

    @Test
    void l2EvictFailureShouldBeRetried() throws Exception {
        // DEL 恰好落在 Redis 闪断窗口内 → 旧值滞留 L2：失效路径必须按双删延迟自动重试
        AtomicInteger evictAttempts = new AtomicInteger();
        CacheChannel flakyL2 = new InMemoryChannel() {
            @Override
            public boolean evictAll(java.util.Collection<String> keys) {
                evictAttempts.incrementAndGet();
                throw new IllegalStateException("redis down");
            }
        };
        TieredEntityCache flaky = new TieredEntityCache(new CacheKitProperties(), l1, flakyL2,
                new NoopInvalidationPublisher(), new DoubleDeleteScheduler(Duration.ofMillis(50)));

        flaky.evictBatch(meta, List.of(1L));

        long deadline = System.currentTimeMillis() + 2000;
        while (evictAttempts.get() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(evictAttempts.get())
                .as("L2 删除失败必须重试（初始 1 次 + 至少 1 次重试）")
                .isGreaterThanOrEqualTo(2);
    }

    @Test
    void decodeFailureShouldNotEvictButReloadOverwrites() {
        // 结构漂移坏值：按未命中回源，回源成功 putBoth 覆盖坏值（0.3.0 起不再逐次 DEL+广播），
        // 并发读同一坏键不会形成失效风暴
        l1.store.put("user_entity:1", "{corrupt");
        l2.store.put("user_entity:1", "{corrupt");
        AtomicInteger loaderCount = new AtomicInteger();

        Object value = cache.load(meta, 1L, null, true, () -> {
            loaderCount.incrementAndGet();
            return new UserEntity(1L, "lv");
        });

        assertThat(value).isEqualTo(new UserEntity(1L, "lv"));
        assertThat(loaderCount.get()).isEqualTo(1);
        assertThat(l1.store.get("user_entity:1")).contains("userName");
        assertThat(l2.store.get("user_entity:1")).contains("userName");
        assertThat(publisher.published).as("坏值由回源覆盖，无需广播失效").isEmpty();
    }
}
