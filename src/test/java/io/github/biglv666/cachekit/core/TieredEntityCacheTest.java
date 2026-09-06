package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
}
