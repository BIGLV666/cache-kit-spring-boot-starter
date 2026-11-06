package io.github.biglv666.cachekit.generic;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import io.github.biglv666.cachekit.channel.CacheChannel;
import io.github.biglv666.cachekit.channel.CacheEntry;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.CacheKit;
import io.github.biglv666.cachekit.core.GenericKeyCodec;
import io.github.biglv666.cachekit.core.InvalidationPublisher;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.support.JsonCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TieredValueCacheTest {

    private CacheKitProperties props;
    private CaffeineChannel l1;
    private TieredValueCache cache;
    private GenericCacheMetadata meta;
    private JavaType valueType;

    @BeforeEach
    void setUp() {
        props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        l1 = new CaffeineChannel(1024);
        cache = new TieredValueCache(props, l1, null, new NoopInvalidationPublisher(), null, "", List.of());
        meta = new GenericCacheMetadata(TestValue.class, "app-config", null, true);
        valueType = JsonCodec.constructType(TestValue.class);
    }

    @Test
    void loadShouldHitCacheAndSkipLoader() {
        AtomicInteger loads = new AtomicInteger();

        Object first = cache.load(meta, "k", valueType, null, true, () -> {
            loads.incrementAndGet();
            return new TestValue("v", 1);
        });
        Object second = cache.load(meta, "k", valueType, null, true, () -> {
            loads.incrementAndGet();
            return new TestValue("v", 2);
        });

        assertThat(first).isEqualTo(new TestValue("v", 1));
        assertThat(second).isEqualTo(new TestValue("v", 1));
        assertThat(loads.get()).isEqualTo(1);
        assertThat(cache.peek(meta, "k", valueType)).isEqualTo(new TestValue("v", 1));
    }

    @Test
    void nullResultShouldBeCachedWhenEnabled() {
        AtomicInteger loads = new AtomicInteger();

        assertThat(cache.load(meta, "absent", valueType, null, true, () -> {
            loads.incrementAndGet();
            return null;
        })).isNull();
        assertThat(cache.load(meta, "absent", valueType, null, true, () -> {
            loads.incrementAndGet();
            return new TestValue("late", 1);
        })).isNull();

        assertThat(loads.get()).isEqualTo(1);
    }

    @Test
    void putShouldUpdateAndEvictShouldRemove() {
        cache.put(meta, "k", new TestValue("put", 7), null);
        assertThat(cache.peek(meta, "k", valueType)).isEqualTo(new TestValue("put", 7));

        cache.evict(meta, "k");
        assertThat(cache.peek(meta, "k", valueType)).isNull();

        AtomicInteger loads = new AtomicInteger();
        Object loaded = cache.load(meta, "k", valueType, null, true, () -> {
            loads.incrementAndGet();
            return new TestValue("reload", 8);
        });
        assertThat(loaded).isEqualTo(new TestValue("reload", 8));
        assertThat(loads.get()).isEqualTo(1);
    }

    @Test
    void genericListValueShouldRoundTrip() {
        JavaType listType = JsonCodec.constructType(new TypeReference<List<TestValue>>() {
        }.getType());
        List<TestValue> value = List.of(new TestValue("a", 1), new TestValue("b", 2));

        Object first = cache.load(meta, "list", listType, null, true, () -> value);
        Object second = cache.load(meta, "list", listType, null, true, () -> List.of());

        assertThat(first).isEqualTo(value);
        assertThat(second).isEqualTo(value);
    }

    @Test
    void concurrentColdReadsShouldLoadOnce() throws Exception {
        int threads = 32;
        AtomicInteger loads = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Object>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return cache.load(meta, "hot", valueType, null, true, () -> {
                        loads.incrementAndGet();
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return new TestValue("hot", 1);
                    });
                }));
            }
            start.countDown();
            for (Future<Object> future : futures) {
                assertThat(future.get(5, TimeUnit.SECONDS)).isEqualTo(new TestValue("hot", 1));
            }
        } finally {
            pool.shutdownNow();
        }
        assertThat(loads.get()).isEqualTo(1);
        assertThat(cache.inflightCount()).isZero();
    }

    @Test
    void checkedExceptionShouldPropagateAndNotBeCached() {
        AtomicInteger loads = new AtomicInteger();

        assertThatThrownBy(() -> cache.loadChecked(meta, "fail", valueType, null, true, () -> {
            loads.incrementAndGet();
            throw new IOException("io down");
        })).isInstanceOf(IOException.class).hasMessage("io down");

        assertThat(loads.get()).isEqualTo(1);
        assertThat(cache.inflightCount()).isZero();
        assertThat(cache.peek(meta, "fail", valueType)).isNull();
    }

    @Test
    void runtimeExceptionShouldPropagateUnchangedThroughSupplierPath() {
        CacheKitException failure = new CacheKitException("runtime");

        assertThatThrownBy(() -> cache.load(meta, "fail", valueType, null, true, () -> {
            throw failure;
        })).isSameAs(failure);
    }

    @Test
    void singleFlightWaiterShouldReceiveOriginalCheckedException() throws Exception {
        IOException failure = new IOException("boom");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Throwable> leader = pool.submit(() -> loadFailure(failure, started, release, loads));
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            Future<Throwable> waiter = pool.submit(() -> loadFailure(failure, started, release, loads));
            Thread.sleep(100);
            release.countDown();

            assertThat(leader.get(5, TimeUnit.SECONDS)).isSameAs(failure);
            assertThat(waiter.get(5, TimeUnit.SECONDS)).isSameAs(failure);
            assertThat(loads.get()).as("等待者不得重复回源").isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
        assertThat(cache.inflightCount()).isZero();
    }

    private Throwable loadFailure(IOException failure, CountDownLatch started,
                                  CountDownLatch release, AtomicInteger loads) {
        try {
            cache.loadChecked(meta, "fail", valueType, null, true, () -> {
                loads.incrementAndGet();
                started.countDown();
                try {
                    release.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw failure;
            });
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    @Test
    void withDbShouldBypassCache() {
        AtomicInteger loads = new AtomicInteger();
        cache.load(meta, "k", valueType, null, true, () -> {
            loads.incrementAndGet();
            return new TestValue("v", 1);
        });

        Object direct = CacheKit.withDb(() -> cache.load(meta, "k", valueType, null, true, () -> {
            loads.incrementAndGet();
            return new TestValue("direct", 2);
        }));

        assertThat(direct).isEqualTo(new TestValue("direct", 2));
        assertThat(loads.get()).isEqualTo(2);
    }

    @Test
    void longKeyShouldBeHashedDeterministically() {
        props.getGeneric().setMaxKeyLength(16);
        TieredValueCache smallKeyCache = new TieredValueCache(props, l1, null,
                new NoopInvalidationPublisher(), null, "", List.of());

        String key1 = smallKeyCache.key(meta, "x".repeat(64));
        String key2 = smallKeyCache.key(meta, "x".repeat(64));
        String key3 = smallKeyCache.key(meta, "y".repeat(64));

        assertThat(key1).isEqualTo(key2).contains(":h:");
        assertThat(key3).isNotEqualTo(key1);
    }

    @Test
    void compressionShouldApplyToL2ButNotL1() {
        CacheKitProperties compressedProps = new CacheKitProperties();
        compressedProps.getL2().setJitter(Duration.ZERO);
        compressedProps.getL2().setCompressionEnabled(true);
        compressedProps.getL2().setCompressionMinKb(0);
        RecordingChannel l2 = new RecordingChannel();
        CaffeineChannel local = new CaffeineChannel(1024);
        TieredValueCache compressedCache = new TieredValueCache(compressedProps, local, l2,
                new NoopInvalidationPublisher(), null, "", List.of());
        TestValue value = new TestValue("x".repeat(2_000), 1);

        compressedCache.put(meta, "compressed", value, null);

        String key = compressedCache.key(meta, "compressed");
        assertThat(l2.values.get(key)).startsWith("gz:");
        assertThat(local.get(key).json()).isEqualTo(JsonCodec.write(value));
    }

    @Test
    void oversizedValueShouldSkipBothLevels() {
        CacheKitProperties smallProps = new CacheKitProperties();
        smallProps.getL2().setJitter(Duration.ZERO);
        smallProps.getL2().setMaxValueKb(1);
        RecordingChannel l2 = new RecordingChannel();
        CaffeineChannel local = new CaffeineChannel(1024);
        TieredValueCache smallCache = new TieredValueCache(smallProps, local, l2,
                new NoopInvalidationPublisher(), null, "", List.of());

        smallCache.put(meta, "big", new TestValue("x".repeat(2_000), 1), null);

        String key = smallCache.key(meta, "big");
        assertThat(local.get(key).hit()).isFalse();
        assertThat(l2.values).doesNotContainKey(key);
        assertThat(smallCache.peek(meta, "big", valueType)).isNull();
    }

    @Test
    void oversizedUpdateShouldEvictStaleValue() {
        CacheKitProperties smallProps = new CacheKitProperties();
        smallProps.getL2().setJitter(Duration.ZERO);
        smallProps.getL2().setMaxValueKb(1);
        RecordingChannel l2 = new RecordingChannel();
        CaffeineChannel local = new CaffeineChannel(1024);
        TieredValueCache smallCache = new TieredValueCache(smallProps, local, l2,
                new NoopInvalidationPublisher(), null, "", List.of());

        smallCache.put(meta, "cfg", new TestValue("old", 1), null);
        assertThat(smallCache.peek(meta, "cfg", valueType)).isEqualTo(new TestValue("old", 1));

        smallCache.put(meta, "cfg", new TestValue("x".repeat(2_000), 2), null);

        assertThat(smallCache.peek(meta, "cfg", valueType))
                .as("超限更新必须失效旧值，不能静默变成 no-op")
                .isNull();
        assertThat(l2.values).doesNotContainKey(smallCache.key(meta, "cfg"));
    }

    @Test
    void serializationFailureShouldEvictStaleValue() {
        cache.put(meta, "cfg", new TestValue("old", 1), null);

        cache.put(meta, "cfg", new Unserializable(), null);

        assertThat(cache.peek(meta, "cfg", valueType))
                .as("序列化失败的更新必须失效旧值")
                .isNull();
    }

    @Test
    void nonPositiveNullTtlShouldDisableNullPlaceholder() {
        props.getL2().setNullTtl(Duration.ZERO);
        AtomicInteger loads = new AtomicInteger();

        cache.load(meta, "absent", valueType, null, true, () -> {
            loads.incrementAndGet();
            return null;
        });
        cache.load(meta, "absent", valueType, null, true, () -> {
            loads.incrementAndGet();
            return null;
        });

        assertThat(loads.get()).as("null-ttl 非正：不得写 null 占位（L1 也不写）").isEqualTo(2);
        assertThat(l1.get(cache.key(meta, "absent")).hit()).isFalse();
    }

    @Test
    void nullPutWithCacheNullDisabledShouldEvictAndCount() throws Exception {
        io.github.biglv666.cachekit.core.DoubleDeleteScheduler scheduler =
                new io.github.biglv666.cachekit.core.DoubleDeleteScheduler(Duration.ofMillis(20));
        io.github.biglv666.cachekit.core.CacheStatsCollector stats =
                new io.github.biglv666.cachekit.core.CacheStatsCollector();
        try {
            TieredValueCache txCache = new TieredValueCache(props, l1, null,
                    new NoopInvalidationPublisher(), scheduler, "", List.of());
            txCache.setStatsCollector(stats);
            GenericCacheMetadata noNull = new GenericCacheMetadata(TestValue.class, "app-config", null, false);

            txCache.put(noNull, "cfg", new TestValue("old", 1), null);
            txCache.put(noNull, "cfg", null, null);

            assertThat(txCache.peek(noNull, "cfg", valueType)).isNull();
            assertThat(stats.snapshot().get("generic:app-config").evictions())
                    .as("put(null) 失效必须计入 evictions").isEqualTo(1);

            // 延迟双删：失效后回填的旧值应被第二次删除清除
            txCache.put(noNull, "cfg", new TestValue("stale", 1), null);
            Thread.sleep(150);
            assertThat(txCache.peek(noNull, "cfg", valueType)).isNull();
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    void nullPutWithPlaceholderDisabledShouldEvictStaleValue() {
        props.getL2().setNullTtl(Duration.ZERO);
        cache.put(meta, "cfg", new TestValue("old", 1), null);

        cache.put(meta, "cfg", null, null);

        assertThat(cache.peek(meta, "cfg", valueType))
                .as("null-ttl 非正时无法写占位，必须失效旧值")
                .isNull();
    }

    @Test
    void evictShouldPublishTaggedGenericKey() {
        List<String> published = new ArrayList<>();
        InvalidationPublisher publisher = new InvalidationPublisher() {
            @Override
            public void publish(String key) {
                published.add(key);
            }
        };
        TieredValueCache publishingCache = new TieredValueCache(props, l1, null,
                publisher, null, "", List.of());
        String rawKey = publishingCache.key(meta, "k");

        publishingCache.evict(meta, "k");

        assertThat(published).containsExactly(GenericKeyCodec.wrap(rawKey));
        assertThat(GenericKeyCodec.isGenericMessage(published.get(0))).isTrue();
    }

    /** Jackson 序列化时 getter 抛异常，用于触发序列化失败分支 */
    static class Unserializable {
        public String getValue() {
            throw new IllegalStateException("boom");
        }
    }

    /** 内存 L2 通道：记录实际写入的 JSON（用于断言压缩/大值策略） */
    static class RecordingChannel implements CacheChannel {
        final Map<String, String> values = new ConcurrentHashMap<>();

        @Override
        public CacheEntry get(String key) {
            String json = values.get(key);
            return json == null ? CacheEntry.miss() : CacheEntry.of(json);
        }

        @Override
        public void put(String key, String json, Duration ttl) {
            values.put(key, json);
        }

        @Override
        public void evict(String key) {
            values.remove(key);
        }
    }
}
