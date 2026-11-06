package io.github.biglv666.cachekit.generic;

import com.fasterxml.jackson.databind.JavaType;
import io.github.biglv666.cachekit.annotation.CacheLoad;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.support.JsonCodec;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 通用缓存混合读写压测：并发 get/put/evict/peek 下不得死锁、不得泄漏 single-flight；
 * {@code @CacheLoad} 命中路径与受检异常失败路径并发下类型正确、无残留。非 JMH 基准，
 * 用于回归并发正确性。
 */
class GenericCacheStressTest {

    static class ConcurrentConfigService {
        final AtomicInteger goodLoads = new AtomicInteger();
        final AtomicInteger badLoads = new AtomicInteger();

        @CacheLoad(key = "#key")
        public TestValue load(String key) {
            goodLoads.incrementAndGet();
            return new TestValue(key, key.length());
        }

        @CacheLoad(key = "'bad:' + #key")
        public TestValue loadChecked(String key) throws IOException {
            badLoads.incrementAndGet();
            try {
                Thread.sleep(50); // 扩大 single-flight 窗口，让等待者加入同一个 in-flight
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IOException("db down: " + key);
        }
    }

    private static TieredValueCache newCache() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        return new TieredValueCache(props, new CaffeineChannel(4096), null,
                new NoopInvalidationPublisher(), null, "", List.of());
    }

    @Test
    void mixedOperationsShouldRemainConsistent() throws Exception {
        TieredValueCache cache = newCache();
        GenericCacheMetadata meta = new GenericCacheMetadata(TestValue.class, "app-config", null, true);
        JavaType valueType = JsonCodec.constructType(TestValue.class);

        int threads = 16;
        int iterations = 2_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                        ThreadLocalRandom random = ThreadLocalRandom.current();
                        for (int j = 0; j < iterations; j++) {
                            String key = "k" + random.nextInt(32);
                            int seq = j;
                            switch (random.nextInt(4)) {
                                case 0 -> cache.load(meta, key, valueType, null, true,
                                        () -> new TestValue(key, seq));
                                case 1 -> cache.put(meta, key, new TestValue(key, seq), null);
                                case 2 -> cache.evict(meta, key);
                                default -> cache.peek(meta, key, valueType);
                            }
                        }
                    } catch (Throwable t) {
                        firstError.compareAndSet(null, t);
                    }
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(firstError.get()).isNull();
        assertThat(cache.inflightCount()).isZero();

        cache.put(meta, "final", new TestValue("final", 1), null);
        assertThat(cache.peek(meta, "final", valueType)).isEqualTo(new TestValue("final", 1));
        cache.evict(meta, "final");
        assertThat(cache.peek(meta, "final", valueType)).isNull();
    }

    @Test
    void aspectConcurrentReadsShouldCacheEachKeyOnce() throws Exception {
        TieredValueCache cache = newCache();
        ConcurrentConfigService target = new ConcurrentConfigService();
        ProxyFactory factory = new ProxyFactory(target);
        factory.addAdvisor(new GenericCacheAspect(cache, new GenericCacheRegistry()));
        ConcurrentConfigService proxy = (ConcurrentConfigService) factory.getProxy();

        int threads = 16;
        int iterations = 2_000;
        int keys = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                        ThreadLocalRandom random = ThreadLocalRandom.current();
                        for (int j = 0; j < iterations; j++) {
                            String key = "k" + random.nextInt(keys);
                            TestValue value = proxy.load(key);
                            if (value == null || !(value instanceof TestValue)
                                    || !key.equals(value.name)) {
                                throw new AssertionError("命中缓存后类型/内容错误: " + key + " -> " + value);
                            }
                        }
                    } catch (Throwable t) {
                        firstError.compareAndSet(null, t);
                    }
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(firstError.get()).isNull();
        assertThat(target.goodLoads).as("每个 key 只回源一次").hasValue(keys);
        assertThat(cache.inflightCount()).isZero();
    }

    @Test
    void concurrentCheckedFailuresShouldNotLeakInflight() throws Exception {
        TieredValueCache cache = newCache();
        ConcurrentConfigService target = new ConcurrentConfigService();
        ProxyFactory factory = new ProxyFactory(target);
        factory.addAdvisor(new GenericCacheAspect(cache, new GenericCacheRegistry()));
        ConcurrentConfigService proxy = (ConcurrentConfigService) factory.getProxy();

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Throwable>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    try {
                        proxy.loadChecked("x");
                        return null;
                    } catch (Throwable t) {
                        return t;
                    }
                }));
            }
            start.countDown();
            for (Future<Throwable> future : futures) {
                Throwable failure = future.get(10, TimeUnit.SECONDS);
                assertThat(failure).isInstanceOf(IOException.class).hasMessage("db down: x");
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(target.badLoads.get()).as("并发失败应尽量合并为一次回源").isGreaterThanOrEqualTo(1);
        assertThat(cache.inflightCount()).as("失败后不得残留 in-flight").isZero();
        assertThat(cache.peek(new GenericCacheMetadata(TestValue.class, "app-config", null, true),
                "bad:x", JsonCodec.constructType(TestValue.class))).as("异常结果不得写入缓存").isNull();

        // 失败不影响其它 key 的缓存能力
        proxy.load("ok");
        proxy.load("ok");
        assertThat(target.goodLoads).hasValue(1);
    }
}
