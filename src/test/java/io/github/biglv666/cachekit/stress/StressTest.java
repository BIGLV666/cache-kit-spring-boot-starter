package io.github.biglv666.cachekit.stress;

import io.github.biglv666.cachekit.annotation.CacheEntity;
import io.github.biglv666.cachekit.annotation.CacheId;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.core.InMemoryChannel;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 压测（非 CI 时效敏感，阈值留足余量）：
 * ① refresh-ahead 热点防击穿——多线程持续读热点键跨越多个刷新周期，
 *    断言 DB 回源次数仅与刷新周期数相关（single-flight + 去重生效）、读无失败无毛刺值；
 * ② gzip+Base64 编解码开销基准（1KB/32KB/256KB），32KB 端到端编解码 &lt; 1ms；
 * ③ 大值混布高并发读写——超限值两级跳写不报错，正常值照常缓存。
 */
class StressTest {

    @CacheEntity(prefix = "stress_user")
    static class StressUser {
        @CacheId
        public Long userId;
        public String userName;

        public StressUser() {
        }

        StressUser(Long userId, String userName) {
            this.userId = userId;
            this.userName = userName;
        }
    }

    @Test
    void refreshAheadHotKeyShouldNotStampedeDb() throws Exception {
        CacheKitProperties props = new CacheKitProperties();
        props.getL1().setTtl(Duration.ofMillis(500));
        props.getL1().setRefreshAhead(Duration.ofMillis(400));
        props.getL2().setJitter(Duration.ZERO);
        EntityMetadata meta = new EntityMetadataRegistry().require(StressUser.class);

        CaffeineChannel l1 = new CaffeineChannel(10_000);
        InMemoryChannel l2 = new InMemoryChannel();
        java.util.concurrent.ThreadPoolExecutor refreshPool = new java.util.concurrent.ThreadPoolExecutor(
                2, 2, 60, java.util.concurrent.TimeUnit.SECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(256));
        refreshPool.allowCoreThreadTimeOut(true);

        TieredEntityCache cache = new TieredEntityCache(props, l1, l2,
                new NoopInvalidationPublisher(), null, "", List.of());
        cache.setRefreshAheadExecutor(refreshPool);

        AtomicInteger dbLoads = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean(false);

        // 预热：填充 L1/L2
        cache.load(meta, 1L, null, true, () -> {
            dbLoads.incrementAndGet();
            return new StressUser(1L, "hot");
        });

        int threads = 32;
        AtomicLong reads = new AtomicLong();
        AtomicInteger errors = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                try {
                    start.await();
                    while (!stop.get()) {
                        Object v = cache.load(meta, 1L, null, true, () -> {
                            // 直达 DB 的路径只可能来自 single-flight 赢家
                            dbLoads.incrementAndGet();
                            return new StressUser(1L, "hot");
                        });
                        if (v == null || ((StressUser) v).userId == null) {
                            errors.incrementAndGet();
                        }
                        reads.incrementAndGet();
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }, "stress-reader-" + i);
            workers.add(t);
            t.start();
        }
        start.countDown();
        Thread.sleep(2000);
        stop.set(true);
        done.await(10, java.util.concurrent.TimeUnit.SECONDS);
        refreshPool.shutdownNow();

        long totalReads = reads.get();
        int totalDbLoads = dbLoads.get();
        System.out.printf("[stress] refresh-ahead: reads=%d dbLoads=%d errors=%d ratio=1/%d%n",
                totalReads, totalDbLoads, errors.get(), totalReads / Math.max(1, totalDbLoads));

        // 全部读成功、值正确
        assertThat(errors.get()).as("并发读期间不应有失败").isZero();
        // 回源仅发生在刷新周期边界（2s / 500ms ≈ 4 个 TTL 周期 + 预热 1 次），给 20 倍余量
        assertThat(totalDbLoads).as("DB 回源不应随读线程数放大").isLessThanOrEqualTo(20 + threads);
        // 防击穿生效的强断言：回源次数与读次数完全解耦
        assertThat(totalDbLoads * 1000L).as("回源/读 放大比应低于 1/1000").isLessThan(totalReads);
    }

    @Test
    void gzipCodecOverheadBenchmark() throws Exception {
        for (int size : new int[]{1024, 32 * 1024, 256 * 1024}) {
            String json = ("{\"userId\":1,\"userName\":\"" + "u".repeat(size) + "\"}");
            byte[] payload = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            // 预热 200 次（JIT）
            for (int i = 0; i < 200; i++) {
                roundtrip(payload);
            }
            int iterations = 2000;
            long start = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                roundtrip(payload);
            }
            long nanos = (System.nanoTime() - start) / iterations;
            System.out.printf("[stress] gzip+base64 roundtrip %6d B: %6d µs/op%n", payload.length, nanos / 1000);
            // 宽松上限：32KB 值的端到端编解码必须远低于一次 DB 查询（< 1ms）；256KB 放宽到 10ms
            if (size == 32 * 1024) {
                assertThat(nanos / 1000).as("32KB 编解码应 < 1ms").isLessThan(1000);
            } else if (size > 32 * 1024) {
                assertThat(nanos / 1000).as("256KB 编解码应 < 10ms").isLessThan(10_000);
            }
        }
    }

    private static String roundtrip(byte[] payload) throws Exception {
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
        try (java.util.zip.GZIPOutputStream gz = new java.util.zip.GZIPOutputStream(buf)) {
            gz.write(payload);
        }
        String stored = "gz:" + java.util.Base64.getEncoder().encodeToString(buf.toByteArray());
        byte[] back = java.util.Base64.getDecoder().decode(stored.substring(3));
        try (java.util.zip.GZIPInputStream in = new java.util.zip.GZIPInputStream(
                new java.io.ByteArrayInputStream(back))) {
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    @Test
    void mixedOversizedTrafficShouldStayStable() throws Exception {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setMaxValueKb(1); // 1KB 上限：大值全部跳写
        props.getL2().setJitter(Duration.ZERO);
        EntityMetadata meta = new EntityMetadataRegistry().require(StressUser.class);
        InMemoryChannel l1 = new InMemoryChannel();
        InMemoryChannel l2 = new InMemoryChannel();
        TieredEntityCache cache = new TieredEntityCache(props, l1, l2,
                new NoopInvalidationPublisher(), null, "", List.of());

        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicLong reads = new AtomicLong();
        AtomicInteger errors = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            final int worker = i;
            new Thread(() -> {
                try {
                    start.await();
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    for (int n = 0; n < 2000; n++) {
                        // 偶数 worker 读小值（正常缓存），奇数 worker 读大值（超限跳写）
                        long id = worker % 2 == 0 ? worker : 10_000 + worker;
                        String name = worker % 2 == 0 ? "u" + id : "x".repeat(4096);
                        Object v = cache.load(meta, id, null, true, () -> new StressUser(id, name));
                        if (v == null) {
                            errors.incrementAndGet();
                        }
                        reads.incrementAndGet();
                        if (rnd.nextInt(64) == 0) {
                            cache.evict(meta, id);
                        }
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }, "stress-mixed-" + i).start();
        }
        start.countDown();
        assertThat(done.await(60, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        System.out.printf("[stress] mixed oversized: reads=%d errors=%d l1Size=%d%n",
                reads.get(), errors.get(), l1.store.size());
        assertThat(errors.get()).as("大值混布不应产生任何失败").isZero();
        // 超限值不进缓存：只有小值（偶数 worker，8 个键）被缓存
        assertThat(l1.store.size()).isLessThanOrEqualTo(8);
        assertThat(l2.store.size()).isLessThanOrEqualTo(8);
    }
}
