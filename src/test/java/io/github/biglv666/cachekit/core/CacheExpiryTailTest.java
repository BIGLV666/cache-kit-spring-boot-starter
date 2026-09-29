package io.github.biglv666.cachekit.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 诊断用（非选型依据）：隔离"自定义 Expiry 的每读重算"是否为 JetCache 多级缓存
 * 尾延迟劣化的根因。两个裸 Caffeine（同容量、同 1000 键、16 线程）：
 * A) 固定 expireAfterWrite（cache-kit CaffeineChannel 的用法，读路径纯读）；
 * B) 自定义 Expiry 且 expireAfterRead 返回值随时间漂移（复刻 JetCache 的
 *    getRestTimeInNanos 语义：min(剩余, accessWindow)，剩余时间随时间连续变化），
 *    Caffeine 每次读回调并可能重排条目定时器。
 */
class CacheExpiryTailTest {

    private static final int KEYS = 1_000;
    private static final int THREADS = 16;
    private static final double SECONDS = 2.0;

    @Test
    void fixedVsVariableExpiryTail() throws Exception {
        com.github.benmanes.caffeine.cache.Cache<String, String> fixed =
                Caffeine.newBuilder().maximumSize(10_000)
                        .expireAfterWrite(Duration.ofSeconds(300)).build();
        com.github.benmanes.caffeine.cache.Cache<String, String> variable =
                Caffeine.newBuilder().maximumSize(10_000)
                        .expireAfter(new Expiry<String, String>() {
                            @Override
                            public long expireAfterCreate(String key, String value, long now) {
                                return TimeUnit.SECONDS.toNanos(300);
                            }

                            @Override
                            public long expireAfterUpdate(String key, String value, long now, long current) {
                                return TimeUnit.SECONDS.toNanos(300);
                            }

                            @Override
                            public long expireAfterRead(String key, String value, long now, long current) {
                                // 复刻 JetCache getRestTimeInNanos 的"时间相关重算"：返回值随 ticker 连续变化
                                // （触发 Caffeine 条目重排），但用 1s 内的抖动量保证始终远离过期——
                                // 早期版本用 now % 300s 会在 JVM 运行到窗口末尾时算出趋近 0 的过期时间，
                                // 条目被清掉导致断言杀线程、await 挂死（间歇性 flaky 的根因）
                                long jitterMillis = TimeUnit.NANOSECONDS.toMillis(now % 1_000_000_000L) & 0xFFF;
                                return TimeUnit.MILLISECONDS.toNanos(300_000L - jitterMillis);
                            }
                        }).build();
        for (int i = 1; i <= KEYS; i++) {
            fixed.put("user:" + i, "v" + i);
            variable.put("user:" + i, "v" + i);
        }

        run("A 固定 expireAfterWrite", fixed);
        run("B 变量 Expiry(每读重算)", variable);
        run("A 固定 expireAfterWrite(复跑)", fixed);
        run("B 变量 Expiry(复跑)", variable);
    }

    private static void run(String name, com.github.benmanes.caffeine.cache.Cache<String, String> cache)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(THREADS);
        List<List<Long>> latencies = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicLong ops = new AtomicLong();
        for (int t = 0; t < THREADS; t++) {
            final int tid = t;
            pool.submit(() -> {
                List<Long> lat = new ArrayList<>(64_000);
                start.await();
                long cursor = tid;
                int misses = 0;
                while (!stop.get()) {
                    String key = "user:" + ((cursor++ % KEYS) + 1);
                    long t0 = System.nanoTime();
                    if (cache.getIfPresent(key) == null) {
                        // 防呆：偶发 miss 记录不中断（断言杀线程会让 await 挂死——此前 flaky 的教训）
                        misses++;
                    }
                    lat.add(System.nanoTime() - t0);
                    ops.incrementAndGet();
                }
                if (misses > 0) {
                    System.out.printf("WARN|expiry-tail 命中 miss %d 次%n", misses);
                }
                latencies.add(lat);
                done.countDown();
                return null;
            });
        }
        start.countDown();
        Thread.sleep((long) (SECONDS * 1000));
        stop.set(true);
        done.await();
        pool.shutdownNow();

        int n = latencies.stream().mapToInt(List::size).sum();
        long[] all = new long[n];
        int idx = 0;
        for (List<Long> lat : latencies) {
            for (Long l : lat) {
                all[idx++] = l / 1_000;
            }
        }
        Arrays.sort(all);
        System.out.printf("ROW|%-30s|%12.0f ops/s|p50 %5d us|p99 %7d us|p999 %8d us|max %9d us%n",
                name, ops.get() / SECONDS, all[n / 2], all[(int) (n * 0.99)],
                all[(int) (n * 0.999)], all[n - 1]);
    }
}
