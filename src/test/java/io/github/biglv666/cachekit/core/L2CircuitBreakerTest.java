package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.exception.CacheKitException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * L2 熔断器状态机单元测试：熔断/半开探测/恢复全转换 + 指标回调 + 并发探测资格。
 */
class L2CircuitBreakerTest {

    /** 记录型监听器：捕获 opened 次数与状态迁移序列 */
    private static class RecordingListener implements CacheMetricsListener {
        final AtomicInteger opened = new AtomicInteger();
        final List<Integer> states = new ArrayList<>();

        @Override
        public void l2CircuitOpened() {
            opened.incrementAndGet();
        }

        @Override
        public void l2CircuitState(int state) {
            synchronized (states) {
                states.add(state);
            }
        }
    }

    @Test
    void failuresBelowThresholdShouldStayClosed() {
        L2CircuitBreaker breaker = new L2CircuitBreaker(3, Duration.ofMillis(200));
        breaker.recordFailure();
        breaker.recordFailure();
        // 未达阈值：继续放行（失败由调用方按降级处理）
        assertThat(breaker.tryAcquire()).isTrue();
        assertThat(breaker.state()).isEqualTo(L2CircuitBreaker.STATE_CLOSED);
    }

    @Test
    void reachingThresholdShouldOpenAndShortCircuit() {
        L2CircuitBreaker breaker = new L2CircuitBreaker(2, Duration.ofMillis(200));
        breaker.recordFailure();
        breaker.recordFailure();
        assertThat(breaker.state()).isEqualTo(L2CircuitBreaker.STATE_OPEN);
        assertThat(breaker.tryAcquire()).isFalse();
    }

    @Test
    void successShouldResetConsecutiveFailures() {
        L2CircuitBreaker breaker = new L2CircuitBreaker(2, Duration.ofMillis(200));
        breaker.recordFailure();
        breaker.recordSuccess();
        breaker.recordFailure();
        // 成功清零后单次失败不足以熔断
        assertThat(breaker.state()).isEqualTo(L2CircuitBreaker.STATE_CLOSED);
        assertThat(breaker.tryAcquire()).isTrue();
    }

    @Test
    void openShouldGrantSingleProbeAfterExpiry() throws InterruptedException {
        L2CircuitBreaker breaker = new L2CircuitBreaker(1, Duration.ofMillis(50));
        breaker.recordFailure();
        assertThat(breaker.tryAcquire()).isFalse();

        // 轮询等待 openDuration 到期，避免固定 sleep 的时序脆弱
        long deadline = System.currentTimeMillis() + 2000;
        boolean granted = false;
        while (System.currentTimeMillis() < deadline) {
            if (breaker.tryAcquire()) {
                granted = true;
                break;
            }
            Thread.sleep(5);
        }
        assertThat(granted).as("openDuration 到期后应授予探测资格").isTrue();
        assertThat(breaker.state()).isEqualTo(L2CircuitBreaker.STATE_HALF_OPEN);
        // HALF_OPEN 期间其余请求继续短路：探测资格至多一个
        assertThat(breaker.tryAcquire()).isFalse();
    }

    @Test
    void halfOpenProbeSuccessShouldClose() throws InterruptedException {
        L2CircuitBreaker breaker = new L2CircuitBreaker(1, Duration.ofMillis(30));
        breaker.recordFailure();
        awaitProbe(breaker);
        breaker.recordSuccess();
        assertThat(breaker.state()).isEqualTo(L2CircuitBreaker.STATE_CLOSED);
        assertThat(breaker.tryAcquire()).isTrue();
    }

    @Test
    void halfOpenProbeFailureShouldReopen() throws InterruptedException {
        L2CircuitBreaker breaker = new L2CircuitBreaker(1, Duration.ofMillis(30));
        breaker.recordFailure();
        awaitProbe(breaker);
        breaker.recordFailure();
        assertThat(breaker.state()).isEqualTo(L2CircuitBreaker.STATE_OPEN);
        assertThat(breaker.tryAcquire()).isFalse();
    }

    @Test
    void metricsShouldBeNotifiedOnOpenAndTransitions() throws InterruptedException {
        RecordingListener listener = new RecordingListener();
        L2CircuitBreaker breaker = new L2CircuitBreaker(1, Duration.ofMillis(30));
        breaker.setMetricsListener(listener);

        breaker.recordFailure();
        assertThat(listener.opened.get()).isEqualTo(1);
        assertThat(listener.states).containsExactly(L2CircuitBreaker.STATE_OPEN);

        awaitProbe(breaker);
        breaker.recordSuccess();
        assertThat(listener.states).containsExactly(
                L2CircuitBreaker.STATE_OPEN, L2CircuitBreaker.STATE_HALF_OPEN, L2CircuitBreaker.STATE_CLOSED);
        // 探测成功不算"新熔断"
        assertThat(listener.opened.get()).isEqualTo(1);
    }

    @Test
    void concurrentHalfOpenShouldGrantExactlyOneProbe() throws Exception {
        L2CircuitBreaker breaker = new L2CircuitBreaker(1, Duration.ofMillis(50));
        breaker.recordFailure();

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                results.add(pool.submit(() -> {
                    // 轮询到 openDuration 到期后抢探测资格：至多一个线程拿到 true
                    long deadline = System.currentTimeMillis() + 1000;
                    while (System.currentTimeMillis() < deadline) {
                        if (breaker.tryAcquire()) {
                            return true;
                        }
                        Thread.sleep(2);
                    }
                    return false;
                }));
            }
            int granted = 0;
            for (Future<Boolean> f : results) {
                if (f.get(3, TimeUnit.SECONDS)) {
                    granted++;
                }
            }
            assertThat(granted).as("HALF_OPEN 探测资格必须恰好授予一个线程").isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void invalidConfigurationShouldFailFast() {
        assertThatThrownBy(() -> new L2CircuitBreaker(0, Duration.ofSeconds(1)))
                .isInstanceOf(CacheKitException.class);
        assertThatThrownBy(() -> new L2CircuitBreaker(1, Duration.ZERO))
                .isInstanceOf(CacheKitException.class);
        assertThatThrownBy(() -> new L2CircuitBreaker(1, null))
                .isInstanceOf(CacheKitException.class);
    }

    private static void awaitProbe(L2CircuitBreaker breaker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < deadline) {
            if (breaker.tryAcquire()) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("openDuration 到期后未获得探测资格");
    }
}
