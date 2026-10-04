package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.exception.CacheKitException;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * L2 通道熔断器：连续失败达到阈值后短路一段时间，避免 Redis 故障期间海量无效重试
 * （每次重试在命令超时前都会阻塞调用线程）。接入点是 {@code RedisChannel}——
 * 组件"L2 故障降级"承诺的唯一边界，广播发布器本就非阻塞、不接入。
 *
 * <p>状态机：CLOSED（正常，逐次累计连续失败）→ 连续失败达阈值 → OPEN（短路，
 * 所有 Redis 调用直接按降级处理，不再触达 Redis）→ openDuration 到期 → HALF_OPEN
 * （放行单个探测请求，其余请求继续短路）→ 探测成功回 CLOSED（失败计数清零），
 * 探测失败回 OPEN 重新计时。</p>
 *
 * <p>线程安全：状态用 {@link AtomicInteger} 承载，HALF_OPEN 的探测资格经 CAS 授予
 * 至多一个线程（防故障恢复瞬间的惊群）。状态迁移存在良性竞态：旧调用与探测并发完成时，
 * 最坏后果是多放行一次调用或晚一轮熔断——降级语义（失败按未命中处理）不受影响。</p>
 *
 * <p>已知边界：熔断不能消除"单次调用阻塞到命令超时"的成本——触发熔断的前 N 次失败
 * 仍会等待命令超时（Lettuce 默认 60s），{@code spring.data.redis.timeout} 的调整建议
 * 依旧成立；熔断省的是后续海量重试，不是第一次的等待。</p>
 */
public class L2CircuitBreaker {

    /** 状态值：正常放行 */
    public static final int STATE_CLOSED = 0;
    /** 状态值：探测期，仅单个探测请求放行 */
    public static final int STATE_HALF_OPEN = 1;
    /** 状态值：熔断短路，全部调用按降级处理 */
    public static final int STATE_OPEN = 2;

    private final int failureThreshold;
    private final long openDurationNanos;

    /** 0=CLOSED 1=HALF_OPEN 2=OPEN；OPEN 到期后由 tryAcquire CAS 迁移到 HALF_OPEN */
    private final AtomicInteger state = new AtomicInteger(STATE_CLOSED);
    /** CLOSED 态的连续失败计数，成功清零 */
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    /** 进入 OPEN 的时刻（nanoTime），仅 OPEN 态有意义 */
    private volatile long openedAtNanos;

    private volatile CacheMetricsListener metrics = new CacheMetricsListener() {
    };

    /**
     * @param failureThreshold 连续失败达到该次数后熔断（CLOSED 态累计，成功清零）
     * @param openDuration     OPEN 态持续时间，到期后放行单个探测请求
     */
    public L2CircuitBreaker(int failureThreshold, Duration openDuration) {
        if (failureThreshold < 1) {
            throw new CacheKitException("熔断器的 failureThreshold 必须 >= 1: " + failureThreshold);
        }
        if (openDuration == null || openDuration.isZero() || openDuration.isNegative()) {
            throw new CacheKitException("熔断器的 openDuration 必须为正数");
        }
        this.failureThreshold = failureThreshold;
        this.openDurationNanos = openDuration.toNanos();
    }

    /** 挂载指标监听器（熔断打开埋点 + 状态 gauge 数据源） */
    public void setMetricsListener(CacheMetricsListener metrics) {
        this.metrics = metrics == null ? new CacheMetricsListener() {
        } : metrics;
    }

    /**
     * 尝试获得一次 Redis 调用资格。
     *
     * @return true 表示允许调用，调用方随后必须调用 {@link #recordSuccess()} 或
     *         {@link #recordFailure()} 之一；false 表示熔断短路，调用方按降级处理
     *         （get 视为未命中、put/evict 跳过），不得触达 Redis
     */
    public boolean tryAcquire() {
        while (true) {
            int s = state.get();
            if (s == STATE_CLOSED) {
                return true;
            }
            if (s == STATE_OPEN) {
                if (System.nanoTime() - openedAtNanos < openDurationNanos) {
                    return false;
                }
                // OPEN 到期：CAS 迁移到 HALF_OPEN 并把探测资格授予本线程；
                // CAS 失败说明其他线程已完成迁移，重读状态
                if (state.compareAndSet(STATE_OPEN, STATE_HALF_OPEN)) {
                    metrics.l2CircuitState(STATE_HALF_OPEN);
                    return true;
                }
                continue;
            }
            // HALF_OPEN：探测资格已被占用（或探测已成功关闭），其余请求继续短路
            return false;
        }
    }

    /** 调用成功：HALF_OPEN 探测成功回 CLOSED；CLOSED 态清零连续失败计数 */
    public void recordSuccess() {
        consecutiveFailures.set(0);
        if (state.getAndSet(STATE_CLOSED) != STATE_CLOSED) {
            metrics.l2CircuitState(STATE_CLOSED);
        }
    }

    /** 调用失败：CLOSED 态累计到阈值即熔断；HALF_OPEN 探测失败回 OPEN 重新计时 */
    public void recordFailure() {
        if (state.get() == STATE_HALF_OPEN) {
            // 探测失败：不依赖连续计数，直接回 OPEN（重新计时）
            openedAtNanos = System.nanoTime();
            if (state.getAndSet(STATE_OPEN) != STATE_OPEN) {
                metrics.l2CircuitOpened();
                metrics.l2CircuitState(STATE_OPEN);
            }
            return;
        }
        if (consecutiveFailures.incrementAndGet() >= failureThreshold
                && state.compareAndSet(STATE_CLOSED, STATE_OPEN)) {
            openedAtNanos = System.nanoTime();
            metrics.l2CircuitOpened();
            metrics.l2CircuitState(STATE_OPEN);
        }
    }

    /** 当前状态（{@link #STATE_CLOSED} / {@link #STATE_HALF_OPEN} / {@link #STATE_OPEN}） */
    public int state() {
        return state.get();
    }

    /** 当前状态名，端点/日志展示用 */
    public String stateName() {
        return switch (state.get()) {
            case STATE_HALF_OPEN -> "HALF_OPEN";
            case STATE_OPEN -> "OPEN";
            default -> "CLOSED";
        };
    }
}
