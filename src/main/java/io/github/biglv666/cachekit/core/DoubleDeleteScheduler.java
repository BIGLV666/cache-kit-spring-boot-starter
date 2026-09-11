package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.exception.CacheKitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 延迟双删调度器：首次删除后按配置延迟再执行一次完整删除。
 *
 * <p>第二次删除幂等，覆盖"读线程查到旧值、写线程删除后、读线程回填脏值"的回填竞态。
 * 使用守护线程池，随容器销毁关闭。
 */
public class DoubleDeleteScheduler {

    private static final Logger log = LoggerFactory.getLogger(DoubleDeleteScheduler.class);

    /** 待执行任务上限：持续高写入下的积压保护，超限跳过（脏数据由 TTL 上界兜底） */
    private static final int MAX_PENDING = 10_000;

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "cache-kit-double-delete");
                t.setDaemon(true);
                return t;
            });

    private final Duration delay;
    private final AtomicInteger pending = new AtomicInteger();
    private volatile long lastWarnAt;

    public DoubleDeleteScheduler(Duration delay) {
        if (delay == null || delay.isZero() || delay.isNegative()) {
            throw new CacheKitException("延迟双删的 delay 必须为正数");
        }
        this.delay = delay;
    }

    /**
     * 调度一次延迟删除任务。积压超过 {@value MAX_PENDING} 条时跳过并限频告警。
     *
     * @param task 删除动作（应包含 L1/L2 清理与广播）
     */
    public void schedule(Runnable task) {
        if (pending.get() >= MAX_PENDING) {
            long now = System.nanoTime();
            if (now - lastWarnAt > 30_000_000_000L) {
                lastWarnAt = now;
                log.warn("延迟双删任务积压超过 {} 条，新任务被跳过（脏数据由 TTL 上界兜底）", MAX_PENDING);
            }
            return;
        }
        pending.incrementAndGet();
        try {
            executor.schedule(() -> {
                try {
                    task.run();
                } catch (Exception e) {
                    log.warn("延迟双删执行失败", e);
                } finally {
                    pending.decrementAndGet();
                }
            }, delay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            pending.decrementAndGet();
            // 调度器已关闭等场景：不影响业务，TTL 上界兜底
            log.warn("延迟双删任务调度失败", e);
        }
    }

    /** 释放线程池 */
    public void shutdown() {
        executor.shutdownNow();
    }
}
