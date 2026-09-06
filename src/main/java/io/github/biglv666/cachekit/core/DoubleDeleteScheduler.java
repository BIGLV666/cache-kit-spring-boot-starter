package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.exception.CacheKitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
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

    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "cache-kit-double-delete");
                t.setDaemon(true);
                return t;
            });

    private final Duration delay;

    public DoubleDeleteScheduler(Duration delay) {
        if (delay == null || delay.isZero() || delay.isNegative()) {
            throw new CacheKitException("延迟双删的 delay 必须为正数");
        }
        this.delay = delay;
    }

    /**
     * 调度一次延迟删除任务。
     *
     * @param task 删除动作（应包含 L1/L2 清理与广播）
     */
    public void schedule(Runnable task) {
        try {
            executor.schedule(() -> {
                try {
                    task.run();
                } catch (Exception e) {
                    log.warn("延迟双删执行失败", e);
                }
            }, delay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // 调度器已关闭等场景：不影响业务，TTL 上界兜底
            log.warn("延迟双删任务调度失败", e);
        }
    }

    /** 释放线程池 */
    public void shutdown() {
        executor.shutdownNow();
    }
}
