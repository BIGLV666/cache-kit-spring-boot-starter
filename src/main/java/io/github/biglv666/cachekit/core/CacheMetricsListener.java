package io.github.biglv666.cachekit.core;

/**
 * 缓存指标监听器：Micrometer 集成（可选）或自定义观测的挂载点。
 * 所有方法均为空默认实现，宿主按需覆写；Micrometer 在类路径时由自动装配
 * 注册 `cache-kit.*` 计数器（命中率、回源、失效、广播收发）。
 */
public interface CacheMetricsListener {

    /** L1 一次查找（按结果） */
    default void l1Lookup(boolean hit) {
    }

    /** L2 一次查找（按结果；L2 未启用或故障降级时不计） */
    default void l2Lookup(boolean hit) {
    }

    /** 一次 DB 回源（批量按 ID 数计） */
    default void dbLoad(int ids) {
    }

    /** 写入一个 null 占位 */
    default void nullPlaceholder() {
    }

    /** 键失效（含双删的第二次删除） */
    default void evict(int keys) {
    }

    /** 失效广播发送 */
    default void broadcastSent() {
    }

    /** 失效广播接收（applied=false 表示校验未通过被忽略） */
    default void broadcastReceived(boolean applied) {
    }
}
