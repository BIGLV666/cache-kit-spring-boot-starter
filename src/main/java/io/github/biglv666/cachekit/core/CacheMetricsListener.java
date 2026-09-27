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

    /** L2 一次调用降级（op 为通道操作名）：Redis 宕机/抖动时按未命中处理 */
    default void l2Fallback(String op) {
    }

    /** L2 删除重试达到上限仍失败：旧值滞留 L2，失效丢失由 L2 TTL 上界兜底 */
    default void evictRetryExhausted() {
    }

    /** 延迟双删任务因积压超限被跳过：脏数据由 TTL 上界兜底 */
    default void doubleDeleteSkipped() {
    }

    /** binlog 位点被服务端清理后重置为最新位点：断连窗口内的失效丢失 */
    default void binlogPositionReset() {
    }

    /**
     * 一次 binlog 行事件的失效传播延迟（MySQL 事件时间戳 → 本实例失效应用）。
     * 受 MySQL 与本机时钟偏差影响，仅作趋势观测。
     */
    default void invalidationDelayMillis(long millis) {
    }
}
