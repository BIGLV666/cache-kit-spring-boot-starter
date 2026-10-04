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

    /**
     * L2 熔断器打开（连续失败达到阈值）：短路期间所有 Redis 调用零开销降级，
     * 不再逐次触达 Redis。{@code l2.fallbacks} 在短路期间也会持续增长。
     */
    default void l2CircuitOpened() {
    }

    /**
     * L2 熔断器状态迁移（0=CLOSED 1=HALF_OPEN 2=OPEN）。
     * 频繁 OPEN→CLOSED 往返说明 Redis 持续抖动，应检查网络与服务端。
     */
    default void l2CircuitState(int state) {
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
     * 启用了 {@link CacheKeyCustomizer} 的宿主中，一批行事件里有 {@code rows} 行无法还原
     * 自定义键段（未覆写 segmentFor / 行列结构漂移），这些行的 binlog 精确失效被跳过，
     * 仅 TTL/延迟双删兜底。持续增长说明自定义段无法从行数据推导，应实现
     * {@code segmentFor} 或检查表结构。
     */
    default void binlogDeriveSkipped(int rows) {
    }

    /**
     * 一次 binlog 行事件的失效传播延迟（MySQL 事件时间戳 → 本实例失效应用）。
     * 受 MySQL 与本机时钟偏差影响，仅作趋势观测。
     */
    default void invalidationDelayMillis(long millis) {
    }

    /**
     * Streams 模式下本实例消费组的滞后（秒）：最新失效事件与本组已读到事件
     * 的时间戳差，0 表示已追平。持续大于 0 说明消费方处理不过来或断连。
     */
    default void streamsLagSeconds(long seconds) {
    }
}
