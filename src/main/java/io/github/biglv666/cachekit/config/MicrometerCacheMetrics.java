package io.github.biglv666.cachekit.config;

import io.github.biglv666.cachekit.core.CacheMetricsListener;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Micrometer 指标实现：宿主引入 micrometer-core（spring-boot-starter-actuator 自带）
 * 后自动注册，通过 /actuator/metrics 或 Prometheus 抓取。
 *
 * <p>指标一览（全部为 Counter）：
 * cache-kit.l1.requests{result=hit|miss} —— L1 命中率
 * cache-kit.l2.requests{result=hit|miss} —— L2 命中率
 * cache-kit.db.loads —— DB 回源次数（批量为批）
 * cache-kit.null.placeholders —— null 占位写入（穿透防护触发）
 * cache-kit.evict.keys —— 失效键数（含双删第二次）
 * cache-kit.broadcast.sent / cache-kit.broadcast.received{applied=true|false}
 * —— 收发差值可观：sent 持续大于 received 总和说明存在广播丢失（由 L1 TTL 兜底）
 * cache-kit.l2.fallbacks{op=get|put|multiGet|evictAll} —— L2 降级次数（Redis 故障按未命中处理）
 * cache-kit.evict.retries.exhausted —— L2 删除重试耗尽（失效丢失，由 L2 TTL 兜底）
 * cache-kit.doubledelete.skipped —— 双删积压跳过（脏数据由 TTL 上界兜底）
 * cache-kit.binlog.position.resets —— binlog 位点重置（断连窗口失效丢失，由 L2 TTL 兜底）
 * —— 以上四项持续增长说明失效链路存在真实丢失窗口，应告警排查</p>
 */
class MicrometerCacheMetrics implements CacheMetricsListener {

    private final Counter l1Hit;
    private final Counter l1Miss;
    private final Counter l2Hit;
    private final Counter l2Miss;
    private final Counter dbLoads;
    private final Counter nullPlaceholders;
    private final Counter evictKeys;
    private final Counter broadcastSent;
    private final Counter broadcastReceivedApplied;
    private final Counter broadcastReceivedSkipped;
    /** 按 op 惰性注册的降级计数器（op 集合固定为通道操作名，无需预注册） */
    private final MeterRegistry registryRef;
    private final java.util.concurrent.ConcurrentHashMap<String, Counter> l2Fallbacks = new java.util.concurrent.ConcurrentHashMap<>();
    private final Counter evictRetriesExhausted;
    private final Counter doubleDeleteSkipped;
    private final Counter binlogPositionResets;
    private final Counter binlogDeriveSkipped;
    private final Counter circuitOpened;
    private final Counter l2ValueOversized;
    private final Counter l2ValueCompressed;
    private final Counter l1RefreshAheadTriggered;
    private final Counter l1RefreshAheadDropped;
    private final Counter l1RefreshAheadFailed;
    private final io.micrometer.core.instrument.Timer invalidationDelay;
    private final java.util.concurrent.atomic.AtomicLong streamsLagSeconds = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong circuitState = new java.util.concurrent.atomic.AtomicLong();

    MicrometerCacheMetrics(MeterRegistry registry) {
        this.registryRef = registry;
        this.l1Hit = registry.counter("cache-kit.l1.requests", "result", "hit");
        this.l1Miss = registry.counter("cache-kit.l1.requests", "result", "miss");
        this.l2Hit = registry.counter("cache-kit.l2.requests", "result", "hit");
        this.l2Miss = registry.counter("cache-kit.l2.requests", "result", "miss");
        this.dbLoads = registry.counter("cache-kit.db.loads");
        this.nullPlaceholders = registry.counter("cache-kit.null.placeholders");
        this.evictKeys = registry.counter("cache-kit.evict.keys");
        this.broadcastSent = registry.counter("cache-kit.broadcast.sent");
        this.broadcastReceivedApplied = registry.counter("cache-kit.broadcast.received", "applied", "true");
        this.broadcastReceivedSkipped = registry.counter("cache-kit.broadcast.received", "applied", "false");
        this.evictRetriesExhausted = registry.counter("cache-kit.evict.retries.exhausted");
        this.doubleDeleteSkipped = registry.counter("cache-kit.doubledelete.skipped");
        this.binlogPositionResets = registry.counter("cache-kit.binlog.position.resets");
        this.binlogDeriveSkipped = registry.counter("cache-kit.binlog.derive.skipped");
        this.circuitOpened = registry.counter("cache-kit.l2.circuit.opened");
        this.l2ValueOversized = registry.counter("cache-kit.l2.value.oversized");
        this.l2ValueCompressed = registry.counter("cache-kit.l2.value.compressed");
        this.l1RefreshAheadTriggered = registry.counter("cache-kit.l1.refreshahead.triggered");
        this.l1RefreshAheadDropped = registry.counter("cache-kit.l1.refreshahead.dropped");
        this.l1RefreshAheadFailed = registry.counter("cache-kit.l1.refreshahead.failed");
        this.invalidationDelay = io.micrometer.core.instrument.Timer
                .builder("cache-kit.invalidation.delay")
                .description("binlog 行事件 MySQL 时间戳 → 本实例失效应用（含时钟偏差，趋势观测用）")
                .publishPercentiles(0.5, 0.99)
                .register(registry);
        // 命中率 Gauge：由 counter 值计算，无流量时发布 NaN（Prometheus 端视为无数据）
        io.micrometer.core.instrument.Gauge.builder("cache-kit.l1.hit.rate", () -> hitRate(l1Hit, l1Miss))
                .description("L1 命中率（hit / (hit+miss)）")
                .register(registry);
        io.micrometer.core.instrument.Gauge.builder("cache-kit.l2.hit.rate", () -> hitRate(l2Hit, l2Miss))
                .description("L2 命中率（hit / (hit+miss)）")
                .register(registry);
        // streams 模式消费组滞后：最新失效事件与本组已读到事件的时间戳差（秒）
        io.micrometer.core.instrument.Gauge.builder("cache-kit.broadcast.streams.lag.seconds",
                        streamsLagSeconds, java.util.concurrent.atomic.AtomicLong::get)
                .description("Streams 消费组滞后（最新失效事件 - 本组已读事件的时间戳差）")
                .register(registry);
        // L2 熔断器状态：0=CLOSED 1=HALF_OPEN 2=OPEN（状态迁移时由熔断器推送）
        io.micrometer.core.instrument.Gauge.builder("cache-kit.l2.circuit.state",
                        circuitState, java.util.concurrent.atomic.AtomicLong::get)
                .description("L2 熔断器状态（0=CLOSED 1=HALF_OPEN 2=OPEN）")
                .register(registry);
    }

    private static double hitRate(Counter hit, Counter miss) {
        double total = hit.count() + miss.count();
        return total == 0 ? Double.NaN : hit.count() / total;
    }

    @Override
    public void l1Lookup(boolean hit) {
        (hit ? l1Hit : l1Miss).increment();
    }

    @Override
    public void l2Lookup(boolean hit) {
        (hit ? l2Hit : l2Miss).increment();
    }

    @Override
    public void dbLoad(int ids) {
        dbLoads.increment(ids);
    }

    @Override
    public void nullPlaceholder() {
        nullPlaceholders.increment();
    }

    @Override
    public void evict(int keys) {
        evictKeys.increment(keys);
    }

    @Override
    public void broadcastSent() {
        broadcastSent.increment();
    }

    @Override
    public void broadcastReceived(boolean applied) {
        (applied ? broadcastReceivedApplied : broadcastReceivedSkipped).increment();
    }

    @Override
    public void l2Fallback(String op) {
        l2Fallbacks.computeIfAbsent(op == null ? "unknown" : op,
                o -> registryRef.counter("cache-kit.l2.fallbacks", "op", o)).increment();
    }

    @Override
    public void l2CircuitOpened() {
        circuitOpened.increment();
    }

    @Override
    public void l2CircuitState(int state) {
        circuitState.set(state);
    }

    @Override
    public void evictRetryExhausted() {
        evictRetriesExhausted.increment();
    }

    @Override
    public void doubleDeleteSkipped() {
        doubleDeleteSkipped.increment();
    }

    @Override
    public void binlogPositionReset() {
        binlogPositionResets.increment();
    }

    @Override
    public void binlogDeriveSkipped(int rows) {
        binlogDeriveSkipped.increment(rows);
    }

    @Override
    public void streamsLagSeconds(long seconds) {
        streamsLagSeconds.set(seconds);
    }

    @Override
    public void l2ValueOversized() {
        l2ValueOversized.increment();
    }

    @Override
    public void l2ValueCompressed() {
        l2ValueCompressed.increment();
    }

    @Override
    public void l1RefreshAheadTriggered() {
        l1RefreshAheadTriggered.increment();
    }

    @Override
    public void l1RefreshAheadDropped() {
        l1RefreshAheadDropped.increment();
    }

    @Override
    public void l1RefreshAheadFailed() {
        l1RefreshAheadFailed.increment();
    }

    @Override
    public void invalidationDelayMillis(long millis) {
        // MySQL 与宿主机时钟偏差可能导致负值：Timer 不接受负值，偏差场景直接跳过
        if (millis >= 0) {
            invalidationDelay.record(millis, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
    }
}
