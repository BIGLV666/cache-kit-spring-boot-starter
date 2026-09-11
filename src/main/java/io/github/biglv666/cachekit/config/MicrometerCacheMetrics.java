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
 * —— 收发差值可观：sent 持续大于 received 总和说明存在广播丢失（由 L1 TTL 兜底）</p>
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

    MicrometerCacheMetrics(MeterRegistry registry) {
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
}
