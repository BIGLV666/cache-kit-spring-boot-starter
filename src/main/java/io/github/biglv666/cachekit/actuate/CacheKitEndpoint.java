package io.github.biglv666.cachekit.actuate;

import io.github.biglv666.cachekit.channel.L1Channel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.CacheStatsCollector;
import io.github.biglv666.cachekit.core.DoubleDeleteScheduler;
import io.github.biglv666.cachekit.core.L2CircuitBreaker;
import io.github.biglv666.cachekit.core.StreamsInvalidationConsumer;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@code /actuator/cachekit} 运维端点（只读）：实体级命中率（L1/L2）、DB 回源与
 * null 占位计数、L1 条数估计、延迟双删积压、streams 消费组滞后、L2 熔断器状态。
 *
 * <p>安全边界：只输出实体前缀、TTL、计数与状态，**不输出任何缓存键值或缓存内容**；
 * 配置仅摘取运行观测相关字段（不含 binlog 账号等敏感项）。端点的暴露范围由宿主
 * actuator 配置管理（{@code management.endpoints.web.exposure.include}）。</p>
 *
 * <p>命中率为进程启动以来的累计值（hit / (hit+miss)），-1 表示无请求；
 * L1 条数为淘汰异步进行的估计值。</p>
 */
@Endpoint(id = "cachekit")
public class CacheKitEndpoint {

    /** 双删积压上限（DoubleDeleteScheduler.MAX_PENDING 的展示副本） */
    private static final int MAX_PENDING = 10_000;

    private final CacheKitProperties props;
    private final EntityMetadataRegistry registry;
    private final ObjectProvider<TieredEntityCache> cache;
    private final ObjectProvider<L1Channel> l1;
    private final ObjectProvider<DoubleDeleteScheduler> doubleDeleteScheduler;
    private final ObjectProvider<StreamsInvalidationConsumer> streamsConsumer;
    private final ObjectProvider<L2CircuitBreaker> circuitBreaker;
    private final ObjectProvider<CacheStatsCollector> stats;

    public CacheKitEndpoint(CacheKitProperties props,
                            EntityMetadataRegistry registry,
                            ObjectProvider<TieredEntityCache> cache,
                            ObjectProvider<L1Channel> l1,
                            ObjectProvider<DoubleDeleteScheduler> doubleDeleteScheduler,
                            ObjectProvider<StreamsInvalidationConsumer> streamsConsumer,
                            ObjectProvider<L2CircuitBreaker> circuitBreaker,
                            ObjectProvider<CacheStatsCollector> stats) {
        this.props = props;
        this.registry = registry;
        this.cache = cache;
        this.l1 = l1;
        this.doubleDeleteScheduler = doubleDeleteScheduler;
        this.streamsConsumer = streamsConsumer;
        this.circuitBreaker = circuitBreaker;
        this.stats = stats;
    }

    @ReadOperation
    public Map<String, Object> cacheKit() {
        Map<String, Object> out = new LinkedHashMap<>();

        out.put("keyNamespace", props.getKeyNamespace());
        out.put("singleFlightInflight", cacheProviderCount());

        Map<String, Object> l1Info = new LinkedHashMap<>();
        L1Channel l1Channel = l1.getIfAvailable();
        l1Info.put("estimatedSize", l1Channel == null ? null : l1Channel.estimatedSize());
        l1Info.put("ttl", props.getL1().getTtl().toString());
        l1Info.put("maxEntries", props.getL1().getMaxEntries());
        l1Info.put("maxWeightKb", props.getL1().getMaxWeightKb());
        out.put("l1", l1Info);

        Map<String, Object> l2Info = new LinkedHashMap<>();
        l2Info.put("ttl", props.getL2().getTtl().toString());
        l2Info.put("nullTtl", props.getL2().getNullTtl().toString());
        L2CircuitBreaker breaker = circuitBreaker.getIfAvailable();
        Map<String, Object> cb = new LinkedHashMap<>();
        cb.put("enabled", breaker != null);
        cb.put("state", breaker == null ? "DISABLED" : breaker.stateName());
        l2Info.put("circuitBreaker", cb);
        out.put("l2", l2Info);

        DoubleDeleteScheduler scheduler = doubleDeleteScheduler.getIfAvailable();
        Map<String, Object> dd = new LinkedHashMap<>();
        dd.put("pending", scheduler == null ? 0 : scheduler.pendingCount());
        dd.put("maxPending", MAX_PENDING);
        out.put("doubleDelete", dd);

        StreamsInvalidationConsumer consumer = streamsConsumer.getIfAvailable();
        if (consumer != null) {
            Map<String, Object> streams = new LinkedHashMap<>();
            streams.put("group", consumer.groupName());
            streams.put("lagSeconds", consumer.lastLagSeconds());
            out.put("streams", streams);
        }

        out.put("entities", entities());
        return out;
    }

    /** 实体列表：注册元数据（前缀/TTL/类型）与统计计数按前缀合并，前缀排序保证输出稳定 */
    private List<Map<String, Object>> entities() {
        CacheStatsCollector collector = stats.getIfAvailable();
        Map<String, CacheStatsCollector.EntityStats> statsMap = collector == null
                ? Map.of()
                : collector.snapshot();
        Map<String, Map<String, Object>> merged = new TreeMap<>();
        // 注册的实体即使尚无流量也要列出（前缀/TTL 是运维核对配置的依据）
        for (io.github.biglv666.cachekit.metadata.EntityMetadata meta : registry.entities()) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("prefix", meta.prefix());
            row.put("entity", meta.entityType().getSimpleName());
            row.put("ttl", meta.ttl() == null ? null : meta.ttl().toString());
            merged.put(meta.prefix(), row);
        }
        for (Map.Entry<String, CacheStatsCollector.EntityStats> e : statsMap.entrySet()) {
            Map<String, Object> row = merged.computeIfAbsent(e.getKey(), p -> {
                Map<String, Object> r = new LinkedHashMap<>();
                r.put("prefix", p);
                return r;
            });
            CacheStatsCollector.EntityStats s = e.getValue();
            row.put("l1Hit", s.l1Hit());
            row.put("l1Miss", s.l1Miss());
            row.put("l1HitRate", s.l1HitRate());
            row.put("l2Hit", s.l2Hit());
            row.put("l2Miss", s.l2Miss());
            row.put("l2HitRate", s.l2HitRate());
            row.put("dbLoads", s.dbLoads());
            row.put("nullPlaceholders", s.nullPlaceholders());
            row.put("evictions", s.evictions());
        }
        return new ArrayList<>(merged.values());
    }

    private int cacheProviderCount() {
        TieredEntityCache c = cache.getIfAvailable();
        return c == null ? 0 : c.inflightCount();
    }
}
