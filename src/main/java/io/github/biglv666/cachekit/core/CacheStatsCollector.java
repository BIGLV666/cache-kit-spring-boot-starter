package io.github.biglv666.cachekit.core;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * 实体维度统计收集器：{@code /actuator/cachekit} 端点的数据源，与全局
 * {@link CacheMetricsListener}（Micrometer 计数器）并行工作、互不影响——
 * Micrometer 保持全局聚合口径，本收集器按实体前缀拆分命中率/回源/失效计数。
 *
 * <p>计数为进程内累计值（自启动起，非滑动窗口）；命中率由消费方按
 * {@code hit / (hit + miss)} 计算，分母为零时无意义。实体数为键前缀数
 * （受实体总数约束），无标签基数膨胀问题。失效计数仅覆盖带元数据的
 * {@code evictBatch} 路径（binlog 精确键路径无实体上下文，不计入）。</p>
 */
public class CacheStatsCollector {

    /** 单个实体的累计计数（LongAdder：高并发写、低频读快照） */
    public static final class EntityStats {
        private final LongAdder l1Hit = new LongAdder();
        private final LongAdder l1Miss = new LongAdder();
        private final LongAdder l2Hit = new LongAdder();
        private final LongAdder l2Miss = new LongAdder();
        private final LongAdder dbLoads = new LongAdder();
        private final LongAdder nullPlaceholders = new LongAdder();
        private final LongAdder evictions = new LongAdder();

        public long l1Hit() {
            return l1Hit.sum();
        }

        public long l1Miss() {
            return l1Miss.sum();
        }

        public long l2Hit() {
            return l2Hit.sum();
        }

        public long l2Miss() {
            return l2Miss.sum();
        }

        public long dbLoads() {
            return dbLoads.sum();
        }

        public long nullPlaceholders() {
            return nullPlaceholders.sum();
        }

        public long evictions() {
            return evictions.sum();
        }

        /** L1 命中率（hit / (hit+miss)），无请求时返回 -1 表示无数据 */
        public double l1HitRate() {
            return rate(l1Hit.sum(), l1Miss.sum());
        }

        /** L2 命中率（hit / (hit+miss)），无请求时返回 -1 表示无数据 */
        public double l2HitRate() {
            return rate(l2Hit.sum(), l2Miss.sum());
        }

        private static double rate(long hit, long miss) {
            long total = hit + miss;
            return total == 0 ? -1D : (double) hit / total;
        }
    }

    private final ConcurrentHashMap<String, EntityStats> entities = new ConcurrentHashMap<>();

    private EntityStats entity(String prefix) {
        return entities.computeIfAbsent(prefix == null || prefix.isBlank() ? "_" : prefix,
                p -> new EntityStats());
    }

    public void l1Lookup(String prefix, boolean hit) {
        EntityStats s = entity(prefix);
        (hit ? s.l1Hit : s.l1Miss).increment();
    }

    public void l2Lookup(String prefix, boolean hit) {
        EntityStats s = entity(prefix);
        (hit ? s.l2Hit : s.l2Miss).increment();
    }

    public void dbLoad(String prefix, int ids) {
        entity(prefix).dbLoads.add(ids);
    }

    public void nullPlaceholder(String prefix) {
        entity(prefix).nullPlaceholders.increment();
    }

    public void evict(String prefix, int keys) {
        entity(prefix).evictions.add(keys);
    }

    /** 全部实体的统计快照（键 = 实体缓存前缀）；只读视图，值为活引用（读到的计数可能略新） */
    public Map<String, EntityStats> snapshot() {
        return Collections.unmodifiableMap(entities);
    }
}
