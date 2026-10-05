package io.github.biglv666.cachekit.channel;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;

import java.time.Duration;

/**
 * L1 本地缓存通道（Caffeine）：每键独立 TTL（创建时生效，非滑动过期），
 * 统一存储 JSON 字符串，避免直接缓存可变对象实例被调用方污染。
 */
public class CaffeineChannel implements L1Channel {

    /** 槽位：JSON 值 + 写入时刻起算的绝对过期点（nanoTime），供 Caffeine Expiry 与剩余 TTL 读取 */
    private record Slot(String json, long ttlNanos, long expireAtNanos) {
    }

    private final Cache<String, Slot> cache;

    public CaffeineChannel(long maxEntries) {
        this(maxEntries, 0);
    }

    /**
     * @param maxEntries  最大条目数（maxWeightKb &le; 0 时生效）
     * @param maxWeightKb 权重上限（单位：K 字符，按序列化 JSON 的 UTF-16 字符数计，非字节）：
     *                    &gt;0 时启用基于权重的淘汰，防止少量大实体（含大字段）撑爆本地内存。
     *                    中文等 BMP 字符 JVM 内存占用约为字符数 2 倍，实际内存上限 ≈ maxWeightKb × 2KB
     */
    public CaffeineChannel(long maxEntries, long maxWeightKb) {
        com.github.benmanes.caffeine.cache.Caffeine<Object, Object> builder = Caffeine.newBuilder();
        if (maxWeightKb > 0) {
            // newBuilder() 固定返回 Caffeine<Object,Object>（3.2.x 非泛型），键实为 String、值为 Slot
            builder.maximumWeight(maxWeightKb)
                    .weigher((com.github.benmanes.caffeine.cache.Weigher<Object, Object>)
                            (key, value) -> Math.max(1, ((Slot) value).json().length() / 1024));
        } else {
            builder.maximumSize(maxEntries);
        }
        this.cache = builder
                .expireAfter(new Expiry<String, Slot>() {
                    @Override
                    public long expireAfterCreate(String key, Slot slot, long now) {
                        return slot.ttlNanos();
                    }

                    @Override
                    public long expireAfterUpdate(String key, Slot slot, long now, long currentDuration) {
                        return slot.ttlNanos();
                    }

                    @Override
                    public long expireAfterRead(String key, Slot slot, long now, long currentDuration) {
                        // 读操作不续期：返回 currentDuration 保持原过期时间（返回 MAX_VALUE 会变成永不过期）
                        return currentDuration;
                    }
                })
                .build();
    }

    @Override
    public CacheEntry get(String key) {
        Slot slot = cache.getIfPresent(key);
        return slot == null ? CacheEntry.miss() : CacheEntry.of(slot.json());
    }

    @Override
    public long remainingTtlNanos(String key) {
        Slot slot = cache.getIfPresent(key);
        if (slot == null) {
            return -1;
        }
        long remaining = slot.expireAtNanos() - System.nanoTime();
        // 到期但尚未被异步清理的条目按剩余 0 处理（getIfPresent 不触发过期判定）
        return Math.max(0, remaining);
    }

    @Override
    public void put(String key, String json, Duration ttl) {
        // 非正 TTL 的统一语义是"跳过写入（禁用该级缓存）"，不是"永不过期"——
        // 组件的脏数据安全模型建立在 TTL 上界之上
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return;
        }
        cache.put(key, new Slot(json, ttl.toNanos(), System.nanoTime() + ttl.toNanos()));
    }

    @Override
    public void evict(String key) {
        cache.invalidate(key);
    }

    @Override
    public long estimatedSize() {
        // 清理异步进行，估计值可能与真实条目数有偏差（运维观测用，非精确值）
        return cache.estimatedSize();
    }
}
