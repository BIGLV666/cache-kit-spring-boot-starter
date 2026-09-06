package io.github.biglv666.cachekit.channel;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;

import java.time.Duration;

/**
 * L1 本地缓存通道（Caffeine）：每键独立 TTL（创建时生效，非滑动过期），
 * 统一存储 JSON 字符串，避免直接缓存可变对象实例被调用方污染。
 */
public class CaffeineChannel implements CacheChannel {

    /** 槽位：JSON 值 + 创建时记录的 TTL，供 Caffeine Expiry 读取 */
    private record Slot(String json, long ttlNanos) {
    }

    private final Cache<String, Slot> cache;

    public CaffeineChannel(long maxEntries) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxEntries)
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
    public void put(String key, String json, Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return;
        }
        cache.put(key, new Slot(json, ttl.toNanos()));
    }

    @Override
    public void evict(String key) {
        cache.invalidate(key);
    }
}
