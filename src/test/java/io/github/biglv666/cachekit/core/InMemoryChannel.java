package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CacheChannel;
import io.github.biglv666.cachekit.channel.CacheEntry;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 测试用内存通道：不处理 TTL，仅验证链路逻辑。
 */
public class InMemoryChannel implements CacheChannel {

    public final Map<String, String> store = new ConcurrentHashMap<>();
    public int putCount;
    public int evictCount;

    @Override
    public CacheEntry get(String key) {
        String raw = store.get(key);
        return raw == null ? CacheEntry.miss() : CacheEntry.of(raw);
    }

    @Override
    public void put(String key, String json, Duration ttl) {
        putCount++;
        store.put(key, json);
    }

    @Override
    public void evict(String key) {
        evictCount++;
        store.remove(key);
    }
}
