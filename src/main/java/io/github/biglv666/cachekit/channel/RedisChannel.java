package io.github.biglv666.cachekit.channel;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;

/**
 * L2 远程缓存通道（Redis，String 结构），TTL 语义与 L1 对齐。
 */
public class RedisChannel implements CacheChannel {

    private final StringRedisTemplate template;

    public RedisChannel(StringRedisTemplate template) {
        this.template = template;
    }

    public StringRedisTemplate template() {
        return template;
    }

    @Override
    public CacheEntry get(String key) {
        String raw = template.opsForValue().get(key);
        return raw == null ? CacheEntry.miss() : CacheEntry.of(raw);
    }

    @Override
    public void put(String key, String json, Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return;
        }
        template.opsForValue().set(key, json, ttl);
    }

    @Override
    public void evict(String key) {
        template.delete(key);
    }
}
