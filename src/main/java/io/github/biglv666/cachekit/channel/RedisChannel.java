package io.github.biglv666.cachekit.channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * L2 远程缓存通道（Redis，String 结构），TTL 语义与 L1 对齐。
 *
 * <p>Redis 不可用时**降级而非失败**：get 视为未命中、put/evict 静默跳过（限频告警），
 * 业务读由 L1/DB 兜底——L2 故障不阻断三级链。
 */
public class RedisChannel implements CacheChannel {

    private static final Logger log = LoggerFactory.getLogger(RedisChannel.class);
    private static final long WARN_INTERVAL_NS = 30_000_000_000L;

    private final StringRedisTemplate template;
    private volatile long lastWarnAt;

    public RedisChannel(StringRedisTemplate template) {
        this.template = template;
    }

    public StringRedisTemplate template() {
        return template;
    }

    private void warnDown(String op, Exception e) {
        long now = System.nanoTime();
        if (now - lastWarnAt > WARN_INTERVAL_NS) {
            lastWarnAt = now;
            log.warn("Redis {} 失败，L2 降级为不可用（本告警 30s 内不重复）: {}", op, e.getMessage());
        }
    }

    @Override
    public CacheEntry get(String key) {
        try {
            String raw = template.opsForValue().get(key);
            return raw == null ? CacheEntry.miss() : CacheEntry.of(raw);
        } catch (Exception e) {
            warnDown("get", e);
            return CacheEntry.miss();
        }
    }

    @Override
    public void put(String key, String json, Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return;
        }
        try {
            template.opsForValue().set(key, json, ttl);
        } catch (Exception e) {
            warnDown("put", e);
        }
    }

    @Override
    public void evict(String key) {
        try {
            template.delete(key);
        } catch (Exception e) {
            warnDown("evict", e);
        }
    }

    @Override
    public boolean evictReliably(String key) {
        try {
            template.delete(key);
            return true;
        } catch (Exception e) {
            warnDown("evict", e);
            return false;
        }
    }

    @Override
    public boolean evictAll(Collection<String> keys) {
        if (keys.isEmpty()) {
            return true;
        }
        try {
            // StringRedisTemplate.delete(Collection) 汇成单条 DEL 多键命令，
            // 替代逐键 DEL（binlog 大事务的行事件风暴场景命令数从 N 降到 1）
            template.delete(keys);
            return true;
        } catch (Exception e) {
            warnDown("evictAll", e);
            return false;
        }
    }

    @Override
    public Map<String, CacheEntry> multiGet(List<String> keys) {
        try {
            List<String> raws = template.opsForValue().multiGet(keys);
            Map<String, CacheEntry> out = new LinkedHashMap<>();
            for (int i = 0; i < keys.size(); i++) {
                String raw = raws == null ? null : raws.get(i);
                out.put(keys.get(i), raw == null ? CacheEntry.miss() : CacheEntry.of(raw));
            }
            return out;
        } catch (Exception e) {
            warnDown("multiGet", e);
            return null; // 调用方回退逐键 get（同样降级）
        }
    }
}
