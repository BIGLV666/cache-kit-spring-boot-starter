package io.github.biglv666.cachekit.channel;

import io.github.biglv666.cachekit.core.CacheMetricsListener;
import io.github.biglv666.cachekit.core.L2CircuitBreaker;
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
 * 业务读由 L1/DB 兜底——L2 故障不阻断三级链。连续失败达到阈值后由可选的
 * {@link L2CircuitBreaker} 短路：短路期间调用零开销降级（不再触达 Redis，
 * 不再逐次阻塞到命令超时），到期放行单个探测请求。</p>
 *
 * <p>所有降级路径计入 {@code l2.fallbacks} 指标（含短路），可通过
 * {@link #setMetricsListener} 挂载监听器观测。</p>
 */
public class RedisChannel implements CacheChannel {

    private static final Logger log = LoggerFactory.getLogger(RedisChannel.class);
    private static final long WARN_INTERVAL_NS = 30_000_000_000L;

    private final StringRedisTemplate template;
    private volatile long lastWarnAt;
    private volatile CacheMetricsListener metrics = new CacheMetricsListener() {
    };
    /** 可选熔断器：null 表示未接入（每次调用都真实触达 Redis） */
    private volatile L2CircuitBreaker breaker;

    public RedisChannel(StringRedisTemplate template) {
        this.template = template;
    }

    public StringRedisTemplate template() {
        return template;
    }

    /** 挂载指标监听器（降级计数 {@code l2.fallbacks} 的数据源） */
    public void setMetricsListener(CacheMetricsListener metrics) {
        this.metrics = metrics == null ? new CacheMetricsListener() {
        } : metrics;
    }

    /** 接入熔断器（可选）；短路判定与状态迁移见 {@link L2CircuitBreaker} */
    public void setCircuitBreaker(L2CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    /**
     * 获取调用资格：无熔断器或熔断器放行时返回 true；
     * 熔断短路时计一次降级（op 维度）并返回 false，调用方按降级处理。
     */
    private boolean acquire(String op) {
        L2CircuitBreaker b = breaker;
        if (b == null || b.tryAcquire()) {
            return true;
        }
        metrics.l2Fallback(op);
        return false;
    }

    private void success() {
        L2CircuitBreaker b = breaker;
        if (b != null) {
            b.recordSuccess();
        }
    }

    private void failure(String op, Exception e) {
        L2CircuitBreaker b = breaker;
        if (b != null) {
            b.recordFailure();
        }
        metrics.l2Fallback(op);
        warnDown(op, e);
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
        if (!acquire("get")) {
            return CacheEntry.miss();
        }
        try {
            String raw = template.opsForValue().get(key);
            success();
            return raw == null ? CacheEntry.miss() : CacheEntry.of(raw);
        } catch (Exception e) {
            failure("get", e);
            return CacheEntry.miss();
        }
    }

    @Override
    public void put(String key, String json, Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            return;
        }
        if (!acquire("put")) {
            return;
        }
        try {
            template.opsForValue().set(key, json, ttl);
            success();
        } catch (Exception e) {
            failure("put", e);
        }
    }

    @Override
    public void evict(String key) {
        if (!acquire("evict")) {
            return;
        }
        try {
            template.delete(key);
            success();
        } catch (Exception e) {
            failure("evict", e);
        }
    }

    @Override
    public boolean evictReliably(String key) {
        if (!acquire("evict")) {
            // 短路等价删除失败：调用方（失效路径）据此安排重试
            metrics.l2Fallback("evict");
            return false;
        }
        try {
            template.delete(key);
            success();
            return true;
        } catch (Exception e) {
            failure("evict", e);
            return false;
        }
    }

    @Override
    public boolean evictAll(Collection<String> keys) {
        if (keys.isEmpty()) {
            return true;
        }
        if (!acquire("evictAll")) {
            return false;
        }
        try {
            // StringRedisTemplate.delete(Collection) 汇成单条 DEL 多键命令，
            // 替代逐键 DEL（binlog 大事务的行事件风暴场景命令数从 N 降到 1）
            template.delete(keys);
            success();
            return true;
        } catch (Exception e) {
            failure("evictAll", e);
            return false;
        }
    }

    @Override
    public Map<String, CacheEntry> multiGet(List<String> keys) {
        if (!acquire("multiGet")) {
            // 短路返回全未命中映射（返回 null 会被调用方当"不支持批量"逐键重试，徒增计数）
            Map<String, CacheEntry> allMiss = new LinkedHashMap<>();
            for (String key : keys) {
                allMiss.put(key, CacheEntry.miss());
            }
            return allMiss;
        }
        try {
            List<String> raws = template.opsForValue().multiGet(keys);
            Map<String, CacheEntry> out = new LinkedHashMap<>();
            for (int i = 0; i < keys.size(); i++) {
                String raw = raws == null ? null : raws.get(i);
                out.put(keys.get(i), raw == null ? CacheEntry.miss() : CacheEntry.of(raw));
            }
            success();
            return out;
        } catch (Exception e) {
            failure("multiGet", e);
            return null; // 调用方回退逐键 get（同样降级）
        }
    }
}
