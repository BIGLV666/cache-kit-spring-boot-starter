package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CacheChannel;
import io.github.biglv666.cachekit.channel.CacheEntry;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.support.JsonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * 三级链核心：L1（Caffeine）→ L2（Redis，可选）→ loader（查 DB）read-through，
 * 命中逐级回填；写路径"先写 DB 后删缓存"，删除含广播与延迟双删。
 *
 * <p>并发控制：同 key 回源经 single-flight 合并，防击穿；null 结果按配置短 TTL 缓存防穿透；
 * TTL 追加随机抖动防雪崩。一致性语义为秒级最终一致（见 README）。</p>
 */
public class TieredEntityCache {

    /**
     * {@link #peek} 的三态结果：命中值 / 命中 null 占位（该 ID 已知不存在）/ 未命中。
     */
    public record CachePeek(State state, Object value) {

        public enum State {
            HIT, HIT_NULL, MISS
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TieredEntityCache.class);

    /** 解码结果标记：命中的是 null 占位 */
    private static final Object NULL_VALUE = new Object();
    /** 解码结果标记：JSON 反序列化失败（实体结构漂移），按未命中继续 */
    private static final Object DECODE_FAILED = new Object();

    private final CacheKitProperties props;
    private final CacheChannel l1;
    private final CacheChannel l2;
    private final InvalidationPublisher publisher;
    private final DoubleDeleteScheduler doubleDeleteScheduler;

    /** single-flight：同 key 的并发回源合并为一个执行 */
    private final ConcurrentHashMap<String, CompletableFuture<Object>> inflight = new ConcurrentHashMap<>();

    public TieredEntityCache(CacheKitProperties props,
                             CacheChannel l1,
                             CacheChannel l2,
                             InvalidationPublisher publisher,
                             DoubleDeleteScheduler doubleDeleteScheduler) {
        this.props = props;
        this.l1 = l1;
        this.l2 = l2;
        this.publisher = publisher;
        this.doubleDeleteScheduler = doubleDeleteScheduler;
    }

    /**
     * 三级 read-through 读取。
     *
     * @param meta       实体元数据
     * @param id         主键值
     * @param ttlOverride 方法级 TTL 覆盖，null 用全局/实体级
     * @param cacheNull  是否缓存 null 结果
     * @param loader     DB 加载逻辑（在 L1/L2 均未命中时执行）
     * @return 实体或 null
     */
    public Object load(EntityMetadata meta, Object id, Duration ttlOverride, boolean cacheNull,
                       Supplier<Object> loader) {
        if (BypassContext.isActive()) {
            return loader.get();
        }
        String key = meta.keyOf(id);

        CacheEntry c1 = l1.get(key);
        Object v = c1.hit() ? decode(key, c1.json(), meta) : DECODE_FAILED;
        if (v != DECODE_FAILED) {
            return v == NULL_VALUE ? null : v;
        }

        if (l2 != null) {
            CacheEntry c2 = l2Get(key);
            if (c2.hit()) {
                v = decode(key, c2.json(), meta);
                if (v != DECODE_FAILED) {
                    if (v != NULL_VALUE) {
                        // L2 命中回填 L1，TTL 用 L1 全局配置
                        l1.put(key, c2.json(), props.getL1().getTtl());
                    }
                    return v == NULL_VALUE ? null : v;
                }
            }
        }

        // L1/L2 均未命中：single-flight 回源，同 key 并发只执行一次 loader。
        // 等待者先取得 future 引用再 join，与 map 的移除时机解耦——
        // 否则回源线程完成后的 remove 会与等待者的唤醒竞态，导致映射函数被重复执行
        CompletableFuture<Object> inflightFuture = inflight.get(key);
        if (inflightFuture == null) {
            CompletableFuture<Object> created = new CompletableFuture<>();
            CompletableFuture<Object> prev = inflight.putIfAbsent(key, created);
            if (prev == null) {
                // 本线程赢得回源权
                try {
                    Object db = loader.get();
                    if (db == null) {
                        if (cacheNull) {
                            // null 占位：防穿透，两级都用短 TTL
                            putBoth(key, JsonCodec.NULL_SENTINEL, props.getL2().getNullTtl(), props.getL2().getNullTtl());
                        }
                    } else {
                        // 序列化失败不丢业务数据：返回结果只是不缓存（实体含不支持的类型时）
                        String json = null;
                        try {
                            json = JsonCodec.write(db);
                        } catch (Exception e) {
                            log.warn("缓存值序列化失败，本次结果不缓存: {}", meta.entityType().getName(), e);
                        }
                        if (json != null) {
                            putBoth(key, json, effectiveTtl(meta, ttlOverride), props.getL1().getTtl());
                        }
                    }
                    created.complete(db);
                    return db;
                } catch (Throwable t) {
                    created.completeExceptionally(t);
                    throw t instanceof RuntimeException re ? re : new CacheKitException(t);
                } finally {
                    inflight.remove(key, created);
                }
            }
            inflightFuture = prev;
        }

        try {
            return inflightFuture.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw cause instanceof RuntimeException re ? re : new CacheKitException(cause);
        }
    }

    /**
     * 失效：立即执行一次完整删除，并按配置调度延迟双删。
     */
    public void evict(EntityMetadata meta, Object id) {
        evictBatch(meta, List.of(id), true);
    }

    /**
     * 批量失效：逐键立即删除 + 广播；延迟双删整个批次只调度一次任务（合并，防行事件风暴）。
     *
     * @param withDoubleDelete  false 时跳过延迟双删（用于调用方自行合并非逐行双删的场景）
     */
    public void evictBatch(EntityMetadata meta, Iterable<Object> ids, boolean withDoubleDelete) {
        List<String> keys = new ArrayList<>();
        for (Object id : ids) {
            if (id != null) {
                keys.add(meta.keyOf(id));
            }
        }
        for (String key : keys) {
            evictOnce(key);
        }
        if (withDoubleDelete && doubleDeleteScheduler != null && !keys.isEmpty()) {
            doubleDeleteScheduler.schedule(() -> keys.forEach(this::evictOnce));
        }
    }

    /**
     * 不触发回源的缓存窥探：供批量查询做"命中/已缓存空/未命中"三态拆分。
     */
    public CachePeek peek(EntityMetadata meta, Object id) {
        String key = meta.keyOf(id);
        CacheEntry c1 = l1.get(key);
        if (c1.hit()) {
            return toPeek(decode(key, c1.json(), meta));
        }
        if (l2 != null) {
            CacheEntry c2 = l2Get(key);
            if (c2.hit()) {
                Object v = decode(key, c2.json(), meta);
                if (v != DECODE_FAILED) {
                    if (v != NULL_VALUE) {
                        l1.put(key, c2.json(), props.getL1().getTtl());
                    }
                    return toPeek(v);
                }
            }
        }
        return new CachePeek(CachePeek.State.MISS, null);
    }

    /** L2 全部调用经此降级：通道抛异常（Redis 宕机等）按未命中处理，限频告警，绝不阻断业务读写 */
    private CacheEntry l2Get(String key) {
        try {
            return l2.get(key);
        } catch (Exception e) {
            warnL2Failure("get", e);
            return CacheEntry.miss();
        }
    }

    private Map<String, CacheEntry> l2MultiGet(List<String> keys) {
        try {
            return l2.multiGet(keys);
        } catch (Exception e) {
            warnL2Failure("multiGet", e);
            return null;
        }
    }

    private void l2Put(String key, String json, Duration ttl) {
        try {
            l2.put(key, json, ttl);
        } catch (Exception e) {
            warnL2Failure("put", e);
        }
    }

    private void l2Evict(String key) {
        try {
            l2.evict(key);
        } catch (Exception e) {
            warnL2Failure("evict", e);
        }
    }

    private volatile long lastL2WarnAt;

    private void warnL2Failure(String op, Exception e) {
        long now = System.nanoTime();
        if (now - lastL2WarnAt > 30_000_000_000L) {
            lastL2WarnAt = now;
            log.warn("L2 通道 {} 失败，降级为不可用（本告警 30s 内不重复）: {}", op, e.getMessage());
        }
    }

    private CachePeek toPeek(Object decoded) {
        if (decoded == DECODE_FAILED) {
            return new CachePeek(CachePeek.State.MISS, null);
        }
        if (decoded == NULL_VALUE) {
            return new CachePeek(CachePeek.State.HIT_NULL, null);
        }
        return new CachePeek(CachePeek.State.HIT, decoded);
    }

    /** 批量流程写回单个命中实体（序列化失败只跳过缓存，不抛出） */
    public void cachePut(EntityMetadata meta, Object id, Object value, Duration ttlOverride) {
        String json;
        try {
            json = JsonCodec.write(value);
        } catch (Exception e) {
            log.warn("缓存值序列化失败，跳过缓存: {}", meta.entityType().getName(), e);
            return;
        }
        putBoth(meta.keyOf(id), json, effectiveTtl(meta, ttlOverride), props.getL1().getTtl());
    }

    /**
     * 批量三态窥探：L1 逐键（本地内存），L2 缺失部分走 {@link CacheChannel#multiGet}（MGET 管道）。
     * 结果顺序与入参 ids 一一对应。
     */
    public List<CachePeek> peekBatch(EntityMetadata meta, List<Object> ids) {
        List<CachePeek> out = new ArrayList<>(ids.size());
        List<String> missKeys = new ArrayList<>();
        List<Integer> missIdx = new ArrayList<>();
        for (Object id : ids) {
            String key = meta.keyOf(id);
            CacheEntry c1 = l1.get(key);
            CachePeek p = c1.hit() ? toPeek(decode(key, c1.json(), meta))
                    : new CachePeek(CachePeek.State.MISS, null);
            out.add(p);
            if (p.state() == CachePeek.State.MISS) {
                missKeys.add(key);
                missIdx.add(out.size() - 1);
            }
        }
        if (!missKeys.isEmpty() && l2 != null) {
            Map<String, CacheEntry> l2res = l2MultiGet(missKeys);
            if (l2res == null) {
                // 通道不支持批量：逐键 get（同样降级语义）
                l2res = new LinkedHashMap<>();
                for (String key : missKeys) {
                    l2res.put(key, l2Get(key));
                }
            }
            for (int i = 0; i < missIdx.size(); i++) {
                String key = missKeys.get(i);
                CacheEntry entry = l2res.get(key);
                if (entry == null || !entry.hit()) {
                    continue;
                }
                CachePeek p = toPeek(decode(key, entry.json(), meta));
                if (p.state() != CachePeek.State.MISS) {
                    if (p.state() == CachePeek.State.HIT) {
                        l1.put(key, entry.json(), props.getL1().getTtl());
                    }
                    out.set(missIdx.get(i), p);
                }
            }
        }
        return out;
    }

    /** 批量流程写回"已缓存空"占位（防穿透） */
    public void cacheNull(EntityMetadata meta, Object id) {
        putBoth(meta.keyOf(id), JsonCodec.NULL_SENTINEL, props.getL2().getNullTtl(), props.getL2().getNullTtl());
    }

    /** 单次完整删除：本地 L1 + L2 + 广播 */
    private void evictOnce(String key) {
        try {
            l1.evict(key);
            if (l2 != null) {
                l2Evict(key);
            }
            publisher.publish(key);
        } catch (Exception e) {
            log.warn("缓存失效执行异常，key={}", key, e);
        }
    }

    private void putBoth(String key, String json, Duration l2Ttl, Duration l1Ttl) {
        if (l2 != null) {
            l2Put(key, json, l2Ttl);
        }
        l1.put(key, json, l1Ttl);
    }

    /** 实际写入 L2 的 TTL：方法级覆盖 &gt; 实体级 &gt; 全局，再叠加随机抖动 */
    private Duration effectiveTtl(EntityMetadata meta, Duration override) {
        Duration base = override != null ? override
                : (meta.ttl() != null ? meta.ttl() : props.getL2().getTtl());
        Duration jitter = props.getL2().getJitter();
        if (jitter == null || jitter.isZero() || jitter.isNegative()) {
            return base;
        }
        long jitterMillis = jitter.toMillis();
        return base.plusMillis(ThreadLocalRandom.current().nextLong(jitterMillis + 1));
    }

    /**
     * 解码 JSON：null 占位 → {@link #NULL_VALUE}；解析失败（实体结构漂移）→ 失效该键并
     * 返回 {@link #DECODE_FAILED}，调用方继续走下一级。
     */
    private Object decode(String key, String json, EntityMetadata meta) {
        if (JsonCodec.NULL_SENTINEL.equals(json)) {
            return NULL_VALUE;
        }
        Object value = JsonCodec.read(json, meta.entityType());
        if (value == null) {
            log.warn("缓存值反序列化失败，已失效该键（实体结构可能已变更）: {}", key);
            evictOnce(key);
            return DECODE_FAILED;
        }
        return value;
    }
}
