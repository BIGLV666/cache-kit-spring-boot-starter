package io.github.biglv666.cachekit.generic;

import io.github.biglv666.cachekit.channel.CacheChannel;
import io.github.biglv666.cachekit.channel.CacheEntry;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.BypassContext;
import io.github.biglv666.cachekit.core.CacheKeyCustomizer;
import io.github.biglv666.cachekit.core.CacheMetricsListener;
import io.github.biglv666.cachekit.core.CacheStatsCollector;
import io.github.biglv666.cachekit.core.DoubleDeleteScheduler;
import io.github.biglv666.cachekit.core.GenericKeyCodec;
import io.github.biglv666.cachekit.core.InvalidationPublisher;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.support.JsonCodec;
import com.fasterxml.jackson.databind.JavaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 通用值缓存三级链：L1（Caffeine）→ L2（Redis，可选）→ loader（用户回填方法），
 * 命中逐级回填；失效包含 L2/L1 删除、广播与延迟双删。
 *
 * <p>与 {@code TieredEntityCache} 的区别：键完全由调用方给出（不再按实体主键组装），
 * 值类型用 Jackson {@link JavaType} 描述（支持 {@code List<T>} 等泛型）。
 * 并发控制、null 占位、TTL 抖动、压缩、大值保护、L2 降级语义与实体缓存保持一致。</p>
 */
public class TieredValueCache {

    private static final Logger log = LoggerFactory.getLogger(TieredValueCache.class);

    /** L2 压缩值标记前缀（与实体缓存共用格式，关闭压缩后存量值仍可读） */
    static final String GZ_PREFIX = "gz:";

    private static final Object NULL_VALUE = new Object();
    private static final Object DECODE_FAILED = new Object();
    private static final int MAX_L2_EVICT_RETRIES = 3;
    private static final String STATS_PREFIX = CacheStatsCollector.GENERIC_STATS_PREFIX;

    private final CacheKitProperties props;
    private final CacheChannel l1;
    private final CacheChannel l2;
    private final InvalidationPublisher publisher;
    private final DoubleDeleteScheduler doubleDeleteScheduler;
    private final String namespace;
    private final List<CacheKeyCustomizer> keyCustomizers;

    private final ConcurrentHashMap<String, CompletableFuture<Object>> inflight = new ConcurrentHashMap<>();

    private final long maxValueChars;
    private final boolean compressionEnabled;
    private final long compressionMinChars;
    private final int maxKeyLength;

    private volatile CacheMetricsListener metrics = new CacheMetricsListener() {
    };
    private volatile CacheStatsCollector stats;
    private volatile long lastL2WarnAt;
    private volatile long lastKeyCustomizerWarnAt;
    private volatile long lastOversizedWarnAt;

    public TieredValueCache(CacheKitProperties props,
                            CacheChannel l1,
                            CacheChannel l2,
                            InvalidationPublisher publisher,
                            DoubleDeleteScheduler doubleDeleteScheduler,
                            String namespace,
                            List<CacheKeyCustomizer> keyCustomizers) {
        this.props = props;
        this.l1 = l1;
        this.l2 = l2;
        this.publisher = publisher;
        this.doubleDeleteScheduler = doubleDeleteScheduler;
        this.namespace = namespace == null ? "" : namespace;
        this.keyCustomizers = keyCustomizers == null ? List.of() : keyCustomizers;
        this.maxValueChars = props.getL2().getMaxValueKb() > 0
                ? props.getL2().getMaxValueKb() * 1024
                : Long.MAX_VALUE;
        this.compressionEnabled = props.getL2().isCompressionEnabled();
        this.compressionMinChars = Math.max(1, props.getL2().getCompressionMinKb()) * 1024;
        this.maxKeyLength = props.getGeneric().getMaxKeyLength();
    }

    /** 挂载指标监听器 */
    public void setMetricsListener(CacheMetricsListener metrics) {
        this.metrics = metrics == null ? new CacheMetricsListener() {
        } : metrics;
    }

    /** 挂载实体维度统计收集器（以 {@code generic:前缀} 区分） */
    public void setStatsCollector(CacheStatsCollector stats) {
        this.stats = stats;
    }

    /** 当前 single-flight 回源中的键数（观测用） */
    public int inflightCount() {
        return inflight.size();
    }

    /** 可抛受检异常的 loader：{@code @CacheLoad} 方法声明的异常需要原样传播给调用方。 */
    @FunctionalInterface
    public interface ThrowingLoader {
        Object load() throws Throwable;
    }

    /**
     * read-through 读取：L1 → L2 → loader，缺失回源并逐级回填。
     * Supplier 不能声明受检异常；需要透传受检异常时使用 {@link #loadChecked}。
     */
    public Object load(GenericCacheMetadata meta, String userKey, JavaType valueType,
                       Duration ttlOverride, boolean cacheNull, Supplier<Object> loader) {
        try {
            return loadChecked(meta, userKey, valueType, ttlOverride, cacheNull, loader::get);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            // Supplier.get() 不可能抛受检异常；保留兜底以免静默吞掉异常
            throw new CacheKitException(t);
        }
    }

    /**
     * read-through 读取（可抛受检异常）：命中直接返回；未命中执行 loader，异常原样传播，
     * 不写入缓存，single-flight 等待者收到同一异常实例。
     */
    public Object loadChecked(GenericCacheMetadata meta, String userKey, JavaType valueType,
                              Duration ttlOverride, boolean cacheNull, ThrowingLoader loader)
            throws Throwable {
        if (BypassContext.isActive()) {
            return loader.load();
        }
        String key = key(meta, userKey);
        return loadResolved(meta, key, valueType, ttlOverride, cacheNull, loader);
    }

    /** 只窥探缓存，不触发回源；未命中/命中 null 占位都返回 null */
    public Object peek(GenericCacheMetadata meta, String userKey, JavaType valueType) {
        if (BypassContext.isActive()) {
            return null;
        }
        String key = key(meta, userKey);
        CacheEntry c1 = l1.get(key);
        metrics.l1Lookup(c1.hit());
        statL1(meta, c1.hit());
        if (c1.hit()) {
            Object v = decode(key, c1.json(), valueType);
            if (v != DECODE_FAILED) {
                return v == NULL_VALUE ? null : v;
            }
        }
        if (l2 != null) {
            CacheEntry c2 = l2Get(key);
            metrics.l2Lookup(c2.hit());
            statL2(meta, c2.hit());
            if (c2.hit()) {
                Object v = decode(key, c2.json(), valueType);
                if (v != DECODE_FAILED) {
                    if (v != NULL_VALUE) {
                        l1.put(key, c2.json(), l1TtlFor(effectiveBase(meta, null)));
                    }
                    return v == NULL_VALUE ? null : v;
                }
            }
        }
        return null;
    }

    /** 主动写入/更新；TTL 覆盖为 null 时使用值类型级或全局配置 */
    public void put(GenericCacheMetadata meta, String userKey, Object value, Duration ttlOverride) {
        String key = key(meta, userKey);
        if (value == null) {
            if (meta.cacheNull() && nullPlaceholderEnabled()) {
                Duration nullTtl = props.getL2().getNullTtl();
                putBoth(key, JsonCodec.NULL_SENTINEL, nullTtl, l1TtlFor(nullTtl));
                metrics.nullPlaceholder();
                statNullPlaceholder(meta);
            } else {
                // 空值不可缓存（cacheNull=false 或 null-ttl 非正）：必须失效旧值，
                // 否则显式更新会静默变成 no-op，读继续返回旧值直到 TTL
                evict(meta, userKey);
            }
            return;
        }
        String json;
        try {
            json = JsonCodec.write(value);
        } catch (Exception e) {
            log.warn("通用缓存值序列化失败，本次更新不缓存并失效旧值: {}", meta.valueType().getName(), e);
            evict(meta, userKey);
            return;
        }
        Duration l2Ttl = effectiveTtl(meta, ttlOverride);
        if (!putBoth(key, json, l2Ttl, l1TtlFor(effectiveBase(meta, ttlOverride)))) {
            // 超过 l2.max-value-kb：与序列化失败同语义，旧值必须失效，
            // 否则显式更新成功返回但读继续拿到旧值
            log.warn("通用缓存值更新未写入（超过 l2.max-value-kb），已失效旧值: {}", key);
            evict(meta, userKey);
        }
    }

    /** 立即失效单个键（含广播与延迟双删）；事务感知由 CacheStore 包装 */
    public void evict(GenericCacheMetadata meta, String userKey) {
        evict(meta, userKey, true);
    }

    /**
     * 立即失效单个键。
     *
     * @param scheduleDoubleDelete 是否调度延迟双删。普通 evict 需要；强制 refresh
     *                             随后会写入新值，若再调度双删会把新值删掉，必须传 false
     */
    public void evict(GenericCacheMetadata meta, String userKey, boolean scheduleDoubleDelete) {
        String key = key(meta, userKey);
        evictKeys(Set.of(key), 0, false);
        if (scheduleDoubleDelete && doubleDeleteScheduler != null) {
            doubleDeleteScheduler.schedule(() -> evictKeys(Set.of(key), 0, true));
        }
        statEvict(meta, 1);
    }

    private Object loadResolved(GenericCacheMetadata meta, String key, JavaType valueType,
                                Duration ttlOverride, boolean cacheNull, ThrowingLoader loader)
            throws Throwable {
        CacheEntry c1 = l1.get(key);
        metrics.l1Lookup(c1.hit());
        statL1(meta, c1.hit());
        Object v = c1.hit() ? decode(key, c1.json(), valueType) : DECODE_FAILED;
        if (v != DECODE_FAILED) {
            return v == NULL_VALUE ? null : v;
        }

        if (l2 != null) {
            CacheEntry c2 = l2Get(key);
            metrics.l2Lookup(c2.hit());
            statL2(meta, c2.hit());
            if (c2.hit()) {
                Object decoded = decode(key, c2.json(), valueType);
                if (decoded != DECODE_FAILED) {
                    if (decoded != NULL_VALUE) {
                        l1.put(key, c2.json(), l1TtlFor(effectiveBase(meta, ttlOverride)));
                    }
                    return decoded == NULL_VALUE ? null : decoded;
                }
            }
        }

        CompletableFuture<Object> existing = inflight.get(key);
        if (existing == null) {
            CompletableFuture<Object> created = new CompletableFuture<>();
            CompletableFuture<Object> prev = inflight.putIfAbsent(key, created);
            if (prev == null) {
                try {
                    // 二次检查：上一个 leader 可能在本线程"读 L1 未命中"之后才写入 L1 并
                    // 从 inflight 移除，本线程随后才成为 leader。此时重读 L1 可直接复用
                    // 刚写入的值，避免同一 key 在单飞窗口边缘重复回源（压测下每 key 必一次）。
                    CacheEntry rechecked = l1.get(key);
                    if (rechecked.hit()) {
                        Object ready = decode(key, rechecked.json(), valueType);
                        if (ready != DECODE_FAILED) {
                            created.complete(rechecked.json());
                            return ready == NULL_VALUE ? null : ready;
                        }
                    }
                    Object loaded = loader.load();
                    Object payload;
                    if (loaded == null) {
                        // null-ttl 非正表示显式关闭负缓存：两级都不写占位
                        // （l1TtlFor 对非正基准会回退 l1.ttl，必须在此处拦截）
                        boolean cachePlaceholder = cacheNull && nullPlaceholderEnabled();
                        if (cachePlaceholder) {
                            Duration nullTtl = props.getL2().getNullTtl();
                            putBoth(key, JsonCodec.NULL_SENTINEL, nullTtl, l1TtlFor(nullTtl));
                            metrics.nullPlaceholder();
                            statNullPlaceholder(meta);
                        }
                        payload = cachePlaceholder ? JsonCodec.NULL_SENTINEL : null;
                    } else {
                        String json = null;
                        try {
                            json = JsonCodec.write(loaded);
                        } catch (Exception e) {
                            log.warn("通用缓存值序列化失败，本次结果不缓存: {}", meta.valueType().getName(), e);
                        }
                        if (json != null) {
                            putBoth(key, json, effectiveTtl(meta, ttlOverride),
                                    l1TtlFor(effectiveBase(meta, ttlOverride)));
                            payload = json;
                        } else {
                            payload = loaded;
                        }
                    }
                    metrics.dbLoad(1);
                    statDbLoad(meta, 1);
                    created.complete(payload);
                    return loaded;
                } catch (Throwable t) {
                    created.completeExceptionally(t);
                    throw t;
                } finally {
                    inflight.remove(key, created);
                }
            }
            existing = prev;
        }

        try {
            return materialize(key, valueType, existing.join());
        } catch (CompletionException e) {
            // join() 的 cause 就是 leader 传给 completeExceptionally 的原异常实例
            throw e.getCause() != null ? e.getCause() : e;
        }
    }

    private Object materialize(String key, JavaType valueType, Object payload) {
        if (payload == null) {
            return null;
        }
        if (payload instanceof String json) {
            Object v = decode(key, json, valueType);
            return v == DECODE_FAILED ? null : v == NULL_VALUE ? null : v;
        }
        return payload;
    }

    /**
     * 完整键组装：自定义段 + namespace + 保留段 + prefix + 用户 key。
     * 过长用户 key 做 SHA-256 归一化，避免 Redis 大键与日志污染。
     */
    public String key(GenericCacheMetadata meta, String userKey) {
        String normalized = normalizeUserKey(meta, userKey);
        StringBuilder sb = new StringBuilder();
        for (CacheKeyCustomizer customizer : keyCustomizers) {
            String segment;
            try {
                segment = customizer.segment();
            } catch (Exception e) {
                warnKeyCustomizerFailure(customizer, e);
                continue;
            }
            if (segment != null && !segment.isBlank()) {
                sb.append(segment).append(':');
            }
        }
        if (!namespace.isBlank()) {
            sb.append(namespace).append(':');
        }
        return sb.append(GenericKeyCodec.SEGMENT).append(':')
                .append(meta.prefix()).append(':').append(normalized).toString();
    }

    private String normalizeUserKey(GenericCacheMetadata meta, String userKey) {
        if (userKey == null || userKey.isBlank()) {
            throw new CacheKitException("通用缓存 key 不能为空: " + meta.valueType().getName());
        }
        if (userKey.length() <= maxKeyLength) {
            return userKey;
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(userKey.getBytes(StandardCharsets.UTF_8));
            return "h:" + HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            // 理论上不会发生：退化为截断，仍保证不同 key 的前缀段可区分
            log.warn("通用缓存 key SHA-256 计算失败，按截断处理: {}", e.getMessage());
            return userKey.substring(0, maxKeyLength);
        }
    }

    private void evictKeys(Set<String> keys, int attempt, boolean allowRetry) {
        if (keys.isEmpty()) {
            return;
        }
        boolean l2Deleted = true;
        if (l2 != null) {
            try {
                l2Deleted = l2.evictAll(keys);
            } catch (Exception e) {
                warnL2Failure("evictAll", e);
                l2Deleted = false;
            }
        }
        for (String key : keys) {
            try {
                l1.evict(key);
                metrics.evict(1);
            } catch (Exception e) {
                log.warn("通用缓存失效执行异常，key={}", key, e);
            }
        }
        Set<String> wrapped = new LinkedHashSet<>(keys.size());
        for (String key : keys) {
            wrapped.add(GenericKeyCodec.wrap(key));
        }
        publisher.publishAll(wrapped);
        keys.forEach(k -> metrics.broadcastSent());
        if (!l2Deleted && allowRetry && doubleDeleteScheduler != null) {
            if (attempt < MAX_L2_EVICT_RETRIES) {
                doubleDeleteScheduler.schedule(() -> evictKeys(keys, attempt + 1, true));
            } else {
                metrics.evictRetryExhausted();
            }
        }
    }

    private CacheEntry l2Get(String key) {
        try {
            CacheEntry entry = l2.get(key);
            return entry.hit() ? decodedL2Entry(key, entry) : entry;
        } catch (Exception e) {
            warnL2Failure("get", e);
            return CacheEntry.miss();
        }
    }

    private CacheEntry decodedL2Entry(String key, CacheEntry entry) {
        String json = entry.json();
        if (json == null || !json.startsWith(GZ_PREFIX)) {
            return entry;
        }
        try {
            byte[] compressed = Base64.getDecoder().decode(json.substring(GZ_PREFIX.length()));
            try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
                return CacheEntry.of(new String(gz.readAllBytes(), StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            log.warn("通用缓存 L2 压缩值解压失败，按未命中处理: {}", key);
            return CacheEntry.miss();
        }
    }

    /**
     * 两级写入。
     *
     * @return 是否真正写入；超过 {@code l2.max-value-kb} 时返回 false（两级均跳过）
     */
    private boolean putBoth(String key, String json, Duration l2Ttl, Duration l1Ttl) {
        if (json.length() > maxValueChars) {
            metrics.l2ValueOversized();
            warnOversized(key, json.length());
            return false;
        }
        if (l2 != null) {
            try {
                l2.put(key, encodeForL2(json), l2Ttl);
            } catch (Exception e) {
                warnL2Failure("put", e);
            }
        }
        l1.put(key, json, l1Ttl);
        return true;
    }

    /** null 占位是否启用：null-ttl 非正表示关闭负缓存（与配置文档一致） */
    private boolean nullPlaceholderEnabled() {
        Duration nullTtl = props.getL2().getNullTtl();
        return nullTtl != null && !nullTtl.isZero() && !nullTtl.isNegative();
    }

    private String encodeForL2(String json) {
        if (!compressionEnabled || json.length() < compressionMinChars) {
            return json;
        }
        ByteArrayOutputStream buf = new ByteArrayOutputStream(json.length() / 2);
        try (GZIPOutputStream gz = new GZIPOutputStream(buf)) {
            gz.write(json.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.warn("通用缓存 L2 值压缩失败，按原文存储: {}", e.getMessage());
            return json;
        }
        metrics.l2ValueCompressed();
        return GZ_PREFIX + Base64.getEncoder().encodeToString(buf.toByteArray());
    }

    private Object decode(String key, String json, JavaType valueType) {
        if (JsonCodec.NULL_SENTINEL.equals(json)) {
            return NULL_VALUE;
        }
        Object value = JsonCodec.read(json, valueType);
        if (value == null) {
            log.warn("通用缓存值反序列化失败，按未命中继续（类型可能已变更，回源成功后自动覆盖）: {}", key);
            return DECODE_FAILED;
        }
        return value;
    }

    private Duration effectiveBase(GenericCacheMetadata meta, Duration override) {
        return override != null ? override
                : (meta.ttl() != null ? meta.ttl() : props.getL2().getTtl());
    }

    private Duration effectiveTtl(GenericCacheMetadata meta, Duration override) {
        Duration base = effectiveBase(meta, override);
        if (base == null || base.isZero() || base.isNegative()) {
            return base;
        }
        Duration jitter = props.getL2().getJitter();
        if (jitter == null || jitter.isZero() || jitter.isNegative()) {
            return base;
        }
        return base.plusMillis(ThreadLocalRandom.current().nextLong(jitter.toMillis() + 1));
    }

    private Duration l1TtlFor(Duration l2Base) {
        Duration l1Ttl = props.getL1().getTtl();
        if (l1Ttl == null || l1Ttl.isZero() || l1Ttl.isNegative()
                || l2Base == null || l2Base.isZero() || l2Base.isNegative()) {
            return l1Ttl;
        }
        return l1Ttl.compareTo(l2Base) <= 0 ? l1Ttl : l2Base;
    }

    private void statL1(GenericCacheMetadata meta, boolean hit) {
        CacheStatsCollector s = stats;
        if (s != null) {
            s.l1Lookup(STATS_PREFIX + meta.prefix(), hit);
        }
    }

    private void statL2(GenericCacheMetadata meta, boolean hit) {
        CacheStatsCollector s = stats;
        if (s != null) {
            s.l2Lookup(STATS_PREFIX + meta.prefix(), hit);
        }
    }

    private void statDbLoad(GenericCacheMetadata meta, int count) {
        CacheStatsCollector s = stats;
        if (s != null) {
            s.dbLoad(STATS_PREFIX + meta.prefix(), count);
        }
    }

    private void statNullPlaceholder(GenericCacheMetadata meta) {
        CacheStatsCollector s = stats;
        if (s != null) {
            s.nullPlaceholder(STATS_PREFIX + meta.prefix());
        }
    }

    private void statEvict(GenericCacheMetadata meta, int keys) {
        CacheStatsCollector s = stats;
        if (s != null) {
            s.evict(STATS_PREFIX + meta.prefix(), keys);
        }
    }

    private void warnL2Failure(String op, Exception e) {
        metrics.l2Fallback(op);
        long now = System.nanoTime();
        if (now - lastL2WarnAt > 30_000_000_000L) {
            lastL2WarnAt = now;
            log.warn("通用缓存 L2 通道 {} 失败，降级为不可用（本告警 30s 内不重复）: {}",
                    op, e.getMessage());
        }
    }

    private void warnKeyCustomizerFailure(CacheKeyCustomizer customizer, Exception e) {
        long now = System.nanoTime();
        if (now - lastKeyCustomizerWarnAt > 30_000_000_000L) {
            lastKeyCustomizerWarnAt = now;
            log.warn("CacheKeyCustomizer.segment() 抛出异常，通用缓存键组装按无自定义段处理"
                    + "（本告警 30s 内不重复）: {}", customizer.getClass().getName(), e);
        }
    }

    private void warnOversized(String key, int chars) {
        long now = System.nanoTime();
        if (now - lastOversizedWarnAt > 30_000_000_000L) {
            lastOversizedWarnAt = now;
            log.warn("通用缓存值 {} 字符超过 l2.max-value-kb 上限，两级均跳过写入（读每次回源）: {}",
                    chars, key);
        }
    }
}
