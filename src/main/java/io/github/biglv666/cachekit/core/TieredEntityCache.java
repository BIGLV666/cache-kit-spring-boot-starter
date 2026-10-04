package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CacheChannel;
import io.github.biglv666.cachekit.channel.CacheEntry;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.exception.IdMisfireException;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.support.JsonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 三级链核心：L1（Caffeine）→ L2（Redis，可选）→ loader（查 DB）read-through，
 * 命中逐级回填；写路径"先写 DB 后删缓存"，删除含广播与延迟双删。
 *
 * <p>并发控制：同 key 回源经 single-flight 合并，防击穿；null 结果按配置短 TTL 缓存防穿透；
 * TTL 追加随机抖动防雪崩。一致性语义为最终一致：常态秒级（双删窗口内），最坏受 L2 TTL
 * 上界约束——回填竞态可能把旧值写回 L2（详见 README 一致性章节）。</p>
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
    private final String namespace;
    private final List<CacheKeyCustomizer> keyCustomizers;

    /** single-flight：同 key 的并发回源合并为一个执行（单条与批量共用） */
    private final ConcurrentHashMap<String, CompletableFuture<Object>> inflight = new ConcurrentHashMap<>();

    private volatile CacheMetricsListener metrics = new CacheMetricsListener() {
    };

    /** 实体维度统计收集器（端点数据源，可选）：与全局 metrics 并行记录 */
    private volatile CacheStatsCollector stats;

    /** 挂载实体维度统计收集器（/actuator/cachekit 数据源），未挂载时跳过统计 */
    public void setStatsCollector(CacheStatsCollector stats) {
        this.stats = stats;
    }

    /** 当前 single-flight 回源中的键数（观测用） */
    public int inflightCount() {
        return inflight.size();
    }

    private void statL1(EntityMetadata meta, boolean hit) {
        CacheStatsCollector s = stats;
        if (s != null) {
            s.l1Lookup(meta.prefix(), hit);
        }
    }

    private void statL2(EntityMetadata meta, boolean hit) {
        CacheStatsCollector s = stats;
        if (s != null) {
            s.l2Lookup(meta.prefix(), hit);
        }
    }

    /** 挂载指标监听器（Micrometer 集成或自定义观测），未挂载时为空实现 */
    public void setMetricsListener(CacheMetricsListener metrics) {
        this.metrics = metrics == null ? new CacheMetricsListener() {
        } : metrics;
    }

    public TieredEntityCache(CacheKitProperties props,
                             CacheChannel l1,
                             CacheChannel l2,
                             InvalidationPublisher publisher,
                             DoubleDeleteScheduler doubleDeleteScheduler) {
        this(props, l1, l2, publisher, doubleDeleteScheduler, "", List.of());
    }

    public TieredEntityCache(CacheKitProperties props,
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
    }

    /**
     * 缓存键组装：自定义段（多租户/多数据源，CacheKeyCustomizer）+ 命名空间（cache-kit.key-namespace）
     * + 实体前缀 + 主键。迁移主键类型/表名时改 namespace 即可整体弃用旧键。
     */
    private String key(EntityMetadata meta, Object id) {
        StringBuilder sb = new StringBuilder();
        for (CacheKeyCustomizer customizer : keyCustomizers) {
            String segment;
            try {
                segment = customizer.segment();
            } catch (Exception e) {
                // SPI 异常按"无段"降级并限频告警：绝不打断读/写/失效主流程
                //（批量注册路径若被 SPI 异常打断，inflight 会残留未完成 future 挂死后续读）
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
        return sb.append(meta.prefix()).append(':').append(id).toString();
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
        if (id == null) {
            // null 主键会产生 "前缀:null" 键并与字面 "null" 主键冲突，直接拒绝
            throw new CacheKitException("缓存读取的主键不能为 null: " + meta.entityType().getName());
        }
        String key = key(meta, id);

        CacheEntry c1 = l1.get(key);
        metrics.l1Lookup(c1.hit());
        statL1(meta, c1.hit());
        Object v = c1.hit() ? decode(key, c1.json(), meta) : DECODE_FAILED;
        if (v != DECODE_FAILED) {
            return v == NULL_VALUE ? null : v;
        }

        if (l2 != null) {
            CacheEntry c2 = l2Get(meta, key);
            metrics.l2Lookup(c2.hit());
            if (c2.hit()) {
                v = decode(key, c2.json(), meta);
                if (v != DECODE_FAILED) {
                    if (v != NULL_VALUE) {
                        // L2 命中回填 L1，TTL 取 l1.ttl 与该实体生效 L2 基准的较小值，
                        // 保证"L1 是更短的脏读上界"不因实体级/方法级 TTL 覆盖而倒装
                        l1.put(key, c2.json(), l1TtlFor(effectiveBase(meta, null)));
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
                    // 等待者拿到的是可物化载荷（JSON 串/null 占位）而非实体实例：
                    // 各自 decode 出新对象，避免 single-flight 等待者共享同一可变实例被并发污染
                    Object payload;
                    if (db == null) {
                        if (cacheNull) {
                            // null 占位：防穿透，两级都用短 TTL
                            Duration nullTtl = props.getL2().getNullTtl();
                            putBoth(key, JsonCodec.NULL_SENTINEL, nullTtl, l1TtlFor(nullTtl));
                            metrics.nullPlaceholder();
                            CacheStatsCollector ns = stats;
                            if (ns != null) {
                                ns.nullPlaceholder(meta.prefix());
                            }
                        }
                        payload = cacheNull ? JsonCodec.NULL_SENTINEL : null;
                    } else {
                        // 序列化失败不丢业务数据：返回结果只是不缓存（实体含不支持的类型时）
                        String json = null;
                        try {
                            json = JsonCodec.write(db);
                        } catch (Exception e) {
                            log.warn("缓存值序列化失败，本次结果不缓存: {}", meta.entityType().getName(), e);
                        }
                        if (json != null) {
                            putBoth(key, json, effectiveTtl(meta, ttlOverride),
                                    l1TtlFor(effectiveBase(meta, ttlOverride)));
                            payload = json;
                        } else {
                            // 不可序列化的罕见兜底：等待者只能共享该实例（本就不会进缓存）
                            payload = db;
                        }
                    }
                    metrics.dbLoad(1);
                    CacheStatsCollector s = stats;
                    if (s != null) {
                        s.dbLoad(meta.prefix(), 1);
                    }
                    created.complete(payload);
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
            return materialize(meta, key, inflightFuture.join());
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw cause instanceof RuntimeException re ? re : new CacheKitException(cause);
        }
    }

    /** single-flight 载荷 → 实体：JSON 串 decode 出新实例，null/null 占位 → null */
    private Object materialize(EntityMetadata meta, String key, Object payload) {
        if (payload == null) {
            return null;
        }
        if (payload instanceof String json) {
            Object v = decode(key, json, meta);
            return v == DECODE_FAILED ? null : v == NULL_VALUE ? null : v;
        }
        return payload;
    }

    /**
     * 失效：立即执行一次完整删除，并按配置调度延迟双删。
     */
    public void evict(EntityMetadata meta, Object id) {
        evictBatch(meta, List.of(id));
    }

    /**
     * 批量 read-through：逐 ID single-flight（与单条 load 共用同一张 inflight 表），
     * 本线程"赢得"的缺失键自动归成一批、一次 {@code dbBatchLoader}（IN 语句）回源；
     * 并发批量请求中重叠的缺失 ID 直接 join 已有 future，不会重复回源。
     *
     * <p>保证：每个缺失 ID 至多回源一次（无论多少并发请求、单条还是批量到达）。</p>
     *
     * @param ids          请求的主键集合（调用方已做过三态窥探，全部应为未命中）
     * @param cacheNull    不存在的 ID 是否写 null 占位
     * @param dbBatchLoader 批量加载逻辑，只查传入的缺失 ID，返回存在的实体
     * @return 与入参 ids 一一对应的结果数组（不存在为 null）
     */
    public Object[] loadBatch(EntityMetadata meta, List<Object> ids, boolean cacheNull,
                              Duration ttlOverride, Function<List<Object>, List<Object>> dbBatchLoader) {
        return loadBatch(meta, ids, cacheNull, ttlOverride, dbBatchLoader, false);
    }

    /**
     * 带严格模式守卫的批量 read-through：{@code strictIds} 为 true 时（注解列表查询路径），
     * 回源结果的主键必须全部能对上请求值，且结果非空——对不上或为空说明请求集合不是主键集合
     * （如把手机号列表当 ID 拆解），向 strict 调用方抛出携带回源原始结果的 {@link IdMisfireException}，
     * 由调用方按条件查询旁路（警告 + 全参重查 + 不缓存），绝不返回空数据或写错键占位。
     * 空结果同样旁路：无法证明请求值是主键，给它写 null 占位的话，占位键永远不会被
     * 该行后续 insert 的失效命中（insert 失效走真实主键）。
     * MP {@code selectBatchIds} 路径请求值即主键，无需开启。
     *
     * <p>误判时 shared future 仍按真实数据正常完成（行命中照常缓存、缺失仅完成 null），
     * join 这些 future 的非 strict 等待者不受本次误判影响；误判异常只抛给当前调用方。</p>
     */
    public Object[] loadBatch(EntityMetadata meta, List<Object> ids, boolean cacheNull,
                              Duration ttlOverride, Function<List<Object>, List<Object>> dbBatchLoader,
                              boolean strictIds) {
        int n = ids.size();
        @SuppressWarnings("unchecked")
        CompletableFuture<Object>[] slots = new CompletableFuture[n];
        String[] keys = new String[n];
        List<Integer> ownedIdx = new ArrayList<>();
        List<Object> ownedIds = new ArrayList<>();
        try {
            for (int i = 0; i < n; i++) {
                Object id = ids.get(i);
                if (id == null) {
                    // null 主键不进缓存链：槽直接按"不存在"完成（对齐 evictBatch 的判空）
                    slots[i] = CompletableFuture.completedFuture(null);
                    continue;
                }
                keys[i] = key(meta, id);
                CompletableFuture<Object> created = new CompletableFuture<>();
                CompletableFuture<Object> prev = inflight.putIfAbsent(keys[i], created);
                if (prev == null) {
                    slots[i] = created;
                    ownedIdx.add(i);
                    ownedIds.add(id);
                } else {
                    slots[i] = prev;
                }
            }
        } catch (Throwable t) {
            // 注册中途失败：已注册的 owned future 必须完成并移除，
            // 否则残留的未完成 future 会让这些键的后续读 join 永久挂起
            for (Integer idx : ownedIdx) {
                slots[idx].completeExceptionally(t);
                inflight.remove(keys[idx], slots[idx]);
            }
            throw t instanceof RuntimeException re ? re : new CacheKitException(t);
        }

        boolean misfired = false;
        List<Object> misfireFresh = List.of();
        if (!ownedIds.isEmpty()) {
            try {
                List<Object> fresh = dbBatchLoader.apply(ownedIds);
                metrics.dbLoad(ownedIds.size());
                CacheStatsCollector s = stats;
                if (s != null) {
                    s.dbLoad(meta.prefix(), ownedIds.size());
                }
                Map<String, Object> freshByKey = new LinkedHashMap<>();
                for (Object entity : fresh) {
                    Object idValue = meta.idOf(entity);
                    if (idValue != null) {
                        freshByKey.put(String.valueOf(idValue), entity);
                    }
                }
                if (strictIds) {
                    // 结果为空同样判误用：空结果无法证明请求值是主键集合，
                    // "全部查不到的条件值"若写占位，占位键不会被对应行的 insert 失效命中
                    Set<String> requestedKeys = new LinkedHashSet<>();
                    for (Object id : ownedIds) {
                        requestedKeys.add(String.valueOf(id));
                    }
                    boolean allMatched = !fresh.isEmpty();
                    for (Object entity : fresh) {
                        Object idValue = meta.idOf(entity);
                        if (idValue == null || !requestedKeys.contains(String.valueOf(idValue))) {
                            allMatched = false;
                            break;
                        }
                    }
                    if (!allMatched) {
                        // 误判：shared future 不能异常完成——同一 future 可能被非 strict 路径
                        //（MP selectBatchIds）或更早到达的 strict 等待者 join，异常完成会把
                        // 本次误判泄漏给无关调用方。改为逐槽按真实数据正常完成
                        //（行命中照常缓存——键即真实主键，安全；缺失仅完成 null、不写占位），
                        // 误判异常只抛给当前 strict 调用方，由其旁路全参重查
                        misfired = true;
                        misfireFresh = fresh;
                    }
                }
                for (int i = 0; i < ownedIds.size(); i++) {
                    Object id = ownedIds.get(i);
                    Object entity = freshByKey.get(String.valueOf(id));
                    Object payload;
                    if (entity != null) {
                        String json = null;
                        try {
                            json = JsonCodec.write(entity);
                        } catch (Exception e) {
                            log.warn("缓存值序列化失败，本次结果不缓存: {}", meta.entityType().getName(), e);
                        }
                        if (json != null) {
                            putBoth(keys[ownedIdx.get(i)], json, effectiveTtl(meta, ttlOverride),
                                    l1TtlFor(effectiveBase(meta, ttlOverride)));
                            payload = json;
                        } else {
                            payload = entity;
                        }
                    } else {
                        // 误判时缺失 ID 不写 null 占位：请求值尚未被证明是主键，
                        // 占位键不会被对应行后续 insert 的失效命中（strict 语义不变）
                        if (cacheNull && !misfired) {
                            Duration nullTtl = props.getL2().getNullTtl();
                            putBoth(keys[ownedIdx.get(i)], JsonCodec.NULL_SENTINEL, nullTtl, l1TtlFor(nullTtl));
                            metrics.nullPlaceholder();
                            CacheStatsCollector ns = stats;
                            if (ns != null) {
                                ns.nullPlaceholder(meta.prefix());
                            }
                        }
                        payload = cacheNull && !misfired ? JsonCodec.NULL_SENTINEL : null;
                    }
                    slots[ownedIdx.get(i)].complete(payload);
                }
            } catch (Throwable t) {
                for (Integer idx : ownedIdx) {
                    slots[idx].completeExceptionally(t);
                }
            } finally {
                // 必须清理：残留的已完成 future 会让后续读永远 join 到旧值（失效失效）
                for (int i = 0; i < ownedIds.size(); i++) {
                    inflight.remove(keys[ownedIdx.get(i)], slots[ownedIdx.get(i)]);
                }
            }
        }
        if (misfired) {
            throw new IdMisfireException(meta.entityType(), misfireFresh);
        }

        Object[] out = new Object[n];
        for (int i = 0; i < n; i++) {
            try {
                out[i] = materialize(meta, keys[i], slots[i].join());
            } catch (CompletionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                throw cause instanceof RuntimeException re ? re : new CacheKitException(cause);
            }
        }
        return out;
    }

    /**
     * 批量失效：逐键立即删除 + 广播；延迟双删整个批次只调度一次任务（合并，防行事件风暴）。
     * 键先去重：binlog UPDATE 的 before/after 双镜像主键相同、调用方传重复 ID 时，DEL/广播命令数减半。
     */
    public void evictBatch(EntityMetadata meta, Iterable<Object> ids) {
        Set<String> keys = new LinkedHashSet<>();
        for (Object id : ids) {
            if (id != null) {
                keys.add(key(meta, id));
            }
        }
        evictKeys(keys, 0);
        if (doubleDeleteScheduler != null && !keys.isEmpty()) {
            doubleDeleteScheduler.schedule(() -> evictKeys(keys, 0));
        }
        CacheStatsCollector s = stats;
        if (s != null && !keys.isEmpty()) {
            s.evict(meta.prefix(), keys.size());
        }
    }

    /**
     * 失效调用方（binlog 键段还原路径）预先拼好的完整键：键已含自定义段与命名空间，
     * 不经过 {@link #key} 组装（binlog 解析线程没有应用上下文，{@code segment()} 拿不到段）。
     * 语义与 {@link #evictBatch} 一致：逐键立即删除 + 广播 + 整批一次延迟双删，键先去重。
     */
    public void evictExactKeys(Iterable<String> keys) {
        if (keys == null) {
            return;
        }
        Set<String> deduped = new LinkedHashSet<>();
        keys.forEach(deduped::add);
        if (deduped.isEmpty()) {
            return;
        }
        evictKeys(deduped, 0);
        if (doubleDeleteScheduler != null) {
            doubleDeleteScheduler.schedule(() -> evictKeys(deduped, 0));
        }
    }

    /** L2 删除失败的重试上限：复用双删调度器的延迟重试，超过后由 L2 TTL 上界兜底 */
    private static final int MAX_L2_EVICT_RETRIES = 3;

    /**
     * 一批键的完整删除：L2 批量 DEL（通道支持时单命令/管道）→ 逐键 L1 → 广播。
     * L2 删除失败（Redis 闪断）时旧值可能滞留 L2：按双删延迟调度重试（P2 兜底——
     * 否则 Redis 恢复后读会命中删除前写入的旧值且无广播），超过次数上限由 L2 TTL 兜底。
     */
    private void evictKeys(Set<String> keys, int attempt) {
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
                log.warn("缓存失效执行异常，key={}", key, e);
            }
        }
        publisher.publishAll(keys);
        keys.forEach(k -> metrics.broadcastSent());
        if (!l2Deleted && doubleDeleteScheduler != null) {
            if (attempt < MAX_L2_EVICT_RETRIES) {
                doubleDeleteScheduler.schedule(() -> evictKeys(keys, attempt + 1));
            } else {
                // 重试耗尽仍失败：旧值滞留 L2，失效丢失（由 L2 TTL 上界兜底），计入指标供告警
                metrics.evictRetryExhausted();
            }
        }
    }

    /**
     * 不触发回源的缓存窥探：供批量查询做"命中/已缓存空/未命中"三态拆分。
     */
    public CachePeek peek(EntityMetadata meta, Object id) {
        if (id == null) {
            return new CachePeek(CachePeek.State.MISS, null);
        }
        String key = key(meta, id);
        CacheEntry c1 = l1.get(key);
        metrics.l1Lookup(c1.hit());
        statL1(meta, c1.hit());
        if (c1.hit()) {
            return toPeek(decode(key, c1.json(), meta));
        }
        if (l2 != null) {
            CacheEntry c2 = l2Get(meta, key);
            metrics.l2Lookup(c2.hit());
            if (c2.hit()) {
                Object v = decode(key, c2.json(), meta);
                if (v != DECODE_FAILED) {
                    if (v != NULL_VALUE) {
                        l1.put(key, c2.json(), l1TtlFor(effectiveBase(meta, null)));
                    }
                    return toPeek(v);
                }
            }
        }
        return new CachePeek(CachePeek.State.MISS, null);
    }

    /** L2 全部调用经此降级：通道抛异常（Redis 宕机等）按未命中处理，限频告警，绝不阻断业务读写 */
    private CacheEntry l2Get(EntityMetadata meta, String key) {
        try {
            CacheEntry entry = l2.get(key);
            CacheStatsCollector s = stats;
            if (s != null) {
                s.l2Lookup(meta.prefix(), entry.hit());
            }
            return entry;
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

    private volatile long lastL2WarnAt;

    private void warnL2Failure(String op, Exception e) {
        metrics.l2Fallback(op);
        long now = System.nanoTime();
        if (now - lastL2WarnAt > 30_000_000_000L) {
            lastL2WarnAt = now;
            log.warn("L2 通道 {} 失败，降级为不可用（本告警 30s 内不重复）: {}", op, e.getMessage());
        }
    }

    private volatile long lastKeyCustomizerWarnAt;

    private void warnKeyCustomizerFailure(CacheKeyCustomizer customizer, Exception e) {
        long now = System.nanoTime();
        if (now - lastKeyCustomizerWarnAt > 30_000_000_000L) {
            lastKeyCustomizerWarnAt = now;
            log.warn("CacheKeyCustomizer.segment() 抛出异常，本次键组装按无自定义段处理（本告警 30s 内不重复）: {}",
                    customizer.getClass().getName(), e);
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

    /**
     * 批量三态窥探：L1 逐键（本地内存），L2 缺失部分走 {@link CacheChannel#multiGet}（MGET 管道）。
     * 结果顺序与入参 ids 一一对应。
     */
    public List<CachePeek> peekBatch(EntityMetadata meta, List<Object> ids) {
        List<CachePeek> out = new ArrayList<>(ids.size());
        List<String> missKeys = new ArrayList<>();
        List<Integer> missIdx = new ArrayList<>();
        for (Object id : ids) {
            if (id == null) {
                // null 主键：不触碰缓存键（防 "前缀:null" 键），按未命中占位保持顺序对齐
                out.add(new CachePeek(CachePeek.State.MISS, null));
                continue;
            }
            String key = key(meta, id);
            CacheEntry c1 = l1.get(key);
            metrics.l1Lookup(c1.hit());
            statL1(meta, c1.hit());
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
                // 通道不支持批量：逐键 get（同样降级语义；计数由下方主循环统一做，避免重复）
                l2res = new LinkedHashMap<>();
                for (String key : missKeys) {
                    l2res.put(key, l2Get(meta, key));
                }
            }
            for (int i = 0; i < missIdx.size(); i++) {
                String key = missKeys.get(i);
                CacheEntry entry = l2res.get(key);
                metrics.l2Lookup(entry != null && entry.hit());
                statL2(meta, entry != null && entry.hit());
                if (entry == null || !entry.hit()) {
                    continue;
                }
                CachePeek p = toPeek(decode(key, entry.json(), meta));
                if (p.state() != CachePeek.State.MISS) {
                    if (p.state() == CachePeek.State.HIT) {
                        l1.put(key, entry.json(), l1TtlFor(effectiveBase(meta, null)));
                    }
                    out.set(missIdx.get(i), p);
                }
            }
        }
        return out;
    }

    /** L2 TTL 基准（抖动前）：方法级覆盖 &gt; 实体级 &gt; 全局 */
    private Duration effectiveBase(EntityMetadata meta, Duration override) {
        return override != null ? override
                : (meta.ttl() != null ? meta.ttl() : props.getL2().getTtl());
    }

    /**
     * L1 TTL：全局 l1.ttl 与"该键生效的 L2 基准 TTL"取较小值。实体级/方法级 TTL 覆盖只作用于
     * L2，若覆盖值小于 l1.ttl 会出现"L2 已过期、L1 仍供旧值"的倒装——L1 必须始终是更短的脏读上界。
     * 任一侧为非正值时保持 l1 原值（非正 TTL 的统一语义是"该级跳过写入/禁用"，见 CacheKitProperties）。
     */
    private Duration l1TtlFor(Duration l2Base) {
        Duration l1 = props.getL1().getTtl();
        if (l1 == null || l1.isZero() || l1.isNegative()
                || l2Base == null || l2Base.isZero() || l2Base.isNegative()) {
            return l1;
        }
        return l1.compareTo(l2Base) <= 0 ? l1 : l2Base;
    }

    private void putBoth(String key, String json, Duration l2Ttl, Duration l1Ttl) {
        if (l2 != null) {
            l2Put(key, json, l2Ttl);
        }
        l1.put(key, json, l1Ttl);
    }

    /** 实际写入 L2 的 TTL：基准 + 随机抖动（防雪崩）；基准非正（禁用 L2 写入）时不抖动，保持语义为"跳过写入" */
    private Duration effectiveTtl(EntityMetadata meta, Duration override) {
        Duration base = effectiveBase(meta, override);
        if (base == null || base.isZero() || base.isNegative()) {
            return base;
        }
        Duration jitter = props.getL2().getJitter();
        if (jitter == null || jitter.isZero() || jitter.isNegative()) {
            return base;
        }
        long jitterMillis = jitter.toMillis();
        return base.plusMillis(ThreadLocalRandom.current().nextLong(jitterMillis + 1));
    }

    /**
     * 解码 JSON：null 占位 → {@link #NULL_VALUE}；解析失败（实体结构漂移）→ 返回
     * {@link #DECODE_FAILED}，调用方按未命中继续走下一级。
     *
     * <p>失败时不主动失效该键：随后的回源成功会 putBoth 覆盖坏值；回源失败则坏键
     * 残留但每次读同样按未命中处理，行为与"DB 挂"一致。逐次 DEL+广播只会放大命令量，
     * 并发读同一坏键时会形成失效风暴（旧实现的问题）。</p>
     */
    private Object decode(String key, String json, EntityMetadata meta) {
        if (JsonCodec.NULL_SENTINEL.equals(json)) {
            return NULL_VALUE;
        }
        Object value = JsonCodec.read(json, meta.entityType());
        if (value == null) {
            log.warn("缓存值反序列化失败，按未命中继续（实体结构可能已变更，回源成功后自动覆盖）: {}", key);
            return DECODE_FAILED;
        }
        return value;
    }
}
