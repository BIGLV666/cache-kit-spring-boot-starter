package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.exception.IdMisfireException;
import io.github.biglv666.cachekit.metadata.EntityMetadata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * 主键批量查询的三级缓存解析器：把"缓存一个集合"转换为"缓存每个 ID"。
 *
 * <p>算法：逐 ID 三态窥探（命中值 / 已缓存空 / 未命中）→ 未命中集合回调 dbBatchLoader
 * 只查缺失部分 → 命中的"已缓存空"按 IN 语义从结果中消失 → 按请求顺序组装。
 * null 占位复用单条查询机制（短 TTL，防穿透，过期后自动再探）。
 * 逐 ID single-flight 由 {@link TieredEntityCache#loadBatch} 保证：并发批量请求重叠的
 * 缺失 ID 只回源一次，单条/批量共用同一张 inflight 表。</p>
 *
 * <p>{@code strictIds} 严格模式（注解列表查询路径开启）：回源结果主键与请求值对不上时
 * 判定请求集合不是主键集合，抛 {@link IdMisfireException} 由调用方旁路，绝不缓存错键。</p>
 */
public final class BatchCacheResolver {

    private BatchCacheResolver() {
    }

    /**
     * @param meta          实体元数据
     * @param requestedIds  请求的主键集合（按此顺序输出，去重）
     * @param cacheNull     是否缓存"已确认不存在"的占位
     * @param ttlOverride   写回 TTL 覆盖，null 用全局/实体级
     * @param dbBatchLoader 只查询缺失 ID 的加载逻辑，返回存在的实体列表
     * @return 实体列表（按请求顺序，跳过不存在与 null）
     */
    public static List<Object> resolve(TieredEntityCache cache,
                                       EntityMetadata meta,
                                       Collection<?> requestedIds,
                                       boolean cacheNull,
                                       Duration ttlOverride,
                                       Function<List<Object>, List<Object>> dbBatchLoader) {
        return resolve(cache, meta, requestedIds, cacheNull, ttlOverride, dbBatchLoader, false);
    }

    /**
     * @param strictIds 严格模式守卫：true 时回源结果主键与请求值对不上会抛
     *                  {@link IdMisfireException}（注解列表查询开启；MP selectBatchIds 路径传 false）
     */
    public static List<Object> resolve(TieredEntityCache cache,
                                       EntityMetadata meta,
                                       Collection<?> requestedIds,
                                       boolean cacheNull,
                                       Duration ttlOverride,
                                       Function<List<Object>, List<Object>> dbBatchLoader,
                                       boolean strictIds) {
        if (BypassContext.isActive()) {
            return dbBatchLoader.apply(new ArrayList<>(new LinkedHashSet<>(requestedIds)));
        }
        // 归一化 map 键（Long 1 与 "1" 指向同一行；复合主键 Collection 归一为 join 段），
        // LinkedHashSet 保持请求顺序并去重
        Set<String> seen = new LinkedHashSet<>();
        Map<String, Object> requestedByKey = new LinkedHashMap<>();
        for (Object id : requestedIds) {
            if (id == null) {
                continue;
            }
            String k = TieredEntityCache.idSegment(id);
            if (k == null) {
                // 复合主键含 null 段：按无主键处理，不进缓存链
                continue;
            }
            if (seen.add(k)) {
                requestedByKey.put(k, id);
            }
        }

        Map<String, Object> resolved = new LinkedHashMap<>();
        List<Object> missing = new ArrayList<>();
        List<Object> orderedIds = new ArrayList<>(requestedByKey.values());
        List<TieredEntityCache.CachePeek> peeks = cache.peekBatch(meta, orderedIds);
        for (int i = 0; i < orderedIds.size(); i++) {
            String k = TieredEntityCache.idSegment(orderedIds.get(i));
            switch (peeks.get(i).state()) {
                case HIT -> resolved.put(k, peeks.get(i).value());
                case HIT_NULL -> {
                    // 已知不存在：IN 语义下从结果消失
                }
                case MISS -> missing.add(orderedIds.get(i));
            }
        }

        if (!missing.isEmpty()) {
            // 逐 ID single-flight 归批回源：并发批量请求重叠的缺失 ID 只回源一次
            Object[] loaded = cache.loadBatch(meta, missing, cacheNull, ttlOverride, dbBatchLoader, strictIds);
            for (int i = 0; i < missing.size(); i++) {
                Object entity = loaded[i];
                if (entity != null) {
                    resolved.put(TieredEntityCache.idSegment(missing.get(i)), entity);
                }
                // 不存在的 ID 已由 loadBatch 写入 null 占位（cacheNull 时）
            }
        }

        List<Object> out = new ArrayList<>(resolved.size());
        for (String k : seen) {
            Object v = resolved.get(k);
            if (v != null) {
                out.add(v);
            }
        }
        return out;
    }
}
