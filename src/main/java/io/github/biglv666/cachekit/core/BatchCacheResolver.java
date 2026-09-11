package io.github.biglv666.cachekit.core;

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
 * null 占位复用单条查询机制（短 TTL，防穿透，过期后自动再探）。</p>
 *
 * <p>已知限制：两个并发批量请求对相同缺失 ID 会各自回源一次（批内无法逐键 single-flight，
 * 回源本身是一条 IN 语句，风暴有界）。</p>
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
        if (BypassContext.isActive()) {
            return dbBatchLoader.apply(new ArrayList<>(new LinkedHashSet<>(requestedIds)));
        }
        // 归一化 map 键（Long 1 与 "1" 指向同一行），LinkedHashSet 保持请求顺序并去重
        Set<String> seen = new LinkedHashSet<>();
        Map<String, Object> requestedByKey = new LinkedHashMap<>();
        for (Object id : requestedIds) {
            if (id == null) {
                continue;
            }
            String k = String.valueOf(id);
            if (seen.add(k)) {
                requestedByKey.put(k, id);
            }
        }

        Map<String, Object> resolved = new LinkedHashMap<>();
        List<Object> missing = new ArrayList<>();
        List<Object> orderedIds = new ArrayList<>(requestedByKey.values());
        List<TieredEntityCache.CachePeek> peeks = cache.peekBatch(meta, orderedIds);
        for (int i = 0; i < orderedIds.size(); i++) {
            String k = String.valueOf(orderedIds.get(i));
            switch (peeks.get(i).state()) {
                case HIT -> resolved.put(k, peeks.get(i).value());
                case HIT_NULL -> {
                    // 已知不存在：IN 语义下从结果消失
                }
                case MISS -> missing.add(orderedIds.get(i));
            }
        }

        if (!missing.isEmpty()) {
            List<Object> fresh = dbBatchLoader.apply(missing);
            Map<String, Object> freshByKey = new LinkedHashMap<>();
            for (Object entity : fresh) {
                Object idValue = meta.idOf(entity);
                if (idValue == null) {
                    continue;
                }
                freshByKey.put(String.valueOf(idValue), entity);
                cache.cachePut(meta, idValue, entity, ttlOverride);
            }
            for (Object id : missing) {
                String k = String.valueOf(id);
                Object entity = freshByKey.get(k);
                if (entity != null) {
                    resolved.put(k, entity);
                } else if (cacheNull) {
                    cache.cacheNull(meta, id);
                }
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
