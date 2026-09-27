package io.github.biglv666.cachekit.handle;

import io.github.biglv666.cachekit.aspect.CacheInvalidateAspect;
import io.github.biglv666.cachekit.core.EntityCache;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * {@code EntityCache} 默认实现：绑定实体类型，委托三级链执行。
 *
 * <p>失效与注解路径共用同一事务感知钩子：活动事务内 evict 延迟到 afterCommit（回滚不失效），
 * 由 {@code cache-kit.tx.evict-after-commit} 控制。注意语义：事务内先写后读同键会读到
 * 缓存中的提交前旧值，需要同事务立即可见时用 {@code CacheKit.withDb} 旁路读取。</p>
 *
 * @param <T> 实体类型
 */
public class DelegatingEntityCache<T> implements EntityCache<T> {

    private final Class<?> entityType;
    private final EntityMetadataRegistry registry;
    private final TieredEntityCache tieredCache;
    /** 事务感知失效委托，null 时（测试直构/降级场景）立即失效 */
    private final CacheInvalidateAspect invalidationDelegate;

    public DelegatingEntityCache(Class<?> entityType, EntityMetadataRegistry registry, TieredEntityCache tieredCache) {
        this(entityType, registry, tieredCache, null);
    }

    public DelegatingEntityCache(Class<?> entityType, EntityMetadataRegistry registry, TieredEntityCache tieredCache,
                                 CacheInvalidateAspect invalidationDelegate) {
        this.entityType = entityType;
        this.registry = registry;
        this.tieredCache = tieredCache;
        this.invalidationDelegate = invalidationDelegate;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(Object id, Supplier<T> dbLoader) {
        EntityMetadata meta = registry.require(entityType);
        return (T) tieredCache.load(meta, id, null, true, dbLoader::get);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<T> getBatch(List<Object> ids, Function<List<Object>, List<T>> dbLoader) {
        EntityMetadata meta = registry.require(entityType);
        // loadBatch 假定调用方已用 peek 拆分命中/未命中（注解路径同约定）：先窥探再只回源缺失部分，
        // 否则已在缓存中的键会被"回源结果里没有它"覆盖成 null 占位
        List<TieredEntityCache.CachePeek> peeks = tieredCache.peekBatch(meta, ids);
        List<Object> missing = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            if (peeks.get(i).state() == TieredEntityCache.CachePeek.State.MISS) {
                missing.add(ids.get(i));
            }
        }
        Map<Object, Object> loaded = new HashMap<>();
        if (!missing.isEmpty()) {
            Object[] out = tieredCache.loadBatch(meta, missing, true, null,
                    m -> (List<Object>) (List<?>) dbLoader.apply(m));
            for (int i = 0; i < missing.size(); i++) {
                loaded.put(missing.get(i), out[i]);
            }
        }
        List<T> result = new ArrayList<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            TieredEntityCache.CachePeek p = peeks.get(i);
            result.add(p.state() == TieredEntityCache.CachePeek.State.MISS
                    ? (T) loaded.get(ids.get(i)) : (T) p.value());
        }
        return result;
    }

    @Override
    public void evict(Object id) {
        EntityMetadata meta = registry.require(entityType);
        if (invalidationDelegate != null) {
            invalidationDelegate.evictSmart(meta, id);
        } else {
            tieredCache.evict(meta, id);
        }
    }

    @Override
    public void evictBatch(Iterable<Object> ids) {
        EntityMetadata meta = registry.require(entityType);
        if (invalidationDelegate != null) {
            invalidationDelegate.evictSmartBatch(meta, ids);
        } else {
            tieredCache.evictBatch(meta, ids);
        }
    }
}
