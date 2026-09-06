package io.github.biglv666.cachekit.handle;

import io.github.biglv666.cachekit.core.EntityCache;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;

import java.util.function.Supplier;

/**
 * {@code EntityCache} 默认实现：绑定实体类型，委托三级链执行。
 *
 * @param <T> 实体类型
 */
public class DelegatingEntityCache<T> implements EntityCache<T> {

    private final Class<?> entityType;
    private final EntityMetadataRegistry registry;
    private final TieredEntityCache tieredCache;

    public DelegatingEntityCache(Class<?> entityType, EntityMetadataRegistry registry, TieredEntityCache tieredCache) {
        this.entityType = entityType;
        this.registry = registry;
        this.tieredCache = tieredCache;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(Object id, Supplier<T> dbLoader) {
        EntityMetadata meta = registry.require(entityType);
        return (T) tieredCache.load(meta, id, null, true, dbLoader::get);
    }

    @Override
    public void evict(Object id) {
        tieredCache.evict(registry.require(entityType), id);
    }
}
