package io.github.biglv666.cachekit.handle;

import io.github.biglv666.cachekit.core.EntityCache;

import java.util.function.Supplier;

/**
 * 空操作句柄：cache-kit.enabled=false 时注入 {@code @CacheHandle} 字段，
 * 读直查 DB、失效为空操作——避免字段留 null 在业务首次调用时抛 NPE。
 *
 * @param <T> 实体类型
 */
final class NoopEntityCache<T> implements EntityCache<T> {

    @Override
    public T get(Object id, Supplier<T> dbLoader) {
        return dbLoader.get();
    }

    @Override
    public void evict(Object id) {
        // 组件已关闭：空操作
    }
}
