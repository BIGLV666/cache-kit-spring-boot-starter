package io.github.biglv666.cachekit.handle;

import io.github.biglv666.cachekit.core.CacheStore;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * 关闭态/降级态的空操作 {@link CacheStore}：读直查 loader，写与失效空操作，
 * 避免 {@code cache-kit.enabled=false} 或通用缓存关闭时字段为 null 导致业务 NPE。
 */
public class NoopCacheStore<T> implements CacheStore<T> {

    @Override
    public T get(String key) {
        return null;
    }

    @Override
    public T get(String key, Supplier<T> loader) {
        return loader.get();
    }

    @Override
    public void put(String key, T value) {
        // no-op
    }

    @Override
    public void put(String key, T value, Duration ttl) {
        // no-op
    }

    @Override
    public void evict(String key) {
        // no-op
    }

    @Override
    public T refresh(String key, Supplier<T> loader) {
        return loader.get();
    }
}
