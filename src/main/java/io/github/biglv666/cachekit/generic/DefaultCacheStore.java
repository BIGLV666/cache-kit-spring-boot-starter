package io.github.biglv666.cachekit.generic;

import com.fasterxml.jackson.databind.JavaType;
import io.github.biglv666.cachekit.aspect.TransactionAwareEviction;
import io.github.biglv666.cachekit.core.CacheStore;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * {@link CacheStore} 默认实现：绑定一个 {@code @CacheObject} 值类型，
 * 委托 {@link TieredValueCache} 执行读/写/失效。
 *
 * <p>失效与实体缓存一致：活动事务内按 {@code cache-kit.tx.evict-after-commit}
 * 延迟到提交后（回滚不失效）；{@link #refresh(String, Supplier)} 是显式强制刷新，
 * 立即删除旧值后回源，不受事务延迟语义影响。</p>
 *
 * @param <T> 缓存值类型
 */
public class DefaultCacheStore<T> implements CacheStore<T> {

    private final GenericCacheMetadata metadata;
    private final JavaType valueType;
    private final TieredValueCache cache;
    private final boolean evictAfterCommit;

    public DefaultCacheStore(GenericCacheMetadata metadata, JavaType valueType,
                             TieredValueCache cache, boolean evictAfterCommit) {
        this.metadata = metadata;
        this.valueType = valueType;
        this.cache = cache;
        this.evictAfterCommit = evictAfterCommit;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(String key) {
        return (T) cache.peek(metadata, key, valueType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(String key, Supplier<T> loader) {
        return (T) cache.load(metadata, key, valueType, null, metadata.cacheNull(), loader::get);
    }

    @Override
    public void put(String key, T value) {
        cache.put(metadata, key, value, null);
    }

    @Override
    public void put(String key, T value, Duration ttl) {
        cache.put(metadata, key, value, ttl);
    }

    @Override
    public void evict(String key) {
        TransactionAwareEviction.evictIfConfigured(() -> cache.evict(metadata, key), evictAfterCommit);
    }

    @Override
    public T refresh(String key, Supplier<T> loader) {
        // 强制刷新语义：先立即删除并广播（不调度延迟双删——随后写入的新值不能被第二次删除），
        // 再直接执行 loader 覆盖缓存。刻意不走 load：即使 L2 删除失败/残留旧值，也绝不再读回旧值
        cache.evict(metadata, key, false);
        T value = loader.get();
        cache.put(metadata, key, value, null);
        return value;
    }

}
