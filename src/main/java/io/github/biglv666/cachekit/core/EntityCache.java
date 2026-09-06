package io.github.biglv666.cachekit.core;

import java.util.function.Supplier;

/**
 * 实体缓存句柄：{@code @CacheHandle} 注入的手动控制入口，
 * 与注解拦截共享同一套三级链，行为完全一致。
 *
 * @param <T> 实体类型
 */
public interface EntityCache<T> {

    /**
     * 三级 read-through：L1 → L2 → dbLoader，命中即返回，未命中执行 loader 并回填。
     *
     * @param id       主键值
     * @param dbLoader 未命中时的 DB 加载逻辑（通常为 mapper 方法引用）
     * @return 实体或 null（未命中且 null 结果按配置缓存）
     */
    T get(Object id, Supplier<T> dbLoader);

    /** 失效指定主键的缓存（本地 L1 + Redis L2 + 广播 + 延迟双删） */
    void evict(Object id);
}
