package io.github.biglv666.cachekit.core;

import java.util.List;
import java.util.function.Function;
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

    /**
     * 批量 read-through（single-flight 合并回源，MGET 管道查 L2）：语义同 {@link #get}。
     * 结果与入参 ids 一一对应，不存在的 ID 为 null（并按配置写 null 占位）。
     *
     * @param ids       主键值集合（自动去重回源，null 主键按不存在处理）
     * @param dbLoader 批量加载逻辑，只查传入的缺失 ID，返回存在的实体
     * @return 与 ids 一一对应的结果列表
     */
    List<T> getBatch(List<Object> ids, Function<List<Object>, List<T>> dbLoader);

    /**
     * 失效指定主键的缓存（本地 L1 + Redis L2 + 广播 + 延迟双删）。
     *
     * <p>事务感知：活动事务内延迟到 afterCommit 执行（{@code cache-kit.tx.evict-after-commit}
     * 默认开），事务回滚不失效。注意：同事务内先写后读同键会读到缓存里的提交前旧值，
     * 需要同事务立即可见时用 {@code CacheKit.withDb} 旁路读取。</p>
     */
    void evict(Object id);

    /** 批量失效：逐键走 {@link #evict}（整批一次广播合并由底层 evictBatch 承担） */
    void evictBatch(Iterable<Object> ids);
}
