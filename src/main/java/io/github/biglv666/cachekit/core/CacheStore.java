package io.github.biglv666.cachekit.core;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * 通用缓存手动句柄：{@code @CacheHandle(值类型.class)} 字段注入，
 * 用于业务自行决定何时读取、更新、删除任意缓存值。
 *
 * <p>句柄与 {@code @CacheLoad} 注解共享同一套 L1/L2、single-flight、null 占位、
 * 延迟双删与广播失效语义；区别只是 key 由调用方直接给出（不再经过 SpEL）。</p>
 *
 * <pre>{@code
 * @CacheHandle(AppConfig.class)
 * private CacheStore<AppConfig> configStore;
 *
 * AppConfig cfg = configStore.get("t1:db");
 * AppConfig fresh = configStore.get("t1:db", () -> remote.load("t1", "db"));
 * configStore.put("t1:db", updated);
 * configStore.evict("t1:db");
 * }</pre>
 *
 * @param <T> 缓存值类型
 */
public interface CacheStore<T> {

    /**
     * 只窥探缓存，不触发回源。
     *
     * @return 命中值；未命中或缓存的是 null 占位时返回 null
     */
    T get(String key);

    /**
     * 未命中时执行 loader 并回填；命中时不执行 loader。
     * null 结果是否缓存由 {@code @CacheObject.cacheNull} 决定。
     */
    T get(String key, Supplier<T> loader);

    /** 主动写入/更新，TTL 使用 {@code @CacheObject.ttl} 或全局配置；null 按 cacheNull 语义处理 */
    void put(String key, T value);

    /** 主动写入/更新并覆盖本次 TTL；TTL 非正表示跳过 L2 写入（仅 L1） */
    void put(String key, T value, Duration ttl);

    /**
     * 删除缓存：L2 + 本实例 L1 + 广播其他实例 L1 + 延迟双删。
     * 处于活动事务时按 {@code cache-kit.tx.evict-after-commit} 延迟到提交后执行。
     */
    void evict(String key);

    /**
     * 强制刷新：立即删除旧值（含广播）后重新执行 loader 并回填。
     * 与 {@link #get(String, Supplier)} 的区别是不信任当前缓存，适合“已知数据变更但未失效”的场景。
     */
    T refresh(String key, Supplier<T> loader);
}
