package io.github.biglv666.cachekit.channel;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 缓存通道抽象：L1（Caffeine）与 L2（Redis）各一个实现，统一存取 JSON 字符串。
 */
public interface CacheChannel {

    /**
     * 读取原始 JSON 值。
     *
     * @return 命中返回 {@link CacheEntry#of}，未命中返回 {@link CacheEntry#miss()}
     */
    CacheEntry get(String key);

    /**
     * 写入原始 JSON 值，ttl 必须为正；非正 TTL 表示该键跳过写入（禁用该级缓存），不是"永不过期"。
     *
     * @param json 值或 {@code JsonCodec.NULL_SENTINEL}（null 占位）
     */
    void put(String key, String json, Duration ttl);

    /** 删除键，键不存在时静默 */
    void evict(String key);

    /**
     * 删除并报告结果：true 表示键已确认删除（或本就不存在），false 表示删除失败（通道故障）。
     * 失效方据此安排重试，避免"删除恰好落在 Redis 闪断窗口内、旧值滞留到 TTL"的失效丢失。
     * 默认实现委托 {@link #evict} 并假定成功——能感知删除失败的通道应覆写。
     */
    default boolean evictReliably(String key) {
        evict(key);
        return true;
    }

    /**
     * 批量删除（Redis 走单条 DEL 多键命令）。返回 false 表示批量删除失败，
     * 调用方退回逐键 {@link #evictReliably} 或安排重试。默认逐键委托 {@link #evict}。
     */
    default boolean evictAll(Collection<String> keys) {
        for (String key : keys) {
            evict(key);
        }
        return true;
    }

    /**
     * 批量读取（管道/ MGET 优化点）。返回 key → entry 的映射，包含全部请求键。
     * 实现不支持批量时返回 null，调用方回退逐键 {@link #get}。
     */
    default Map<String, CacheEntry> multiGet(List<String> keys) {
        Map<String, CacheEntry> out = new LinkedHashMap<>();
        for (String key : keys) {
            out.put(key, get(key));
        }
        return out;
    }
}
