package io.github.biglv666.cachekit.channel;

import java.time.Duration;

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
     * 写入原始 JSON 值，ttl 必须为正。
     *
     * @param json 值或 {@code JsonCodec.NULL_SENTINEL}（null 占位）
     */
    void put(String key, String json, Duration ttl);

    /** 删除键，键不存在时静默 */
    void evict(String key);
}
