package io.github.biglv666.cachekit.channel;

/**
 * 缓存通道查询结果：hit 区分"未命中"与"命中但值为 null"（穿透占位）。
 *
 * @param hit  是否命中
 * @param json 命中的 JSON 字符串，可能为 null 占位符
 */
public record CacheEntry(boolean hit, String json) {

    public static CacheEntry miss() {
        return new CacheEntry(false, null);
    }

    public static CacheEntry of(String json) {
        return new CacheEntry(true, json);
    }
}
