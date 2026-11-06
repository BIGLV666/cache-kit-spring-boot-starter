package io.github.biglv666.cachekit.generic;

import java.time.Duration;

/**
 * 通用缓存元数据：值类型、键前缀、默认 TTL、null 缓存策略。
 *
 * @param valueType 标注 {@code @CacheObject} 的值类型
 * @param prefix    缓存键前缀（不含保留段与 namespace）
 * @param ttl       值类型级 TTL 覆盖，null 表示使用全局 L2 TTL
 * @param cacheNull 是否缓存 null 结果
 */
public record GenericCacheMetadata(Class<?> valueType, String prefix, Duration ttl, boolean cacheNull) {
}
