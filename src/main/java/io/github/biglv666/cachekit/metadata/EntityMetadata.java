package io.github.biglv666.cachekit.metadata;

import io.github.biglv666.cachekit.exception.CacheKitException;

import java.lang.reflect.Field;
import java.time.Duration;

/**
 * 实体缓存元数据：缓存键前缀、主键字段、可选的实体级 TTL 覆盖。
 *
 * <p>由 {@link EntityMetadataRegistry} 在首次使用时惰性解析并缓存。
 */
public final class EntityMetadata {

    private final Class<?> entityType;
    private final String prefix;
    private final Field idField;
    private final Duration ttl;

    EntityMetadata(Class<?> entityType, String prefix, Field idField, Duration ttl) {
        this.entityType = entityType;
        this.prefix = prefix;
        this.idField = idField;
        this.ttl = ttl;
    }

    public Class<?> entityType() {
        return entityType;
    }

    public String prefix() {
        return prefix;
    }

    public Field idField() {
        return idField;
    }

    /** 实体级 TTL 覆盖，null 表示使用全局配置 */
    public Duration ttl() {
        return ttl;
    }

    /** 缓存键：形如 {@code user:123} */
    public String keyOf(Object id) {
        return prefix + ":" + id;
    }

    /** 从实体实例读取主键值 */
    public Object idOf(Object entity) {
        try {
            return idField.get(entity);
        } catch (IllegalAccessException e) {
            throw new CacheKitException("读取主键失败: " + entityType.getName(), e);
        }
    }
}
