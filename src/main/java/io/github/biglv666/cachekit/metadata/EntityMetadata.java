package io.github.biglv666.cachekit.metadata;

import io.github.biglv666.cachekit.exception.CacheKitException;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 实体缓存元数据：缓存键前缀、主键字段（单主键或 {@code @CacheId} 复合主键）、
 * 可选的实体级 TTL 覆盖。
 *
 * <p>由 {@link EntityMetadataRegistry} 在首次使用时惰性解析并缓存。
 * 复合主键（0.3.3+）：多个 {@code @CacheId} 字段按声明顺序 join ':' 组成键段，
 * 仅支持注解查询路径（MyBatis-Plus 本身不支持复合主键）。</p>
 */
public final class EntityMetadata {

    private final Class<?> entityType;
    private final String prefix;
    private final List<Field> idFields;
    private final Duration ttl;

    EntityMetadata(Class<?> entityType, String prefix, List<Field> idFields, Duration ttl) {
        this.entityType = entityType;
        this.prefix = prefix;
        this.idFields = List.copyOf(idFields);
        this.ttl = ttl;
    }

    public Class<?> entityType() {
        return entityType;
    }

    public String prefix() {
        return prefix;
    }

    /**
     * 唯一主键字段（兼容既有调用方）；复合主键实体无唯一主键字段，抛出异常指引用
     * {@link #idFields()}。
     */
    public Field idField() {
        if (idFields.size() != 1) {
            throw new CacheKitException("实体 " + entityType.getName() + " 是复合主键（"
                    + idFields.size() + " 个 @CacheId 字段），没有唯一主键字段，请改用 idFields()");
        }
        return idFields.get(0);
    }

    /**
     * 主键字段列表（不可变，按声明顺序）：单主键为单元素列表；复合主键按此顺序
     * join ':' 组成键段，读/失效/binlog 还原必须使用同一顺序。
     */
    public List<Field> idFields() {
        return idFields;
    }

    /** 实体级 TTL 覆盖，null 表示使用全局配置 */
    public Duration ttl() {
        return ttl;
    }

    /**
     * 从实体实例读取主键值：单主键返回字段值（兼容既有语义，调用方以
     * {@code String.valueOf} 归一化）；复合主键按声明序取各字段值 join ':'，
     * 任一段为 null 返回 null（调用方按无主键处理）。
     */
    public Object idOf(Object entity) {
        if (idFields.size() == 1) {
            return readField(entity, idFields.get(0));
        }
        List<Object> values = new ArrayList<>(idFields.size());
        for (Field field : idFields) {
            values.add(readField(entity, field));
        }
        return joinSegments(values);
    }

    private Object readField(Object entity, Field field) {
        try {
            return field.get(entity);
        } catch (IllegalAccessException e) {
            throw new CacheKitException("读取主键失败: " + entityType.getName(), e);
        }
    }

    /**
     * 复合主键键段 join：段按序以 ':' 连接；任一段为 null 返回 null（调用方按无主键处理）；
     * 段含 ':' 抛 {@link CacheKitException}——会造成键段歧义（"1:2" 无法区分复合键
     * (1,"2") 与单键 "1:2"），这样的值绝不落缓存（读路径拒绝，失效路径跳过）。
     * 单主键值不受此约束（String 主键含 ':' 是既有合法用法，广播键解析已按段匹配处理）。
     */
    public static String joinSegments(List<Object> segments) {
        StringBuilder sb = new StringBuilder();
        for (Object segment : segments) {
            if (segment == null) {
                return null;
            }
            String s = String.valueOf(segment);
            if (s.indexOf(':') >= 0) {
                throw new CacheKitException("复合主键值不能包含 ':'（键段歧义，无法与组合键区分）: " + s);
            }
            if (sb.length() > 0) {
                sb.append(':');
            }
            sb.append(s);
        }
        return sb.length() == 0 ? null : sb.toString();
    }
}
