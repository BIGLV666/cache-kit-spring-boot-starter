package io.github.biglv666.cachekit.metadata;

import io.github.biglv666.cachekit.annotation.CacheEntity;
import io.github.biglv666.cachekit.annotation.CacheId;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.support.NamingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 实体元数据注册表：惰性解析 {@code @CacheEntity} / MyBatis-Plus {@code @TableName}、
 * {@code @TableId} 并缓存结果。
 *
 * <p>可缓存判定：存在 {@code @CacheEntity}、{@code @CacheId}、MP {@code @TableName} 或
 * MP {@code @TableId} 之一，且能确定唯一主键字段。其余实体返回 null（不支持缓存）。
 * MyBatis-Plus 注解通过反射按名读取，避免对 MP 的硬依赖。
 */
public class EntityMetadataRegistry {

    private static final Logger log = LoggerFactory.getLogger(EntityMetadataRegistry.class);

    private static final String MP_TABLE_NAME = "com.baomidou.mybatisplus.annotation.TableName";
    private static final String MP_TABLE_ID = "com.baomidou.mybatisplus.annotation.TableId";

    private final Map<Class<?>, Optional<EntityMetadata>> cache = new ConcurrentHashMap<>();
    private final boolean mpAvailable;
    private final Class<? extends Annotation> mpTableName;
    private final Class<? extends Annotation> mpTableId;

    public EntityMetadataRegistry() {
        this.mpTableName = loadMpAnnotation(MP_TABLE_NAME);
        this.mpTableId = loadMpAnnotation(MP_TABLE_ID);
        this.mpAvailable = mpTableName != null;
    }

    /** MP 是否在类路径（日志与测试用途） */
    public boolean isMybatisPlusAvailable() {
        return mpAvailable;
    }

    /**
     * 获取实体元数据，不可缓存时抛出异常。
     *
     * @throws CacheKitException 实体未注册缓存元数据
     */
    public EntityMetadata require(Class<?> entityType) {
        EntityMetadata meta = find(entityType);
        if (meta == null) {
            throw new CacheKitException(
                    "实体 " + entityType.getName() + " 未声明 @CacheEntity/@CacheId（或 MyBatis-Plus 注解），不支持缓存");
        }
        return meta;
    }

    /**
     * 获取实体元数据，不可缓存返回 null。解析结果按类缓存（含否定缓存）。
     */
    public EntityMetadata find(Class<?> entityType) {
        if (entityType == null || entityType.isPrimitive() || entityType.isArray()) {
            return null;
        }
        return cache.computeIfAbsent(entityType, t -> Optional.ofNullable(parse(t))).orElse(null);
    }

    /**
     * 按表名（键前缀）查找已解析的实体元数据，用于 binlog 行事件匹配。
     * 尚未解析的实体没有缓存条目，错过其失效事件是无害的。
     */
    public EntityMetadata findByPrefix(String tableName) {
        for (Optional<EntityMetadata> meta : cache.values()) {
            if (meta.isPresent() && meta.get().prefix().equals(tableName)) {
                return meta.get();
            }
        }
        return null;
    }

    private EntityMetadata parse(Class<?> type) {
        CacheEntity cacheEntity = type.getAnnotation(CacheEntity.class);
        String prefix;
        Duration ttl = null;
        boolean cacheable = false;

        if (cacheEntity != null) {
            prefix = cacheEntity.prefix().isBlank()
                    ? NamingUtils.camelToSnake(type.getSimpleName())
                    : cacheEntity.prefix();
            ttl = cacheEntity.ttl() > 0 ? Duration.ofSeconds(cacheEntity.ttl()) : null;
            cacheable = true;
        } else if (mpTableName != null && type.isAnnotationPresent(mpTableName)) {
            String tableName = annotationValue(type.getAnnotation(mpTableName));
            prefix = tableName == null || tableName.isBlank()
                    ? NamingUtils.camelToSnake(type.getSimpleName())
                    : tableName;
            cacheable = true;
        } else {
            prefix = NamingUtils.camelToSnake(type.getSimpleName());
        }

        Field idField = findIdField(type);
        // 有主键字段（@CacheId/@TableId）即视为可缓存，允许纯 MP 实体零注解接入
        if (idField != null && (idField.isAnnotationPresent(CacheId.class)
                || (mpTableId != null && idField.isAnnotationPresent(mpTableId)))) {
            cacheable = true;
        }
        if (!cacheable || idField == null) {
            log.debug("实体 {} 不满足缓存元数据要求，不参与缓存", type.getName());
            return null;
        }
        idField.setAccessible(true);
        return new EntityMetadata(type, prefix, idField, ttl);
    }

    /** 主键字段解析优先级：@CacheId → MP @TableId → 名为 id 的字段 */
    private Field findIdField(Class<?> type) {
        Field byCacheId = findAnnotatedField(type, CacheId.class);
        if (byCacheId != null) {
            return byCacheId;
        }
        if (mpTableId != null) {
            Field byTableId = findAnnotatedField(type, mpTableId);
            if (byTableId != null) {
                return byTableId;
            }
        }
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (f.getName().equals("id")) {
                    return f;
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Class<? extends Annotation> loadMpAnnotation(String className) {
        try {
            return (Class<? extends Annotation>) Class.forName(className);
        } catch (ClassNotFoundException | LinkageError e) {
            return null;
        }
    }

    /** 读取注解的 value() 属性，缺失或读取失败返回 null */
    private String annotationValue(Annotation annotation) {
        try {
            Object v = annotation.getClass().getMethod("value").invoke(annotation);
            return v instanceof String s ? s : null;
        } catch (Exception e) {
            return null;
        }
    }

    private Field findAnnotatedField(Class<?> type, Class<? extends Annotation> annotation) {
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (f.isAnnotationPresent(annotation)) {
                    return f;
                }
            }
        }
        return null;
    }
}
