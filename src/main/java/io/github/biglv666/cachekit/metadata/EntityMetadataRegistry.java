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
import java.util.ArrayList;
import java.util.List;
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
    /** 缓存前缀 → 所属实体：前缀是全局键空间，冲突会让两个实体互相覆盖缓存值，必须 fail-fast */
    private final Map<String, Class<?>> prefixOwners = new ConcurrentHashMap<>();
    /** 缓存前缀 → 元数据索引：binlog 行事件按表名 O(1) 匹配 */
    private final Map<String, EntityMetadata> byPrefix = new ConcurrentHashMap<>();
    private final boolean mpAvailable;
    private final Class<? extends Annotation> mpTableName;
    private final Class<? extends Annotation> mpTableId;
    private volatile DefaultTableNameResolver defaultTableNameResolver;

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
        return byPrefix.get(tableName);
    }

    /** 已解析实体的只读快照（前缀 → 元数据），运维端点展示用 */
    public java.util.Collection<EntityMetadata> entities() {
        return java.util.Collections.unmodifiableCollection(byPrefix.values());
    }

    /**
     * 广播键解析（订阅端失效用）：键形如 {@code [自定义段:]namespace:前缀:id}，
     * 主键段本身可能含 ':'（String 主键），因此不能从右侧按冒号切分。
     * 判定规则：实体前缀必须作为完整段出现（键以 {@code 前缀:} 开头，或键中含 {@code :前缀:}），
     * 且配置了 namespace 时前缀的紧邻前一段必须是 namespace。
     * customizer 段不校验——其他实例/租户的键在本地不存在，删除是无害空操作。
     *
     * <p>返回 null 表示前缀无法匹配已知实体（广播被忽略，防任意客户端清缓存）。</p>
     */
    public EntityMetadata findByBroadcastKey(String key, String namespace) {
        String ns = namespace == null ? "" : namespace;
        for (EntityMetadata meta : byPrefix.values()) {
            if (matchesBroadcastKey(key, meta.prefix(), ns)) {
                return meta;
            }
        }
        return null;
    }

    private boolean matchesBroadcastKey(String key, String prefix, String namespace) {
        // 前缀作为首段：后面紧跟 id 段，此时键里不允许再有 namespace 段
        if (key.startsWith(prefix + ":")) {
            return namespace.isBlank();
        }
        // 前缀作为中间段（其后至少还有 id 段）：namespace（若配置）必须是紧邻前一段
        String needle = ":" + prefix + ":";
        int idx = key.indexOf(needle);
        while (idx >= 0) {
            if (namespace.isBlank()) {
                return true;
            }
            String before = key.substring(0, idx);
            if (before.equals(namespace) || before.endsWith(":" + namespace)) {
                return true;
            }
            idx = key.indexOf(needle, idx + 1);
        }
        return false;
    }

    /**
     * 注册 MP 全局表名感知的默认表名解析器（由 MyBatis-Plus 适配层注入）：
     * 实体无 {@code @TableName}（或 value 为空）时，用 MP 全局 table-prefix / table-underline
     * 推导物理表名，保证缓存前缀与 binlog 行事件表名一致；未注入则维持类名驼峰转蛇形的兜底。
     */
    public void setDefaultTableNameResolver(DefaultTableNameResolver resolver) {
        this.defaultTableNameResolver = resolver;
    }

    /** 默认表名解析器：输入实体类，返回物理表名（可返回 null 走兜底） */
    public interface DefaultTableNameResolver {
        String resolve(Class<?> entityType);
    }

    private EntityMetadata parse(Class<?> type) {
        CacheEntity cacheEntity = type.getAnnotation(CacheEntity.class);
        String prefix;
        Duration ttl = null;
        boolean cacheable = false;

        if (cacheEntity != null) {
            prefix = cacheEntity.prefix().isBlank()
                    ? defaultTableName(type)
                    : cacheEntity.prefix();
            ttl = cacheEntity.ttl() > 0 ? Duration.ofSeconds(cacheEntity.ttl()) : null;
            cacheable = true;
        } else if (mpTableName != null && type.isAnnotationPresent(mpTableName)) {
            String tableName = annotationValue(type.getAnnotation(mpTableName));
            prefix = tableName == null || tableName.isBlank()
                    ? defaultTableName(type)
                    : tableName;
            cacheable = true;
        } else {
            prefix = defaultTableName(type);
        }

        List<Field> idFields = findIdFields(type);
        // 有主键字段（@CacheId/@TableId）即视为可缓存，允许纯 MP 实体零注解接入
        Field firstId = idFields.isEmpty() ? null : idFields.get(0);
        if (firstId != null && (firstId.isAnnotationPresent(CacheId.class)
                || (mpTableId != null && firstId.isAnnotationPresent(mpTableId)))) {
            cacheable = true;
        }
        if (!cacheable || idFields.isEmpty()) {
            log.debug("实体 {} 不满足缓存元数据要求，不参与缓存", type.getName());
            return null;
        }
        // 前缀冲突 fail-fast：键空间按 prefix 划分，两个实体共享前缀会互相覆盖缓存值——
        // 字段子集读对方的 JSON 不报错（FAIL_ON_UNKNOWN_PROPERTIES 关闭），静默缺字段返回错数据
        Class<?> prevOwner = prefixOwners.putIfAbsent(prefix, type);
        if (prevOwner != null && prevOwner != type) {
            throw new CacheKitException("实体 " + type.getName() + " 与 " + prevOwner.getName()
                    + " 的缓存前缀冲突（'" + prefix + "'）：同前缀实体共享同一键空间，会互相覆盖缓存值"
                    + "（读取方按自己的实体类型反序列化，字段子集会静默缺字段）。"
                    + "请用 @CacheEntity.prefix / @TableName 区分前缀，或移除其中一个实体的缓存接入");
        }
        for (Field field : idFields) {
            field.setAccessible(true);
        }
        EntityMetadata metadata = new EntityMetadata(type, prefix, idFields, ttl);
        byPrefix.put(prefix, metadata);
        return metadata;
    }

    /** 默认表名：MP 全局 table-prefix/table-underline 感知（解析器已注入时），否则类名驼峰转蛇形 */
    private String defaultTableName(Class<?> type) {
        DefaultTableNameResolver resolver = defaultTableNameResolver;
        if (resolver != null) {
            try {
                String resolved = resolver.resolve(type);
                if (resolved != null && !resolved.isBlank()) {
                    return resolved;
                }
            } catch (Exception e) {
                throw new CacheKitException("解析实体 " + type.getName() + " 的默认表名失败", e);
            }
        }
        return NamingUtils.camelToSnake(type.getSimpleName());
    }

    /**
     * 主键字段解析优先级：@CacheId → MP @TableId → 名为 id 的字段。
     * 多个 @CacheId 组成复合主键（声明顺序，父类字段在后）——仅注解查询路径支持；
     * 多个 MP @TableId 仍抛错：MyBatis-Plus 本身不支持复合主键，其自动接入路径
     *（selectById 等）拿到的主键会与复合键段错位。
     */
    private List<Field> findIdFields(Class<?> type) {
        List<Field> byCacheId = findAnnotatedFields(type, CacheId.class);
        if (!byCacheId.isEmpty()) {
            return byCacheId;
        }
        if (mpTableId != null) {
            List<Field> byTableId = findAnnotatedFields(type, mpTableId);
            if (byTableId.size() > 1) {
                throw new CacheKitException("实体 " + type.getName() + " 声明了多个 MP @TableId 主键字段"
                        + "（" + byTableId.get(0).getName() + " / " + byTableId.get(1).getName() + "）："
                        + "MyBatis-Plus 不支持复合主键。复合主键请改用多个 @CacheId"
                        + "（仅注解查询/手动句柄路径），或只保留一个 @TableId");
            }
            if (!byTableId.isEmpty()) {
                return byTableId;
            }
        }
        Field byName = null;
        int named = 0;
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (f.getName().equals("id")) {
                    byName = f;
                    named++;
                }
            }
        }
        if (named > 1) {
            throw new CacheKitException("实体 " + type.getName()
                    + " 声明了多个名为 id 的字段，无法确定主键：请用 @CacheId/@TableId 显式标注唯一主键");
        }
        return byName == null ? List.of() : List.of(byName);
    }

    /** 收集全部注解主键字段（类层级自子类到父类、字段声明顺序），可能为空 */
    private List<Field> findAnnotatedFields(Class<?> type, Class<? extends Annotation> annotation) {
        List<Field> found = new ArrayList<>();
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (f.isAnnotationPresent(annotation)) {
                    found.add(f);
                }
            }
        }
        return found;
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
}
