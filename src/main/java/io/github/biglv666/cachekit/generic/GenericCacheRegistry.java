package io.github.biglv666.cachekit.generic;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.biglv666.cachekit.annotation.CacheLoad;
import io.github.biglv666.cachekit.annotation.CacheObject;
import io.github.biglv666.cachekit.core.GenericKeyCodec;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.support.NamingUtils;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 通用缓存元数据注册表：解析 {@code @CacheObject}、方法返回类型中的值类型，
 * 维护 {@code prefix → 元数据} 索引（广播白名单与类型解析共用）。
 *
 * <p>前缀是键空间的一部分：同一 prefix 绑定两个不同类型会在注册期 fail-fast，
 * 避免缓存值被另一种类型反序列化后静默缺字段/错数据。方法返回类型无法解析出
 * 唯一 {@code @CacheObject} 类型时同样 fail-fast，可用 {@code @CacheLoad.value}
 * 显式指定元数据来源。</p>
 */
public class GenericCacheRegistry {

    private final Map<Class<?>, Optional<GenericCacheMetadata>> cache = new ConcurrentHashMap<>();
    private final Map<String, GenericCacheMetadata> byPrefix = new ConcurrentHashMap<>();
    private final Map<MethodKey, GenericCacheMetadata> byMethod = new ConcurrentHashMap<>();

    /** 获取值类型元数据，未标注返回 null */
    public GenericCacheMetadata find(Class<?> valueType) {
        if (valueType == null || valueType.isPrimitive() || valueType.isArray()) {
            return null;
        }
        return cache.computeIfAbsent(valueType, t -> Optional.ofNullable(parse(t))).orElse(null);
    }

    /** 获取值类型元数据，未标注抛异常 */
    public GenericCacheMetadata require(Class<?> valueType) {
        GenericCacheMetadata meta = find(valueType);
        if (meta == null) {
            throw new CacheKitException("类型 " + valueType.getName()
                    + " 未标注 @CacheObject，不能被通用缓存使用");
        }
        return meta;
    }

    /**
     * 解析并注册一个 {@code @CacheLoad} 方法（以方法声明类作为类型解析目标）。
     * 结果按 {@code (Method, targetClass)} 缓存；重复调用幂等。
     */
    public GenericCacheMetadata register(Method method, CacheLoad cacheLoad) {
        return register(method, method.getDeclaringClass(), cacheLoad);
    }

    /**
     * 解析并注册一个 {@code @CacheLoad} 方法，返回其元数据。
     * 结果按 {@code (Method, targetClass)} 缓存；同一接口方法被不同泛型实现类使用时互不干扰。
     *
     * @param targetClass 实际实现类，用于解析返回类型中的类型变量；null 退回方法声明类
     */
    public GenericCacheMetadata register(Method method, Class<?> targetClass, CacheLoad cacheLoad) {
        if (cacheLoad.key() == null || cacheLoad.key().isBlank()) {
            throw new CacheKitException("@CacheLoad.key 不能为空: " + method);
        }
        if (method.getReturnType() == void.class) {
            throw new CacheKitException("@CacheLoad 方法必须有返回值，不能是 void: " + method);
        }
        Class<?> resolvedTarget = targetClass == null ? method.getDeclaringClass() : targetClass;
        MethodKey key = new MethodKey(method, resolvedTarget);
        GenericCacheMetadata cached = byMethod.get(key);
        if (cached != null) {
            return cached;
        }
        Class<?> valueType = resolveValueType(method, resolvedTarget, cacheLoad);
        GenericCacheMetadata meta = require(valueType);
        GenericCacheMetadata prev = byMethod.putIfAbsent(key, meta);
        return prev == null ? meta : prev;
    }

    /** 按前缀查元数据（广播校验用）；未知前缀返回 null */
    public GenericCacheMetadata findByPrefix(String prefix) {
        return prefix == null ? null : byPrefix.get(prefix);
    }

    /**
     * 广播键解析：键形如 {@code [自定义段:]namespace:__gk__:prefix:用户key}。
     * 返回 null 表示前缀未知或被 namespace 校验拒绝，调用方应忽略该消息。
     */
    public GenericCacheMetadata findByBroadcastKey(String key, String namespace) {
        if (key == null || key.isBlank()) {
            return null;
        }
        String ns = namespace == null ? "" : namespace;
        String segmentPrefix = GenericKeyCodec.SEGMENT + ":";
        String marker = ":" + segmentPrefix;
        int prefixStart;
        if (key.startsWith(segmentPrefix)) {
            if (!ns.isBlank()) {
                return null;
            }
            prefixStart = segmentPrefix.length();
        } else {
            int idx = key.indexOf(marker);
            if (idx < 0) {
                return null;
            }
            if (!ns.isBlank()) {
                String before = key.substring(0, idx);
                if (!before.equals(ns) && !before.endsWith(":" + ns)) {
                    return null;
                }
            }
            prefixStart = idx + marker.length();
        }
        int prefixEnd = key.indexOf(':', prefixStart);
        if (prefixEnd < 0) {
            return null;
        }
        return byPrefix.get(key.substring(prefixStart, prefixEnd));
    }

    /** 已注册值类型快照（运维/测试用） */
    public Set<Class<?>> valueTypes() {
        Set<Class<?>> types = new LinkedHashSet<>();
        for (GenericCacheMetadata meta : byPrefix.values()) {
            types.add(meta.valueType());
        }
        return java.util.Collections.unmodifiableSet(types);
    }

    /** 已注册前缀快照（运维/测试用） */
    public Set<String> prefixes() {
        return java.util.Collections.unmodifiableSet(byPrefix.keySet());
    }

    private GenericCacheMetadata parse(Class<?> type) {
        CacheObject annotation = type.getAnnotation(CacheObject.class);
        if (annotation == null) {
            return null;
        }
        validateInstantiable(type);
        String prefix = annotation.prefix().isBlank()
                ? NamingUtils.camelToSnake(type.getSimpleName())
                : annotation.prefix().trim();
        validatePrefix(type, prefix);
        Duration ttl = annotation.ttl() > 0 ? Duration.ofSeconds(annotation.ttl()) : null;
        GenericCacheMetadata meta = new GenericCacheMetadata(type, prefix, ttl, annotation.cacheNull());
        GenericCacheMetadata prev = byPrefix.putIfAbsent(prefix, meta);
        if (prev != null && prev.valueType() != type) {
            throw new CacheKitException("通用缓存前缀冲突（'" + prefix + "'）："
                    + prev.valueType().getName() + " 与 " + type.getName()
                    + " 共用同一前缀会互相反序列化成错数据。"
                    + "请用 @CacheObject.prefix 区分");
        }
        return meta;
    }

    /**
     * Jackson 只能实例化具体类；接口/抽象类除非显式配置 {@code @JsonTypeInfo} 多态信息，
     * 否则启动后写入成功、每次读取都解码失败（静默 miss）。这里按组件 fail-fast 原则拒绝。
     */
    private static void validateInstantiable(Class<?> type) {
        if (!type.isInterface() && !Modifier.isAbstract(type.getModifiers())) {
            return;
        }
        if (AnnotatedElementUtils.findMergedAnnotation(type, JsonTypeInfo.class) != null) {
            return;
        }
        throw new CacheKitException("@CacheObject 不能标注在接口或抽象类上（Jackson 无法实例化）: "
                + type.getName() + "。请标注具体实现类，或配置 Jackson @JsonTypeInfo 启用多态反序列化");
    }

    private static void validatePrefix(Class<?> type, String prefix) {
        if (prefix.isBlank()) {
            throw new CacheKitException("@CacheObject.prefix 不能为空: " + type.getName());
        }
        if (prefix.indexOf(':') >= 0) {
            throw new CacheKitException("@CacheObject.prefix 不能包含 ':'（键段歧义）: "
                    + type.getName() + " -> " + prefix);
        }
        if (prefix.contains(GenericKeyCodec.SEGMENT)) {
            throw new CacheKitException("@CacheObject.prefix 不能包含保留段 '" + GenericKeyCodec.SEGMENT
                    + "': " + type.getName() + " -> " + prefix);
        }
    }

    private Class<?> resolveValueType(Method method, Class<?> targetClass, CacheLoad cacheLoad) {
        Class<?> explicit = cacheLoad.value();
        if (explicit != void.class && explicit != Void.class) {
            return explicit;
        }
        Set<Class<?>> found = new LinkedHashSet<>();
        collectAnnotatedTypes(ResolvableType.forMethodReturnType(method, targetClass), found, new LinkedHashSet<>());
        if (found.isEmpty()) {
            throw new CacheKitException("@CacheLoad 无法从方法返回类型解析出 @CacheObject 值类型: " + method
                    + "。请让返回类型（或其泛型参数）标注 @CacheObject，"
                    + "或用 @CacheLoad(value = Xxx.class) 指定元数据来源");
        }
        if (found.size() > 1) {
            throw new CacheKitException("@CacheLoad 返回类型的泛型参数中发现多个 @CacheObject 类型 "
                    + found.stream().map(Class::getName).toList()
                    + "，无法确定缓存前缀: " + method + "。请用 @CacheLoad(value = Xxx.class) 明确指定");
        }
        return found.iterator().next();
    }

    private void collectAnnotatedTypes(ResolvableType type, Set<Class<?>> found, Set<ResolvableType> visited) {
        if (type == null || type == ResolvableType.NONE || !visited.add(type)) {
            return;
        }
        Class<?> raw = type.resolve();
        if (raw != null && raw.isAnnotationPresent(CacheObject.class)) {
            found.add(raw);
            return;
        }
        if (raw != null && raw.isArray()) {
            collectAnnotatedTypes(type.getComponentType(), found, visited);
            return;
        }
        for (ResolvableType generic : type.getGenerics()) {
            collectAnnotatedTypes(generic, found, visited);
        }
    }

    /** 注册缓存键：同一接口/父类方法在不同实现类下可能解析出不同值类型。 */
    private record MethodKey(Method method, Class<?> targetClass) {
    }
}
