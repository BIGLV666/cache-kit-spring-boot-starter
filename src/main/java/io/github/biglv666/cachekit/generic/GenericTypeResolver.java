package io.github.biglv666.cachekit.generic;

import com.fasterxml.jackson.databind.JavaType;
import io.github.biglv666.cachekit.support.JsonCodec;
import org.springframework.core.ResolvableType;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Map;

/**
 * 通用缓存类型解析：把方法返回类型 / 句柄字段类型按<b>具体实现类</b>解析成 Jackson
 * {@link JavaType}，并在泛型无法解析时用 {@code @CacheLoad.value} / {@code @CacheHandle.value}
 * 兜底，避免命中缓存时退化成 {@code LinkedHashMap}。
 *
 * <p>解析顺序：</p>
 * <ol>
 *   <li>类型变量按实现类给出的实际实参替换（含 {@code Api<T> → List<String>} 这类参数化实参）；</li>
 *   <li>类型变量仍未解析、返回 {@code Object}、无界通配符 → 用显式值类型；</li>
 *   <li>容器（{@code List<T>}、{@code Map<K,V>} 等）逐层递归，未解析的类型变量用显式值类型替换；</li>
 *   <li>裸 {@code Map} / {@code Map<?, V>} 的键类型无法从 JSON 对象还原，按 {@code String} 处理。</li>
 * </ol>
 */
public final class GenericTypeResolver {

    private GenericTypeResolver() {
    }

    /**
     * 解析 {@code @CacheLoad} 方法的反序列化类型。
     *
     * @param method           被注解的方法（可能是接口/父类方法，返回类型含类型变量）
     * @param targetClass      实际实现类，用于解析类型变量；null 时退回声明类
     * @param explicitValueType {@code @CacheLoad.value}，无法解析时作为兜底；null 表示未指定
     */
    public static JavaType methodReturnType(Method method, Class<?> targetClass, Class<?> explicitValueType) {
        Class<?> target = targetClass == null ? method.getDeclaringClass() : targetClass;
        ResolvableType returnType = ResolvableType.forMethodReturnType(method, target);
        // 顶层声明为 Object 且显式给了值类型（@CacheLoad.value）才替换；
        // 嵌套的 Map<String,Object> 等显式 Object 保持不变。
        if (explicitValueType != null && returnType.getType() == Object.class) {
            return JsonCodec.constructType(explicitValueType);
        }
        return toJavaType(returnType, explicitValueType, typeVariableMap(target));
    }

    /**
     * 解析 {@code @CacheHandle} 字段 {@code CacheStore<T>} 的反序列化类型。
     *
     * @param field            字段（可能声明在泛型父类中）
     * @param targetClass      字段所属 bean 的实现类，用于解析父类类型变量
     * @param explicitValueType {@code @CacheHandle.value} 对应的值类型，无法解析时兜底
     */
    public static JavaType fieldType(Field field, Class<?> targetClass, Class<?> explicitValueType) {
        Class<?> target = targetClass == null ? field.getDeclaringClass() : targetClass;
        ResolvableType valueType = ResolvableType.forField(field, target).getGeneric(0);
        // 与 @CacheLoad 一致：顶层就是 Object（如 CacheStore<Object>）时用句柄值类型兜底
        if (explicitValueType != null && valueType.getType() == Object.class) {
            return JsonCodec.constructType(explicitValueType);
        }
        return toJavaType(valueType, explicitValueType, typeVariableMap(target));
    }

    /**
     * 实现类上类型变量 → 实际实参的映射。ResolvableType 对
     * {@code Map<T, String>}（T = List<String>）只能解析出裸 List，
     * 这里借 Spring 的映射保留参数化实参。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Map<TypeVariable<?>, Type> typeVariableMap(Class<?> target) {
        return (Map) org.springframework.core.GenericTypeResolver.getTypeVariableMap(target);
    }

    private static JavaType toJavaType(ResolvableType type, Class<?> explicitValueType,
                                       Map<TypeVariable<?>, Type> variables) {
        if (type == null || type == ResolvableType.NONE) {
            return fallback(explicitValueType);
        }
        Type rawType = type.getType();
        if (rawType == null) {
            return fallback(explicitValueType);
        }
        if (rawType instanceof TypeVariable<?> variable) {
            return resolveVariable(type, variable, explicitValueType, variables);
        }
        if (rawType instanceof WildcardType wildcard) {
            return resolveWildcard(type, wildcard, explicitValueType, variables);
        }
        if (rawType instanceof GenericArrayType) {
            return JsonCodec.constructArrayType(toJavaType(type.getComponentType(), explicitValueType, variables));
        }

        Class<?> raw = type.resolve();
        if (raw == null) {
            return fallback(explicitValueType);
        }
        ResolvableType[] generics = type.getGenerics();
        if (generics.length == 0) {
            return JsonCodec.constructType(rawType);
        }

        JavaType[] arguments = new JavaType[generics.length];
        for (int i = 0; i < generics.length; i++) {
            // 裸 Map / Map<?, V> 的键类型无法从 JSON 对象还原，按 String 处理；
            // 声明了具体键类型（Map<Long, Foo>）时保留泛型解析结果。
            if (i == 0 && Map.class.isAssignableFrom(raw)
                    && explicitValueType != null && isUnresolved(generics[i])) {
                arguments[i] = JsonCodec.constructType(String.class);
            } else {
                arguments[i] = toJavaType(generics[i], explicitValueType, variables);
            }
        }
        return JsonCodec.constructParametricType(raw, arguments);
    }

    private static JavaType resolveVariable(ResolvableType type, TypeVariable<?> variable,
                                            Class<?> explicitValueType,
                                            Map<TypeVariable<?>, Type> variables) {
        Type mapped = variables.get(variable);
        if (mapped != null && mapped != variable) {
            if (mapped instanceof TypeVariable<?> mappedVariable) {
                // 自引用泛型（T extends Comparable<T> 等）：退回上界，避免无限递归
                return JsonCodec.constructType(boundOf(mappedVariable));
            }
            return toJavaType(ResolvableType.forType(mapped), explicitValueType, variables);
        }
        if (isUnresolvedVariable(type) && explicitValueType != null) {
            return JsonCodec.constructType(explicitValueType);
        }
        Class<?> resolved = type.resolve();
        if (resolved != null) {
            return JsonCodec.constructType(resolved);
        }
        return explicitValueType != null
                ? JsonCodec.constructType(explicitValueType)
                : JsonCodec.constructType(boundOf(variable));
    }

    private static JavaType resolveWildcard(ResolvableType type, WildcardType wildcard,
                                            Class<?> explicitValueType,
                                            Map<TypeVariable<?>, Type> variables) {
        Type[] upperBounds = wildcard.getUpperBounds();
        Type bound = upperBounds.length == 0 ? Object.class : upperBounds[0];
        if (bound instanceof TypeVariable<?> variable) {
            Type mapped = variables.get(variable);
            if (mapped != null && mapped != variable) {
                return toJavaType(ResolvableType.forType(mapped), explicitValueType, variables);
            }
            Class<?> resolved = type.resolve();
            return resolved != null && resolved != Object.class
                    ? JsonCodec.constructType(resolved)
                    : fallback(explicitValueType);
        }
        if (bound == Object.class) {
            // 无界通配符（含裸容器的擦除结果）：无法推断，使用显式值类型
            return fallback(explicitValueType);
        }
        return toJavaType(ResolvableType.forType(bound), explicitValueType, variables);
    }

    /**
     * 类型变量是否仍未解析：{@code resolve()} 要么是 Object，要么只是声明上界，
     * 而非实现类给出的具体实参。
     */
    private static boolean isUnresolvedVariable(ResolvableType type) {
        if (!(type.getType() instanceof TypeVariable<?> variable)) {
            return false;
        }
        Class<?> resolved = type.resolve();
        if (resolved == null || resolved == Object.class) {
            return true;
        }
        Type[] bounds = variable.getBounds();
        return bounds.length > 0 && resolved.equals(bounds[0]);
    }

    /** 类型变量或（由裸容器擦除产生的）无界通配符：都没有可用的具体实参 */
    private static boolean isUnresolved(ResolvableType type) {
        if (type.getType() instanceof TypeVariable<?>) {
            return isUnresolvedVariable(type);
        }
        if (type.getType() instanceof WildcardType wildcard) {
            Type[] upperBounds = wildcard.getUpperBounds();
            return upperBounds.length == 0 || upperBounds[0] == Object.class;
        }
        return false;
    }

    private static Type boundOf(TypeVariable<?> variable) {
        Type[] bounds = variable.getBounds();
        return bounds.length == 1 ? bounds[0] : Object.class;
    }

    private static JavaType fallback(Class<?> explicitValueType) {
        return JsonCodec.constructType(explicitValueType == null ? Object.class : explicitValueType);
    }
}
