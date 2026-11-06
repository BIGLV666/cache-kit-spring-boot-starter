package io.github.biglv666.cachekit.annotation;

import io.github.biglv666.cachekit.core.CacheStore;
import io.github.biglv666.cachekit.core.EntityCache;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 字段注入注解：按字段类型注入缓存句柄。
 *
 * <ul>
 *   <li>{@link EntityCache}：实体缓存句柄，{@link #value()} 为实体类型；</li>
 *   <li>{@link CacheStore}：通用缓存句柄，{@link #value()} 为标注 {@code @CacheObject} 的值类型。</li>
 * </ul>
 *
 * <pre>{@code
 * @CacheHandle(User.class)
 * private EntityCache<User> userCache;
 *
 * @CacheHandle(AppConfig.class)
 * private CacheStore<AppConfig> configStore;
 * }</pre>
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheHandle {

    /** 实体类型（EntityCache）或通用缓存值类型（CacheStore） */
    Class<?> value();
}
