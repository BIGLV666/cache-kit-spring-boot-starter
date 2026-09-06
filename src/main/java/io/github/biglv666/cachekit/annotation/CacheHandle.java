package io.github.biglv666.cachekit.annotation;

import io.github.biglv666.cachekit.core.EntityCache;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 字段注入注解：向字段注入 {@link EntityCache} 句柄，
 * 用于无法加方法注解的场景（三方 mapper、动态拼接查询）。
 *
 * <pre>{@code
 * @CacheHandle(User.class)
 * private EntityCache<User> userCache;
 *
 * User u = userCache.get(id, () -> mapper.selectByUserId(id));
 * }</pre>
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheHandle {

    /** 缓存对应的实体类型 */
    Class<?> value();
}
