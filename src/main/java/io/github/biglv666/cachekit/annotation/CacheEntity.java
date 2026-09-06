package io.github.biglv666.cachekit.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记实体可被缓存（非 MyBatis-Plus 项目的元数据来源）。
 *
 * <p>类路径检测到 MyBatis-Plus 时优先复用 {@code @TableName} / {@code @TableId}，
 * 本注解用于非 MP 项目，或在 MP 项目中覆盖部分参数。两者并存时本注解优先。
 *
 * <p>缓存键形如 {@code prefix:主键值}，prefix 缺省为类名转蛇形（与 MyBatis 驼峰映射约定一致）。
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheEntity {

    /** 缓存键前缀，默认类名转蛇形，如 UserLogin → user_login */
    String prefix() default "";

    /** 覆盖全局 L2 TTL（秒），&le;0 表示使用全局配置 */
    long ttl() default -1;
}
