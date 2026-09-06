package io.github.biglv666.cachekit.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 查询方法注解：命中缓存直接返回，未命中才执行方法体（查 DB）并回填两级缓存。
 *
 * <p>MVP 仅支持 by-ID 查询：主键从方法参数推导（实体实例参数、参数名与主键字段同名、
 * 或唯一标量参数）。返回类型必须是已注册元数据的实体类；列表/分页返回会打警告并直查 DB。
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CachedQuery {

    /** 自定义 TTL（秒），&le;0 使用全局或实体级配置 */
    long ttl() default -1;

    /** SpEL 条件表达式，返回 false 时直查 DB 不走缓存；可用方法参数名引用参数 */
    String condition() default "";

    /** 是否缓存 null 结果（防穿透）；null 使用独立的短 TTL */
    boolean cacheNull() default true;
}
