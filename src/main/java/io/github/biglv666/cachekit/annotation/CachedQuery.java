package io.github.biglv666.cachekit.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 查询方法注解：命中缓存直接返回，未命中才执行方法体（查 DB）并回填两级缓存。
 *
 * <p>主键严格推导（单值）：实体实例参数、或标量参数且参数名（MyBatis {@code @Param} 值
 * 或 Java 参数名）与主键字段同名；推导不出（如条件字段查询）会打警告并直查 DB，
 * 绝不猜测——猜测会把条件值当主键回填错键。
 *
 * <p>列表：返回 {@code List<实体>} 且参数中有唯一集合参数时按 ID 拆解缓存（selectBatchIds 语义），
 * 并开启严格守卫——回源结果主键与请求值对不上（集合实为条件值，如手机号列表）时判定误用，
 * 警告 + 直查 DB 不缓存。其他返回类型（DTO/分页等）打一次警告后直查 DB。
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
