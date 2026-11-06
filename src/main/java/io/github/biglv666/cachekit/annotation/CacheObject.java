package io.github.biglv666.cachekit.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记可被通用缓存（任意键值）缓存的<b>值类型</b>。
 *
 * <p>与实体缓存（{@link CacheEntity}）的区别：通用缓存不要求主键、不依赖表名/binlog，
 * 缓存键由 {@link CacheLoad#key()} 的 SpEL 或 {@code CacheStore} 句柄显式给出，
 * 值的更新/删除时机由业务通过句柄控制。</p>
 *
 * <p>缓存键格式为 {@code [自定义段:]namespace:__gk__:prefix:用户key}；同一 {@code prefix}
 * 只能绑定一个值类型（启动期/首次注册时 fail-fast），否则不同类型会互相反序列化成错数据。</p>
 *
 * <p>值类型必须是 Jackson 可实例化的具体类。标注在<b>接口或抽象类</b>上时，
 * 除非类型带 {@code @com.fasterxml.jackson.annotation.JsonTypeInfo} 多态信息，
 * 否则启动/首次注册即 fail-fast——否则会写入成功但每次读取都解码失败，表现为永远 miss。</p>
 *
 * <pre>{@code
 * @CacheObject(prefix = "app-config", ttl = 300)
 * public class AppConfig { ... }
 * }</pre>
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheObject {

    /** 缓存键前缀，默认类名转蛇形（与实体缓存默认表名规则一致）；不能为空或包含 ':' */
    String prefix() default "";

    /** 默认 L2 TTL（秒），≤0 使用全局 cache-kit.l2.ttl */
    long ttl() default -1;

    /** 是否缓存 null 结果（防穿透）；方法级 {@link CacheLoad#cacheNull()} 可进一步关闭 */
    boolean cacheNull() default true;
}
