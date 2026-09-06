package io.github.biglv666.cachekit.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 写方法注解：方法成功返回后失效对应实体的缓存（本地 L1 + Redis L2），
 * 并通过 pub/sub 广播通知其他实例清除各自的 L1，随后按配置执行一次延迟双删。
 *
 * <p>必须与真实写库方法配合使用，顺序为"先写 DB 后删缓存"。
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheInvalidate {

    /**
     * 实体类型。写方法返回值通常是影响行数，无法从返回类型推导实体：
     * 参数中有实体实例（如 updateById(User)）时可省略，标量参数场景必须显式指定。
     */
    Class<?> entity() default void.class;
}
