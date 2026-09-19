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
 * 主键来源：参数中的实体实例（如 updateById(User)）读其主键字段；
 * 实体集合参数（如 updateBatch(List&lt;User&gt;)）逐元素收集、一次批量失效；
 * 标量参数需参数名与主键字段同名——标量集合不收（无法证明是主键，绝不猜测）。
 * MP 项目的 BaseMapper/IService 批量写无需本注解（自动切面覆盖）。</p>
 *
 * <p>事务感知：方法处于活动事务时失效延迟到 afterCommit（{@code cache-kit.tx.evict-after-commit}
 * 默认开），事务回滚不失效；同事务内先写后读同键会读到缓存里的提交前旧值，
 * 需要同事务立即可见时用 {@code CacheKit.withDb} 旁路读取。</p>
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
