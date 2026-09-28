package io.github.biglv666.cachekit.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 启动预热：标记一个 Spring Bean 方法，应用就绪后在后台线程自动执行一次。
 *
 * <p>方法体内正常走缓存路径（mapper 查询、{@code EntityCache.get}、{@code @CachedQuery} 方法），
 * 缓存回填由既有链路完成——本注解只负责"启动后执行一次"，不含任何额外缓存语义。
 * 典型用法：把热点数据的首批查询写在一个无参方法里。</p>
 *
 * <pre>{@code
 * @Component
 * static class HotDataWarmup {
 *     @CacheWarmup
 *     void loadHotUsers() {
 *         userMapper.selectBatchIds(List.of(1L, 2L, 3L));
 *     }
 * }
 * }</pre>
 *
 * <p>多个预热方法按 {@link #order()} 升序执行（同序按 Bean 定义顺序），默认并行度 1（顺序执行）；
 * 单个方法抛异常只告警，不影响其他预热与启动。可用 {@code cache-kit.warmup.enabled=false} 关闭
 * （默认开），{@code cache-kit.warmup.parallelism} 控制并发度。</p>
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheWarmup {

    /** 执行顺序（升序，小者先执行），同序按 Bean 定义顺序 */
    int order() default 0;
}
