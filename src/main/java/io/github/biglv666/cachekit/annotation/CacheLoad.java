package io.github.biglv666.cachekit.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 通用缓存读取/回填入口：命中直接返回缓存值，未命中才执行方法体（用户自定义回填逻辑），
 * 并逐级写入 L1/L2。
 *
 * <p>与 {@link CachedQuery} 的区别：不要求返回实体、不推导主键，缓存键完全由
 * {@link #key()} 的 SpEL 决定，可缓存任意可 JSON 序列化的值（DTO、配置对象、
 * {@code List<T>}、{@code Map<K,V>} 等）。</p>
 *
 * <p>值类型解析：方法返回类型直接标注 {@link CacheObject}，或返回
 * {@code List<T>}/{@code Set<T>}/{@code Map<K,V>}/数组且泛型参数中
 * 有唯一标注 {@link CacheObject} 的类型；无法解析时启动/首次调用 fail-fast，
 * 可用 {@link #value()} 显式指定元数据来源类型。反序列化始终按方法完整泛型返回类型进行。</p>
 *
 * <p>泛型按<b>实际实现类</b>解析：切面用 {@code ResolvableType.forMethodReturnType(method, targetClass)}
 * 替换方法返回类型中的类型变量，{@code T}、{@code List<T>}、{@code Map<K,V>}、{@code T[]}
 * 均按实现类实参展开（如 {@code Service extends Base<AppConfig>} 中的 {@code T} → {@code AppConfig}）。
 * 仅未解析位置（顶层 {@code Object}、未绑定类型变量、无界通配符）回退到 {@link #value()}；
 * 裸 {@code List}/{@code Set} 用 {@code value} 作元素类型，裸 {@code Map}/{@code Map<?, V>}
 * 的键按 {@code String} 处理。</p>
 *
 * <p>注解位置：可标在接口方法或泛型父类方法上，切面按目标类回溯查找
 * （方法 → 具体重写方法 → 实现接口 → 父类），实现类无需重复标注；
 * 自调用不经过代理，与 Spring AOP 一致。</p>
 *
 * <p>异常语义：方法声明的受检异常原样传播（single-flight 等待者收到同一异常实例），
 * 异常结果不写入缓存。</p>
 *
 * <pre>{@code
 * @CacheLoad(key = "#tenant + ':' + #name")
 * public AppConfig load(String tenant, String name) { ... }
 * }</pre>
 *
 * <p>生命周期：更新/删除通过 {@code CacheStore} 句柄或 TTL 控制；通用缓存不接 binlog，
 * 框架不会因为数据库写入自动失效任意派生值。</p>
 */
@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheLoad {

    /**
     * 缓存键 SpEL 表达式（必填），结果转为字符串；可用参数名引用方法参数，
     * 例如 {@code "#userId"}、{@code "#tenant + ':' + #name"}。
     * 结果为空/null 视为配置错误（抛异常，不猜测 key）。
     */
    String key();

    /**
     * 元数据来源类型（可选）：方法返回类型本身或泛型参数无法解析出 {@link CacheObject} 时，
     * 用它指定 prefix/TTL/cacheNull 的来源；同时作为反序列化时<b>未解析位置</b>
     * （顶层 {@code Object}、未绑定类型变量、无界通配符、裸 {@code List}/{@code Set} 元素、
     * 裸 {@code Map} 的值）的兜底类型。已解析的泛型位置不受影响，
     * 显式声明的嵌套 {@code Object}（如 {@code Map<String, Object>}）也保持不变。
     */
    Class<?> value() default void.class;

    /** 本次读取的 L2 TTL 覆盖（秒），≤0 使用 {@link CacheObject#ttl()} 或全局配置 */
    long ttl() default -1;

    /** SpEL 条件表达式，返回 false 时直查方法体（不走缓存）；可用方法参数名引用参数 */
    String condition() default "";

    /** 是否缓存 null 结果；与方法所属 {@link CacheObject#cacheNull()} 同时为 true 才缓存 */
    boolean cacheNull() default true;
}
