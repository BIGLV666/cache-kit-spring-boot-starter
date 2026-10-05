package io.github.biglv666.cachekit.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记实体主键字段（非 MyBatis-Plus 项目的元数据来源）。
 *
 * <p>解析优先级：{@code @CacheId} → MP {@code @TableId} → 名为 {@code id} 的字段。
 * 标注多个 @CacheId 组成复合主键（0.3.3+，声明顺序 join ':'；仅注解查询/手动句柄路径，
 * MyBatis-Plus 自动切面不支持）。</p>
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheId {
}
