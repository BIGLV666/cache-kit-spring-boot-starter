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
 * 复合主键不支持。
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CacheId {
}
