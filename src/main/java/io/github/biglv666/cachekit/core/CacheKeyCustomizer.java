package io.github.biglv666.cachekit.core;

/**
 * 缓存键自定义段 SPI：多数据源 / 多租户隔离。
 *
 * <p>实现 Bean 返回的段会被拼在缓存键最前面（{@code 段:表名:主键}），
 * 典型实现从租户上下文 ThreadLocal 读取当前租户/数据源标识。
 * 不同段的键互不可见；注册多个 Bean 时按未定义顺序全部拼接。</p>
 */
public interface CacheKeyCustomizer {

    /**
     * @return 当前上下文的键段（如租户 ID）；null 或空白表示本次调用不附加段
     */
    String segment();
}
