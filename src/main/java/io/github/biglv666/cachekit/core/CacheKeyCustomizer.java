package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.metadata.EntityMetadata;

import java.io.Serializable;
import java.util.Map;

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

    /**
     * binlog 失效场景的键段还原：binlog 行事件在解析线程处理、不经过应用线程，
     * {@link #segment()} 依赖的 ThreadLocal 上下文不可用。实现本方法从行数据还原键段
     * （如行的 tenant_id 列）后，binlog 对该行的失效才能拼出与读路径一致的精确键，
     * 解决"多租户键的 binlog 失效不命中"限制。未覆写（或返回 null）时，该行的
     * binlog 失效按既有语义跳过精确失效（仅 TTL/延迟双删兜底），并计入
     * {@code cache-kit.binlog.derive.skipped} 指标供监控。
     *
     * @param meta    实体元数据
     * @param rowData 该行的全部列值（列名 → 值，列名与 information_schema 一致；
     *                仅当行镜像列数与表列数一致时提供）
     * @return 该行对应的键段；null 或空白表示无法从该行还原
     */
    default String segmentFor(EntityMetadata meta, Map<String, Serializable> rowData) {
        return null;
    }
}
