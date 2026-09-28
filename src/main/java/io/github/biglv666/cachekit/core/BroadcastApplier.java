package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CacheChannel;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 广播失效应用器：校验广播键归属（实体前缀 + namespace）后清除本地 L1。
 * pub/sub 订阅器与 Streams 消费者共用，保证两种通道语义完全一致。
 *
 * <p>安全边界：只清除"键前缀能匹配到已解析实体元数据"的键——
 * 防止能连上 Redis 的任意客户端向 topic/stream 发消息清空任意缓存键（缓存 DoS 面）。
 * 尚未解析的实体本就没有缓存条目，其消息被忽略是无害的。</p>
 */
public class BroadcastApplier {

    private static final Logger log = LoggerFactory.getLogger(BroadcastApplier.class);

    private final CacheChannel l1;
    private final EntityMetadataRegistry registry;
    private final String namespace;
    private volatile CacheMetricsListener metrics = new CacheMetricsListener() {
    };

    public BroadcastApplier(CacheChannel l1, EntityMetadataRegistry registry, String namespace) {
        this.l1 = l1;
        this.registry = registry;
        this.namespace = namespace == null ? "" : namespace;
    }

    /** 挂载指标监听器 */
    public void setMetricsListener(CacheMetricsListener metrics) {
        this.metrics = metrics == null ? new CacheMetricsListener() {
        } : metrics;
    }

    /**
     * 应用一条广播失效消息。
     *
     * @param key 完整缓存键
     * @return true 表示已应用（键匹配已知实体并清除 L1）；false 表示被忽略（键无法匹配）
     */
    public boolean apply(String key) {
        if (key == null || key.isBlank()) {
            return false;
        }
        // 键形如 [customizer段:]namespace:前缀:id，主键段可能含 ':'，不能按冒号切分——
        // 由注册表按"实体前缀作为完整段 + namespace 紧邻校验"解析，
        // 与 TieredEntityCache.key() 的组装规则对齐（含 CacheKeyCustomizer 段的键也能匹配）。
        // customizer 段不校验：其他实例/租户的键在本地不存在，删除是无害空操作。
        if (registry.findByBroadcastKey(key, namespace) == null) {
            metrics.broadcastReceived(false);
            log.debug("收到无法匹配已知实体的广播，忽略: {}", key);
            return false;
        }
        metrics.broadcastReceived(true);
        l1.evict(key);
        log.debug("收到失效广播，已清除本地 L1: {}", key);
        return true;
    }
}
