package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

import java.nio.charset.StandardCharsets;

/**
 * 失效订阅器：收到广播后清除本实例的 L1（发布方已负责删除 L2）。
 * pub/sub 不保证送达，L1 短 TTL 是丢消息时的最终兜底。
 *
 * <p>安全边界：只清除"键前缀能匹配到已解析实体元数据"的键——
 * 防止能连上 Redis 的任意客户端向 topic 发消息清空任意缓存键（缓存 DoS 面）。
 * 尚未解析的实体本就没有缓存条目，其消息被忽略是无害的。</p>
 */
public class InvalidationSubscriber implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(InvalidationSubscriber.class);

    private final CaffeineChannel l1;
    private final EntityMetadataRegistry registry;
    private final String namespace;
    private volatile CacheMetricsListener metrics = new CacheMetricsListener() {
    };

    public InvalidationSubscriber(CaffeineChannel l1, EntityMetadataRegistry registry) {
        this(l1, registry, "");
    }

    public InvalidationSubscriber(CaffeineChannel l1, EntityMetadataRegistry registry, String namespace) {
        this.l1 = l1;
        this.registry = registry;
        this.namespace = namespace == null ? "" : namespace;
    }

    /** 挂载指标监听器 */
    public void setMetricsListener(CacheMetricsListener metrics) {
        this.metrics = metrics == null ? new CacheMetricsListener() {
        } : metrics;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String key = new String(message.getBody(), StandardCharsets.UTF_8);
        if (key == null || key.isBlank()) {
            return;
        }
        // 命名空间段校验（CacheKeyCustomizer 段不校验：其他实例/租户的键在本地不存在，删除是无害空操作）
        String rest = key;
        if (!namespace.isBlank()) {
            if (!rest.startsWith(namespace + ":")) {
                log.debug("收到其他命名空间的广播，忽略: {}", key);
                return;
            }
            rest = rest.substring(namespace.length() + 1);
        }
        int idx = rest.lastIndexOf(':');
        if (idx <= 0 || registry.findByPrefix(rest.substring(0, idx)) == null) {
            metrics.broadcastReceived(false);
            log.debug("收到无法匹配已知实体的广播，忽略: {}", key);
            return;
        }
        metrics.broadcastReceived(true);
        l1.evict(key);
        log.debug("收到失效广播，已清除本地 L1: {}", key);
    }
}
