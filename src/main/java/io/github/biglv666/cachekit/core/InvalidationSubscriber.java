package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

import java.nio.charset.StandardCharsets;

/**
 * 失效订阅器（pub/sub）：收到广播后清除本实例的 L1（发布方已负责删除 L2）。
 * 核心校验/失效逻辑委托 {@link BroadcastApplier}，与 Streams 消费者共用。
 * pub/sub 不保证送达，L1 短 TTL 是丢消息时的最终兜底。
 */
public class InvalidationSubscriber implements MessageListener {

    private final BroadcastApplier applier;

    public InvalidationSubscriber(CaffeineChannel l1, EntityMetadataRegistry registry) {
        this(l1, registry, "");
    }

    public InvalidationSubscriber(CaffeineChannel l1, EntityMetadataRegistry registry, String namespace) {
        this.applier = new BroadcastApplier(l1, registry, namespace);
    }

    /** 挂载指标监听器 */
    public void setMetricsListener(CacheMetricsListener metrics) {
        applier.setMetricsListener(metrics);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        applier.apply(new String(message.getBody(), StandardCharsets.UTF_8));
    }
}
