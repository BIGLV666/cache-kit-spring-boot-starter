package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

import java.nio.charset.StandardCharsets;

/**
 * 失效订阅器：收到广播后清除本实例的 L1（发布方已负责删除 L2）。
 * pub/sub 不保证送达，L1 短 TTL 是丢消息时的最终兜底。
 */
public class InvalidationSubscriber implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(InvalidationSubscriber.class);

    private final CaffeineChannel l1;

    public InvalidationSubscriber(CaffeineChannel l1) {
        this.l1 = l1;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String key = new String(message.getBody(), StandardCharsets.UTF_8);
        if (key == null || key.isBlank()) {
            return;
        }
        l1.evict(key);
        log.debug("收到失效广播，已清除本地 L1: {}", key);
    }
}
