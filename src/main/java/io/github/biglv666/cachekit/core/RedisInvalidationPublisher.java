package io.github.biglv666.cachekit.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

/**
 * Redis 失效发布器：向广播 topic 发送完整缓存键。
 */
public class RedisInvalidationPublisher implements InvalidationPublisher {

    private static final Logger log = LoggerFactory.getLogger(RedisInvalidationPublisher.class);

    private final StringRedisTemplate template;
    private final String topic;

    public RedisInvalidationPublisher(StringRedisTemplate template, String topic) {
        this.template = template;
        this.topic = topic;
    }

    @Override
    public void publish(String key) {
        try {
            template.convertAndSend(topic, key);
        } catch (Exception e) {
            // 广播失败不阻断业务：L2 已删，残余脏数据由延迟双删与 TTL 上界兜底
            log.warn("失效广播发送失败，key={}", key, e);
        }
    }

    @Override
    public void publishAll(Collection<String> keys) {
        if (keys.isEmpty()) {
            return;
        }
        try {
            // 管道化 PUBLISH：批量失效（binlog 行事件风暴）时把 N 次往返压成 1 次
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            template.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) connection -> {
                for (String key : keys) {
                    connection.publish(topicBytes, key.getBytes(StandardCharsets.UTF_8));
                }
                return null;
            });
        } catch (Exception e) {
            // 批量发送失败退回逐条（同样失败仅告警）：L2 已删，残余脏数据由双删与 TTL 上界兜底
            log.warn("失效广播批量发送失败，回退逐条发送（{} 条）", keys.size(), e);
            for (String key : keys) {
                publish(key);
            }
        }
    }
}
