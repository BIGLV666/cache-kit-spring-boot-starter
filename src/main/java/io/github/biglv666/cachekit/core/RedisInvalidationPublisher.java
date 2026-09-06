package io.github.biglv666.cachekit.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

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
}
