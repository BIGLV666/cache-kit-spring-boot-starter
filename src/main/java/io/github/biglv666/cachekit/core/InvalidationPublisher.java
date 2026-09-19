package io.github.biglv666.cachekit.core;

import java.util.Collection;

/**
 * 失效广播发布器：写路径删除缓存后通知集群内其他实例清除各自的 L1。
 * 未启用 Redis 时使用 {@link NoopInvalidationPublisher}。
 */
public interface InvalidationPublisher {

    /** 发布失效消息，payload 为完整缓存键 */
    void publish(String key);

    /** 批量发布失效消息（Redis 实现走管道化），默认逐条委托 {@link #publish} */
    default void publishAll(Collection<String> keys) {
        for (String key : keys) {
            publish(key);
        }
    }
}
