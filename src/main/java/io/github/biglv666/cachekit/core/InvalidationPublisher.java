package io.github.biglv666.cachekit.core;

/**
 * 失效广播发布器：写路径删除缓存后通知集群内其他实例清除各自的 L1。
 * 未启用 Redis 时使用 {@link NoopInvalidationPublisher}。
 */
public interface InvalidationPublisher {

    /** 发布失效消息，payload 为完整缓存键 */
    void publish(String key);
}
