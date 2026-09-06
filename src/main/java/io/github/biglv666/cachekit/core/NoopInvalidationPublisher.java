package io.github.biglv666.cachekit.core;

/**
 * 空发布器：单实例或未启用广播时使用。
 */
public class NoopInvalidationPublisher implements InvalidationPublisher {

    @Override
    public void publish(String key) {
        // no-op
    }
}
