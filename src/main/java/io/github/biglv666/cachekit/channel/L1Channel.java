package io.github.biglv666.cachekit.channel;

/**
 * L1 本地缓存通道 SPI：实现该接口并注册为 Spring Bean 即可替换默认的
 * {@link CaffeineChannel}（容量/TTL 治理由实现自行承担，键语义与 {@link CacheChannel} 一致）。
 *
 * <p>异步语义约束：实现必须是进程内本地缓存——失效广播只对本实例的 L1 键执行删除，
 * 远端存储实现（把 L1 做成另一个 Redis）会导致广播语义失效，不要这样做。</p>
 */
public interface L1Channel extends CacheChannel {

    /**
     * 当前条目数估计值（L1 淘汰异步进行，非精确值）；实现不支持时返回 -1。
     * 供运维端点展示，不影响缓存语义。
     */
    default long estimatedSize() {
        return -1;
    }
}
