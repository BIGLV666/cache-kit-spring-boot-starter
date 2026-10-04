package io.github.biglv666.cachekit.core;

import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.cluster.pubsub.RedisClusterPubSubAdapter;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.github.biglv666.cachekit.exception.CacheKitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * sharded pub/sub（Redis 7.0+ SSUBSCRIBE）失效订阅器：以独立消费方身份订阅分片频道，
 * 消息经 {@link BroadcastApplier} 校验后清除本实例 L1，与 pub/sub、streams 两种通道
 * 语义完全一致。Cluster 模式下 SSUBSCRIBE 只连 topic 所属分片节点（替代全节点订阅，
 * 连接开销 O(节点数) → O(1)）；单机/主从模式下与普通 pub/sub 等价。
 *
 * <p>实现基于 Lettuce 原生 pub/sub 连接（spring-data-redis 未暴露 SSUBSCRIBE）：
 * 仅支持 Lettuce 客户端（装配处已校验）。订阅在 start() 一次性建立；此后每 30s 幂等
 * 重发一次 SSUBSCRIBE（重复订阅无副作用），覆盖 Lettuce 重连后订阅状态未恢复的窗口。</p>
 *
 * <p>回退链：start() 订阅失败（Redis &lt; 7.0 的 unknown command、非 Lettuce、连接异常）
 * 时告警并回退普通 pub/sub 容器——广播是加速器不是真相源，回退只损失 Cluster 下的
 * 分片开销优化，不损失失效能力。回退后不再自动切回（避免抖动）。</p>
 */
public class ShardedInvalidationSubscriber implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ShardedInvalidationSubscriber.class);

    /** 幂等重订阅周期（秒）：覆盖 Lettuce 重连后订阅状态未恢复的窗口 */
    private static final int RESUBSCRIBE_SECONDS = 30;

    private final LettuceConnectionFactory factory;
    private final BroadcastApplier applier;
    private final String topic;
    private final byte[] topicBytes;

    private volatile StatefulRedisPubSubConnection<byte[], byte[]> standaloneConn;
    private volatile StatefulRedisClusterPubSubConnection<byte[], byte[]> clusterConn;
    private volatile RedisMessageListenerContainer fallbackContainer;
    private volatile ScheduledExecutorService resubscriber;
    private volatile boolean running;

    public ShardedInvalidationSubscriber(LettuceConnectionFactory factory, BroadcastApplier applier, String topic) {
        this.factory = factory;
        this.applier = applier;
        this.topic = topic;
        this.topicBytes = topic.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        try {
            subscribeSharded();
            AtomicInteger seq = new AtomicInteger();
            resubscriber = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "cache-kit-sharded-resubscribe-" + seq.getAndIncrement());
                t.setDaemon(true);
                return t;
            });
            resubscriber.scheduleWithFixedDelay(this::resubscribe,
                    RESUBSCRIBE_SECONDS, RESUBSCRIBE_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("sharded pub/sub 订阅失败，回退普通 pub/sub 模式（Cluster 部署需 Redis 7.0+ 与 Lettuce 客户端）: {}",
                    e.getMessage());
            closeShardedQuietly();
            startFallback();
        }
    }

    /** 建立 sharded 订阅：cluster 与 standalone 两类客户端分别走对应的原生 pub/sub 连接 */
    private void subscribeSharded() {
        io.lettuce.core.AbstractRedisClient client = factory.getNativeClient();
        if (client instanceof RedisClusterClient clusterClient) {
            clusterConn = clusterClient.connectPubSub(ByteArrayCodec.INSTANCE);
            clusterConn.addListener(new RedisClusterPubSubAdapter<>() {
                @Override
                public void smessage(RedisClusterNode node, byte[] channel, byte[] message) {
                    apply(channel, message);
                }
            });
            // 集群连接按 channel 首键路由到分片属主节点（Lettuce 集群命令的通槽路由机制）
            clusterConn.sync().ssubscribe(topicBytes);
        } else if (client instanceof RedisClient redisClient) {
            standaloneConn = redisClient.connectPubSub(ByteArrayCodec.INSTANCE);
            standaloneConn.addListener(new RedisPubSubAdapter<>() {
                @Override
                public void smessage(byte[] channel, byte[] message) {
                    apply(channel, message);
                }
            });
            standaloneConn.sync().ssubscribe(topicBytes);
        } else {
            throw new CacheKitException("无法识别的 Lettuce 客户端类型: " + client.getClass().getName());
        }
        log.info("cache-kit 失效广播已启用 sharded pub/sub 模式（topic='{}'）", topic);
    }

    /** 幂等重发 SSUBSCRIBE：重复订阅无副作用；失败静默（下个周期重试，发布侧不依赖单次送达） */
    private void resubscribe() {
        try {
            if (clusterConn != null) {
                clusterConn.sync().ssubscribe(topicBytes);
            } else if (standaloneConn != null) {
                standaloneConn.sync().ssubscribe(topicBytes);
            }
        } catch (Exception ignored) {
            // 连接抖动：下个周期重试
        }
    }

    private void apply(byte[] channel, byte[] message) {
        applier.apply(new String(message, StandardCharsets.UTF_8));
    }

    /** 回退普通 pub/sub：复用同一个 BroadcastApplier，校验/失效/指标语义完全一致 */
    private void startFallback() {
        try {
            RedisMessageListenerContainer container = new RedisMessageListenerContainer();
            container.setConnectionFactory(factory);
            container.addMessageListener(this::onPubSubMessage, new ChannelTopic(topic));
            container.afterPropertiesSet();
            container.start();
            fallbackContainer = container;
        } catch (Exception e) {
            log.warn("sharded pub/sub 回退 pub/sub 也失败（Redis 不可用？），实例停用失效广播，等待下次发布轮重试", e);
        }
    }

    private void onPubSubMessage(Message message, byte[] pattern) {
        applier.apply(new String(message.getBody(), StandardCharsets.UTF_8));
    }

    @Override
    public synchronized void stop() {
        running = false;
        ScheduledExecutorService r = resubscriber;
        if (r != null) {
            r.shutdownNow();
            resubscriber = null;
        }
        closeShardedQuietly();
        RedisMessageListenerContainer container = fallbackContainer;
        if (container != null) {
            fallbackContainer = null;
            try {
                container.stop();
                container.destroy();
            } catch (Exception ignored) {
                // 关闭失败无需处理
            }
        }
    }

    private void closeShardedQuietly() {
        StatefulRedisPubSubConnection<byte[], byte[]> s = standaloneConn;
        standaloneConn = null;
        if (s != null) {
            try {
                s.close();
            } catch (Exception ignored) {
            }
        }
        StatefulRedisClusterPubSubConnection<byte[], byte[]> c = clusterConn;
        clusterConn = null;
        if (c != null) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** 当前是否处于 sharded 模式（false 表示已回退 pub/sub），端点展示用 */
    public boolean isShardedActive() {
        return standaloneConn != null || clusterConn != null;
    }
}
