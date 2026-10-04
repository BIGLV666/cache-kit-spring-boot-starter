package io.github.biglv666.cachekit.core;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.lettuce.LettuceConnection;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * sharded pub/sub（Redis 7.0+ SSUBSCRIBE/SPUBLISH）失效发布器：SPUBLISH 只把失效消息送达
 * topic 所属分片节点，Redis Cluster 下替代"全节点广播"的 PUBLISH，跨槽广播开销从
 * O(节点数) 降为 O(1)。单机/主从模式下与普通 pub/sub 语义相同（无分片）。
 *
 * <p>Lettuce 6.x 未内置 SPUBLISH 命令封装：以自定义 {@link ProtocolKeyword} 走通用
 * dispatch。连接复用宿主工厂的连接池/共享原生连接（不额外维护连接生命周期）；
 * 仅支持 Lettuce 客户端（装配处已校验，非 Lettuce 回退 pub/sub）。</p>
 *
 * <p>失败语义与 {@link RedisInvalidationPublisher} 一致：广播失败不阻断业务，
 * 限频告警（L2 已删，残余脏数据由延迟双删与 TTL 上界兜底）；批量发送失败回退逐条。</p>
 */
public class ShardedInvalidationPublisher implements InvalidationPublisher {

    private static final Logger log = LoggerFactory.getLogger(ShardedInvalidationPublisher.class);

    /** Lettuce 未内置 SPUBLISH：自定义命令字（Redis 7.0+） */
    private static final ProtocolKeyword SPUBLISH = new ProtocolKeyword() {
        @Override
        public byte[] getBytes() {
            return "SPUBLISH".getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public String toString() {
            return "SPUBLISH";
        }
    };

    private static final ByteArrayCodec CODEC = ByteArrayCodec.INSTANCE;
    private static final Duration PIPELINE_AWAIT = Duration.ofSeconds(5);

    private final LettuceConnectionFactory factory;
    private final String topic;
    private final byte[] topicBytes;
    private volatile long lastWarnAt;

    public ShardedInvalidationPublisher(LettuceConnectionFactory factory, String topic) {
        this.factory = factory;
        this.topic = topic;
        this.topicBytes = topic.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void publish(String key) {
        try (RedisConnection connection = factory.getConnection()) {
            // 直取工厂原始连接：StringRedisTemplate 会把连接包装成 DefaultStringRedisConnection
            //（拿不到 Lettuce 原生连接），工厂连接无装饰器、无 CloseSuppressing 代理
            dispatch((LettuceConnection) connection, key.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            // 广播失败不阻断业务：L2 已删，残余脏数据由延迟双删与 TTL 上界兜底
            warn("失效广播 SPUBLISH 失败，key=" + key, e);
        }
    }

    @Override
    public void publishAll(Collection<String> keys) {
        if (keys.isEmpty()) {
            return;
        }
        try (RedisConnection connection = factory.getConnection()) {
            StatefulRedisConnection<byte[], byte[]> stateful = stateful((LettuceConnection) connection);
            // 管道化 SPUBLISH：全部命令出栈后统一 await，N 次往返压成 1 次
            List<AsyncCommand<byte[], byte[], Long>> futures = new ArrayList<>(keys.size());
            for (String key : keys) {
                futures.add(command(key.getBytes(StandardCharsets.UTF_8)));
            }
            stateful.dispatch(new ArrayList<>(futures));
            LettuceFutures.awaitAll(PIPELINE_AWAIT, futures.toArray(new java.util.concurrent.Future[0]));
        } catch (Exception e) {
            warn("失效广播批量 SPUBLISH 失败，回退逐条发送（" + keys.size() + " 条）", e);
            for (String key : keys) {
                publish(key);
            }
        }
    }

    /** 单条同步 dispatch（standalone 走普通连接，cluster 按 topic 首键路由到分片属主节点） */
    private void dispatch(LettuceConnection connection, byte[] keyBytes) {
        AsyncCommand<byte[], byte[], Long> cmd = command(keyBytes);
        stateful(connection).dispatch(cmd);
        LettuceFutures.awaitAll(PIPELINE_AWAIT, cmd);
    }

    /** 组装 SPUBLISH 命令：响应为整数（送达的订阅者数），IntegerOutput 承载（StatusOutput 不支持整数回复） */
    private AsyncCommand<byte[], byte[], Long> command(byte[] keyBytes) {
        return new AsyncCommand<>(new Command<>(SPUBLISH, new IntegerOutput<>(CODEC), args(keyBytes)));
    }

    private CommandArgs<byte[], byte[]> args(byte[] keyBytes) {
        return new CommandArgs<>(CODEC).addKey(topicBytes).addValue(keyBytes);
    }

    /**
     * 从 Spring 的 Lettuce 连接取底层 StatefulRedisConnection：
     * standalone 为 {@link RedisAsyncCommands}、cluster 为 {@link RedisAdvancedClusterAsyncCommands}，
     * 两者经 {@code getStatefulConnection()} 回到状态化连接。集群连接不直接 dispatch
     * （自定义命令的通道路由依赖实现细节），改按 topic 的 hash 槽取属主节点连接，
     * 与 SSUBSCRIBE 落点一致，确定性送达分片节点。
     */
    private StatefulRedisConnection<byte[], byte[]> stateful(LettuceConnection connection) {
        Object nativeConn = connection.getNativeConnection();
        if (nativeConn instanceof RedisAdvancedClusterAsyncCommands) {
            @SuppressWarnings("unchecked")
            StatefulRedisClusterConnection<byte[], byte[]> cluster =
                    ((RedisAdvancedClusterAsyncCommands<byte[], byte[]>) nativeConn).getStatefulConnection();
            return cluster.getConnection(topic);
        }
        @SuppressWarnings("unchecked")
        RedisAsyncCommands<byte[], byte[]> standalone = (RedisAsyncCommands<byte[], byte[]>) nativeConn;
        return standalone.getStatefulConnection();
    }

    private void warn(String message, Exception e) {
        long now = System.nanoTime();
        if (now - lastWarnAt > 30_000_000_000L) {
            lastWarnAt = now;
            log.warn("{}（本告警 30s 内不重复）", message, e);
        }
    }
}
