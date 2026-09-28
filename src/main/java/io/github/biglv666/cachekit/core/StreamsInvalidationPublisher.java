package io.github.biglv666.cachekit.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

/**
 * Redis Streams 失效发布器：向 Stream XADD 缓存键（{@code MAXLEN ~ N} 近似裁剪防无限增长）。
 *
 * <p>与 pub/sub 的区别：消费组 ACK，实例短暂掉线（进程重启窗口内、GC 停顿、网络抖动）
 * 不丢失效——每实例独立消费组全量消费，掉线期间的条目保留在组内待读。
 * 代价是 Redis 侧多一份 Stream 数据（受 MAXLEN 上界约束）。</p>
 */
public class StreamsInvalidationPublisher implements InvalidationPublisher {

    private static final Logger log = LoggerFactory.getLogger(StreamsInvalidationPublisher.class);

    /** Stream 键（复用广播 topic 配置） */
    private final String stream;
    private final StringRedisTemplate template;
    /** 近似裁剪上界：超过后旧条目可被淘汰（消费组落后超过该量时由 L1 TTL 兜底） */
    private final int maxlen;

    public StreamsInvalidationPublisher(StringRedisTemplate template, String stream, int maxlen) {
        this.template = template;
        this.stream = stream;
        this.maxlen = maxlen;
    }

    private void xAdd(byte[] streamBytes, byte[] keyBytes) {
        template.execute((org.springframework.data.redis.core.RedisCallback<Object>) connection ->
                connection.streamCommands().xAdd(
                        MapRecord.create(streamBytes, Map.of("k".getBytes(StandardCharsets.UTF_8), keyBytes)),
                        XAddOptions.maxlen(maxlen).approximateTrimming(true)));
    }

    @Override
    public void publish(String key) {
        try {
            xAdd(stream.getBytes(StandardCharsets.UTF_8), key.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            // 广播失败不阻断业务：L2 已删，残余脏数据由延迟双删与 TTL 上界兜底
            log.warn("失效广播 XADD 失败，key={}", key, e);
        }
    }

    @Override
    public void publishAll(Collection<String> keys) {
        if (keys.isEmpty()) {
            return;
        }
        try {
            byte[] streamBytes = stream.getBytes(StandardCharsets.UTF_8);
            // 管道化 XADD：批量失效（binlog 行事件风暴）时把 N 次往返压成 1 次
            template.executePipelined((org.springframework.data.redis.core.RedisCallback<Object>) connection -> {
                for (String key : keys) {
                    connection.streamCommands().xAdd(
                            MapRecord.create(streamBytes,
                                    Map.of("k".getBytes(StandardCharsets.UTF_8),
                                            key.getBytes(StandardCharsets.UTF_8))),
                            XAddOptions.maxlen(maxlen).approximateTrimming(true));
                }
                return null;
            });
        } catch (Exception e) {
            log.warn("失效广播批量 XADD 失败，回退逐条发送（{} 条）", keys.size(), e);
            for (String key : keys) {
                publish(key);
            }
        }
    }
}
