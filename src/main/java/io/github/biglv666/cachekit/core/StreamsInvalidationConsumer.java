package io.github.biglv666.cachekit.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamInfo;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.data.redis.connection.stream.StreamInfo;

/**
 * Redis Streams 失效消费者（每实例独立消费组）：XREADGROUP 全量消费广播 Stream，
 * 逐条应用失效后 XACK——消费组 ACK 语义使实例短暂掉线（重启/网络抖动）不丢失效，
 * 掉线期间的条目保留在组内待读，恢复后补投（应用失效是幂等 DEL）。
 *
 * <p>位点策略：新组从 {@code $}（最新）开始——实例（重）启动时 L1 为空，
 * 不需要历史失效消息；L2 的删除由写方直接 DEL 承担，与广播通道无关。</p>
 *
 * <p>悬空组清理：组名内嵌进程启动纪元 {@code g-<epoch>-<uuid>}，实例正常关闭时自毁；
 * 崩溃残留的组由存活实例周期性清理（纪元超过 30 分钟且组内消费者全部闲置超过 10 分钟——
 * 健康消费者的闲置时间不超过 BLOCK 时长 5 秒，不会误杀）。</p>
 *
 * <p>降级语义与 L2 一致：Redis 异常时限频告警、退避重试，绝不阻断业务读写。</p>
 */
public class StreamsInvalidationConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(StreamsInvalidationConsumer.class);

    /** XREADGROUP 单次最多消费条数 */
    private static final int BATCH = 64;
    /** 无消息时阻塞等待时长（毫秒）：兼顾送达延迟与 Redis 空转 */
    private static final long BLOCK_MILLIS = 5_000;
    /** Redis 异常后的重试退避（毫秒） */
    private static final long RETRY_BACKOFF_MILLIS = 3_000;
    /** 悬空组清理扫描间隔（毫秒） */
    private static final long SWEEP_INTERVAL_MILLIS = 300_000;
    /** 滞后量测节流间隔（毫秒）：避免每轮 BLOCK 空转时频繁 XINFO */
    private static final long LAG_COMPUTE_INTERVAL_MILLIS = 2_000;
    /** 组纪元早于当前时间该毫秒数才允许清理（新组豁免） */
    private static final long GROUP_EPOCH_GRACE_MILLIS = 1_800_000L;
    /** 消费者闲置超过该毫秒数视为死亡（健康消费者闲置至多 BLOCK_MILLIS） */
    private static final long CONSUMER_IDLE_DEATH_MILLIS = 600_000L;

    private final StringRedisTemplate template;
    private final BroadcastApplier applier;
    private final String stream;
    private final String group;
    private final String consumer;
    private final AtomicLong lastWarnAt = new AtomicLong();
    private volatile boolean running;
    private Thread worker;
    private long lastLagComputeAt;
    private volatile CacheMetricsListener metrics = new CacheMetricsListener() {
    };

    /** 挂载指标监听器（消费组滞后 gauge 数据源） */
    public void setMetricsListener(CacheMetricsListener metrics) {
        this.metrics = metrics == null ? new CacheMetricsListener() {
        } : metrics;
    }

    public StreamsInvalidationConsumer(StringRedisTemplate template, BroadcastApplier applier, String stream) {
        this.template = template;
        this.applier = applier;
        this.stream = stream;
        long epoch = System.currentTimeMillis();
        this.group = "g-" + epoch + "-" + UUID.randomUUID().toString().substring(0, 8);
        this.consumer = "c-" + epoch;
    }

    /** 供测试断言组名格式 */
    String groupName() {
        return group;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        worker = new Thread(this::loop, "cache-kit-streams-invalidate");
        worker.setDaemon(true);
        worker.start();
    }

    @Override
    public synchronized void stop() {
        running = false;
        Thread w = worker;
        if (w != null) {
            w.interrupt();
        }
        // 优雅关闭：自毁消费组，避免 Redis 侧残留悬空组
        try {
            template.execute((org.springframework.data.redis.core.RedisCallback<Object>) connection ->
                    connection.streamCommands().xGroupDestroy(
                            stream.getBytes(StandardCharsets.UTF_8), group));
        } catch (Exception ignored) {
            // 已无法连接 Redis：残留组由其他实例的清理扫描回收
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void loop() {
        ensureGroup();
        long lastSweep = 0;
        while (running) {
            try {
                List<MapRecord<String, Object, Object>> records = readBatch();
                if (records != null) {
                    for (MapRecord<String, Object, Object> record : records) {
                        apply(record);
                    }
                    ack(records);
                }
                long now = System.currentTimeMillis();
                if (now - lastLagComputeAt >= LAG_COMPUTE_INTERVAL_MILLIS) {
                    lastLagComputeAt = now;
                    computeAndReportLag();
                }
                if (now - lastSweep > SWEEP_INTERVAL_MILLIS) {
                    lastSweep = now;
                    sweepStaleGroups();
                }
            } catch (Exception e) {
                warnRateLimited(e);
                if (!running) {
                    return;
                }
                sleep(RETRY_BACKOFF_MILLIS);
            }
        }
    }

    /**
     * 滞后量测（节流 {@value LAG_COMPUTE_INTERVAL_MILLIS}ms）：
     * 本组已读到事件（lastDeliveredId）与 Stream 最新事件（lastGeneratedId）的时间戳差。
     * lastDelivered 为 null/"0-0"（组从未读到）时跳过——不更新 gauge，避免误报"已追平"。
     */
    private void computeAndReportLag() {
        try {
            String lastGenerated = template.opsForStream().info(stream).lastGeneratedId();
            String lastDelivered = null;
            for (StreamInfo.XInfoGroup g : template.opsForStream().groups(stream)) {
                if (group.equals(g.groupName())) {
                    lastDelivered = g.lastDeliveredId();
                    break;
                }
            }
            if (lastGenerated == null || lastDelivered == null || lastDelivered.startsWith("0-")) {
                return;
            }
            long lag = parseStreamIdMillis(lastGenerated) - parseStreamIdMillis(lastDelivered);
            if (lag >= 0) {
                metrics.streamsLagSeconds(lag / 1000);
            }
        } catch (Exception e) {
            debugOrWarn("消费组滞后计算失败（下轮重试）", e);
        }
    }

    /** Stream ID "millis-seq" 的毫秒段 */
    private static long parseStreamIdMillis(String recordId) {
        int dash = recordId.indexOf('-');
        try {
            return dash > 0 ? Long.parseLong(recordId.substring(0, dash)) : Long.parseLong(recordId);
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    /** 创建消费组（MKSTREAM 顺带建空 Stream）；已存在则忽略 BUSYGROUP */
    private void ensureGroup() {
        try {
            template.execute((org.springframework.data.redis.core.RedisCallback<Object>) connection ->
                    connection.streamCommands().xGroupCreate(
                            stream.getBytes(StandardCharsets.UTF_8), group,
                            org.springframework.data.redis.connection.stream.ReadOffset.latest(), true));
        } catch (Exception e) {
            debugOrWarn("创建消费组失败（BUSYGROUP 等场景可继续）", e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<MapRecord<String, Object, Object>> readBatch() {
        List<?> records = template.opsForStream().read(
                org.springframework.data.redis.connection.stream.Consumer.from(group, consumer),
                org.springframework.data.redis.connection.stream.StreamReadOptions.empty()
                        .count(BATCH)
                        .block(Duration.ofMillis(BLOCK_MILLIS)),
                org.springframework.data.redis.connection.stream.StreamOffset.create(stream,
                        org.springframework.data.redis.connection.stream.ReadOffset.lastConsumed()));
        return (List<MapRecord<String, Object, Object>>) records;
    }

    @SuppressWarnings("unchecked")
    private void apply(MapRecord<String, Object, Object> record) {
        Object key = record.getValue().get("k");
        if (key instanceof String s) {
            applier.apply(s);
        }
    }

    private void ack(List<MapRecord<String, Object, Object>> records) {
        try {
            template.opsForStream().acknowledge(stream, group,
                    records.stream().map(MapRecord::getId).toArray(org.springframework.data.redis.connection.stream.RecordId[]::new));
        } catch (Exception e) {
            // ACK 失败：条目留在 PEL，应用失效是幂等 DEL，重投无害
            warnRateLimited(e);
        }
    }

    /**
     * 清理崩溃实例残留的悬空消费组：组纪元超过 30 分钟且组内无活跃消费者（全部闲置
     * 超过 10 分钟或没有消费者）时销毁。健康实例的消费者每轮 BLOCK 至多闲置 5 秒，不会命中。
     */
    private void sweepStaleGroups() {
        try {
            long now = System.currentTimeMillis();
            for (StreamInfo.XInfoGroup g : template.opsForStream().groups(stream)) {
                String name = g.groupName();
                if (!isOwnGroupName(name) || !isStaleEpoch(name, now)) {
                    continue;
                }
                boolean alive = false;
                for (StreamInfo.XInfoConsumer c : template.opsForStream().consumers(stream, name)) {
                    if (c.idleTimeMs() < CONSUMER_IDLE_DEATH_MILLIS) {
                        alive = true;
                        break;
                    }
                }
                if (!alive) {
                    log.info("清理崩溃实例残留的失效广播消费组（纪元过期且无活跃消费者）: {}", name);
                    template.execute((org.springframework.data.redis.core.RedisCallback<Object>) connection ->
                            connection.streamCommands().xGroupDestroy(
                                    stream.getBytes(StandardCharsets.UTF_8), name));
                }
            }
        } catch (Exception e) {
            debugOrWarn("消费组清理扫描失败（下轮重试）", e);
        }
    }

    private static boolean isOwnGroupName(String name) {
        return name.startsWith("g-");
    }

    private static boolean isStaleEpoch(String groupName, long now) {
        int end = groupName.indexOf('-', 2);
        if (end < 0) {
            return false;
        }
        try {
            return now - Long.parseLong(groupName.substring(2, end)) > GROUP_EPOCH_GRACE_MILLIS;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private void warnRateLimited(Exception e) {
        long now = System.nanoTime();
        long prev = lastWarnAt.get();
        if (now - prev > 30_000_000_000L && lastWarnAt.compareAndSet(prev, now)) {
            log.warn("失效广播消费异常，退避重试（本告警 30s 内不重复）: {}", e.getMessage());
        }
    }

    private void debugOrWarn(String msg, Exception e) {
        log.debug(msg, e);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
