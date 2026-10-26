package io.github.biglv666.cachekit.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamInfo;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

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
    /** 消费组重建后的短暂让出（毫秒）：防 Redis 侧组被反复删除时形成紧凑重试循环 */
    private static final long GROUP_RECOVERY_BACKOFF_MILLIS = 100;
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
    private final AtomicLong groupRecreatedCount = new AtomicLong();
    private volatile boolean running;
    /**
     * 消费组是否已就绪：启动时 Redis 不可用、运行中 Redis 重启/FLUSHALL/主从切换导致组丢失时
     * 为 false，下一轮先重建组再读；否则 XREADGROUP 会一直抛 NOGROUP 且永远不会自愈。
     */
    private volatile boolean groupReady;
    /**
     * 本进程是否曾经成功建组：用于区分"首次启动"（从 $ 读，L1 为空不需要历史消息）
     * 与"运行中组丢失"（从 0-0 重放当前保留的 Stream，尽量补投断连窗口内的失效消息）。
     */
    private volatile boolean everGroupReady;
    /** 是否处于"运行中组丢失 → 等待重建"的状态（用于自愈计数，避免启动期正常建组被误计） */
    private volatile boolean recoveringGroup;
    private volatile long lastGroupRecreateWarnAt;
    private Thread worker;
    private long lastLagComputeAt;
    /** 最近一次量测的滞后秒数（-1 = 尚未量测），lastDelivered 为空时不更新，避免误报"已追平" */
    private volatile long lastLagSeconds = -1;
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

    /** 消费组名（内嵌启动纪元，端点展示与测试断言用） */
    public String groupName() {
        return group;
    }

    /** 最近一次量测的消费组滞后（秒）；尚未量测过返回 -1（端点展示用） */
    public long lastLagSeconds() {
        return lastLagSeconds;
    }

    /** 消费组自愈重建次数（端点展示用；对应指标 broadcast.streams.group.recreated） */
    public long groupRecreatedCount() {
        return groupRecreatedCount.get();
    }

    /** 当前消费组是否已就绪（端到端排障用） */
    public boolean isGroupReady() {
        return groupReady;
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
        groupReady = false;
        // 正常停机销毁了消费组：下次启动按首次启动语义从 $ 读，避免无意义地重放整个 Stream
        everGroupReady = false;
        recoveringGroup = false;
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
        long lastSweep = 0;
        while (running) {
            try {
                if (!groupReady) {
                    // 启动时 Redis 不可用、或上一轮读发现组已丢失：先幂等重建
                    ensureGroup();
                }
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
                if (running && isNoGroup(e)) {
                    // 组丢失（Redis 重启/FLUSHALL/主从切换）：XREADGROUP 会永远抛 NOGROUP，
                    // 必须重建组才能恢复；重建成功后继续读，不进入长退避。
                    // 只有"曾经成功建组后丢失"才算重建：启动期首次建组失败（ACL 拒绝 XGROUP CREATE
                    // 等）不应计入 group.recreated，也不该声称从 Stream 重放
                    groupReady = false;
                    recoveringGroup = everGroupReady;
                    if (ensureGroup()) {
                        sleep(GROUP_RECOVERY_BACKOFF_MILLIS);
                        continue;
                    }
                }
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
            StreamInfo.XInfoStream info = template.opsForStream().info(stream);
            if (info == null) {
                // Stream 键不存在（尚未有失效发布，或已被 Redis 淘汰/删除）：无从量测
                return;
            }
            String lastGenerated = info.lastGeneratedId();
            if (lastGenerated == null) {
                return;
            }
            String lastDelivered = null;
            for (StreamInfo.XInfoGroup g : template.opsForStream().groups(stream)) {
                if (group.equals(g.groupName())) {
                    lastDelivered = g.lastDeliveredId();
                    break;
                }
            }
            if (lastDelivered == null || lastDelivered.startsWith("0-")) {
                return;
            }
            long lag = parseStreamIdMillis(lastGenerated) - parseStreamIdMillis(lastDelivered);
            if (lag >= 0) {
                lastLagSeconds = lag / 1000;
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

    /**
     * 创建消费组（MKSTREAM 顺带建空 Stream）；已存在（BUSYGROUP）视为成功。
     *
     * <p>位点选择：首次启动从 {@code $}（最新）开始——实例刚启动时 L1 为空，不需要历史失效；
     * 运行中组丢失（Redis 重启/FLUSHALL/主从切换）则从 {@code 0-0} 重放当前保留的 Stream，
     * 尽量补投组丢失期间发布的失效消息（应用失效是幂等 DEL，重放无害；总量受
     * {@code streams-maxlen} 约束）。</p>
     *
     * @return true 表示组可用（新建或已存在）；false 表示 Redis 不可用等，调用方按退避重试
     */
    private boolean ensureGroup() {
        org.springframework.data.redis.connection.stream.ReadOffset start =
                everGroupReady
                        ? org.springframework.data.redis.connection.stream.ReadOffset.from("0-0")
                        : org.springframework.data.redis.connection.stream.ReadOffset.latest();
        try {
            template.execute((org.springframework.data.redis.core.RedisCallback<Object>) connection ->
                    connection.streamCommands().xGroupCreate(
                            stream.getBytes(StandardCharsets.UTF_8), group, start, true));
            groupReady = true;
            everGroupReady = true;
            markGroupRecreatedIfRecovering();
            return true;
        } catch (Exception e) {
            if (isBusyGroup(e)) {
                // 组已存在：并发创建/进程内重试的正常路径
                groupReady = true;
                everGroupReady = true;
                markGroupRecreatedIfRecovering();
                return true;
            }
            groupReady = false;
            debugOrWarn("创建消费组失败（Redis 不可用等，稍后重试）", e);
            return false;
        }
    }

    /** Redis 错误链中是否包含 NOGROUP（组/Stream 不存在） */
    private static boolean isNoGroup(Throwable error) {
        return errorChainContains(error, "NOGROUP");
    }

    /** Redis 错误链中是否包含 BUSYGROUP（组已存在） */
    private static boolean isBusyGroup(Throwable error) {
        return errorChainContains(error, "BUSYGROUP");
    }

    private static boolean errorChainContains(Throwable error, String token) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            String message = t.getMessage();
            if (message != null && message.toUpperCase(java.util.Locale.ROOT).contains(token)) {
                return true;
            }
        }
        return false;
    }

    /** 运行中组丢失后的首次成功重建：计数 + 告警（启动期正常建组不计） */
    private void markGroupRecreatedIfRecovering() {
        if (!recoveringGroup) {
            return;
        }
        recoveringGroup = false;
        groupRecreatedCount.incrementAndGet();
        metrics.streamsGroupRecreated();
        warnGroupRecreated();
    }

    /** 组自愈重建告警（30s 限频）：说明 Redis 侧状态丢失，Stream 保留范围外的旧消息由 L1 TTL 兜底 */
    private void warnGroupRecreated() {
        long now = System.nanoTime();
        if (now - lastGroupRecreateWarnAt > 30_000_000_000L) {
            lastGroupRecreateWarnAt = now;
            log.warn("失效广播消费组丢失（Redis 重启/FLUSHALL/主从切换或启动时 Redis 不可用），"
                    + "已重建消费组 '{}' 并从 Stream 保留范围重放失效；已超出 streams-maxlen 的旧消息"
                    + "由 L1 TTL 兜底（本告警 30s 内不重复）", group);
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
