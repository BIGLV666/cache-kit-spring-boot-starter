package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CacheEntry;
import io.github.biglv666.cachekit.channel.L1Channel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L1 预刷新测试：剩余 TTL 低于窗口触发异步刷新（返回当前值，读永远拿未过期值），
 * 去重表保证窗口内同键只提交一次；失败吞掉、队列满丢弃、支持关闭。
 * 用可控 L1 桩（可设定 remainingTtlNanos）驱动，不依赖真实时钟。
 */
class RefreshAheadTest {

    /** 可控 L1：remainingTtlNanos 由测试设定；不支持时返回 -1（预刷新自动禁用） */
    static class ControllableL1 implements L1Channel {
        final Map<String, String> store = new ConcurrentHashMap<>();
        volatile long remaining = -1;
        int evictCount;

        @Override
        public CacheEntry get(String key) {
            String raw = store.get(key);
            return raw == null ? CacheEntry.miss() : CacheEntry.of(raw);
        }

        @Override
        public void put(String key, String json, Duration ttl) {
            store.put(key, json);
        }

        @Override
        public void evict(String key) {
            evictCount++;
            store.remove(key);
        }

        @Override
        public long remainingTtlNanos(String key) {
            return store.containsKey(key) ? remaining : -1;
        }
    }

    /** 捕获任务不立即执行的执行器：验证去重与丢弃语义 */
    static class CapturingExecutor implements java.util.concurrent.Executor {
        final List<Runnable> tasks = new CopyOnWriteArrayList<>();
        volatile boolean reject = false;

        @Override
        public void execute(Runnable command) {
            if (reject) {
                throw new java.util.concurrent.RejectedExecutionException("queue full");
            }
            tasks.add(command);
        }
    }

    static class RecordingMetrics implements CacheMetricsListener {
        final AtomicInteger triggered = new AtomicInteger();
        final AtomicInteger dropped = new AtomicInteger();
        final AtomicInteger failed = new AtomicInteger();

        @Override
        public void l1RefreshAheadTriggered() {
            triggered.incrementAndGet();
        }

        @Override
        public void l1RefreshAheadDropped() {
            dropped.incrementAndGet();
        }

        @Override
        public void l1RefreshAheadFailed() {
            failed.incrementAndGet();
        }
    }

    private ControllableL1 l1;
    private InMemoryChannel l2;
    private CapturingExecutor executor;
    private RecordingMetrics metrics;
    private TieredEntityCache cache;
    private EntityMetadata meta;

    @BeforeEach
    void setUp() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL1().setRefreshAhead(Duration.ofSeconds(4));
        props.getL2().setJitter(Duration.ZERO);
        l1 = new ControllableL1();
        l2 = new InMemoryChannel();
        executor = new CapturingExecutor();
        metrics = new RecordingMetrics();
        cache = new TieredEntityCache(props, l1, l2, new NoopInvalidationPublisher(), null, "", List.of());
        cache.setMetricsListener(metrics);
        cache.setRefreshAheadExecutor(executor);
        meta = new EntityMetadataRegistry().require(UserEntity.class);
    }

    @Test
    void remainingAboveWindowShouldNotTrigger() {
        l1.remaining = Duration.ofSeconds(9).toNanos();
        l1.store.put("user_entity:1", "{\"userId\":1,\"userName\":\"stale\"}");

        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "fresh"));

        assertThat(executor.tasks).isEmpty();
        assertThat(metrics.triggered.get()).isZero();
        assertThat(l1.store.get("user_entity:1")).contains("stale");
    }

    @Test
    void remainingBelowWindowShouldSubmitRefreshTaskAndReturnCurrentValue() {
        l1.remaining = Duration.ofSeconds(2).toNanos();
        l1.store.put("user_entity:1", "{\"userId\":1,\"userName\":\"stale\"}");

        Object v = cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "fresh"));

        // 读路径拿到的是当前值（任务已提交但未执行），刷新任务待执行
        assertThat(((UserEntity) v).getUserName()).isEqualTo("stale");
        assertThat(executor.tasks).hasSize(1);
        assertThat(metrics.triggered.get()).isEqualTo(1);

        // 执行任务：删本地键 → 走 L2/DB 回填新值
        l2.store.put("user_entity:1", "{\"userId\":1,\"userName\":\"from-l2\"}");
        executor.tasks.get(0).run();
        assertThat(l1.evictCount).isEqualTo(1);
        assertThat(l1.store.get("user_entity:1")).contains("from-l2");
        assertThat(metrics.failed.get()).isZero();
    }

    @Test
    void concurrentReadsDuringRefreshWindowShouldDeduplicateToSingleTask() {
        l1.remaining = Duration.ofSeconds(2).toNanos();
        l1.store.put("user_entity:1", "{\"userId\":1,\"userName\":\"stale\"}");

        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "fresh"));
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "fresh"));
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "fresh"));

        assertThat(executor.tasks).hasSize(1);
        assertThat(metrics.triggered.get()).isEqualTo(1);
    }

    @Test
    void failedRefreshShouldSwallowAndReleaseDedup() throws Exception {
        l1.remaining = Duration.ofSeconds(2).toNanos();
        l1.store.put("user_entity:1", "{\"userId\":1,\"userName\":\"stale\"}");
        AtomicBoolean dbDown = new AtomicBoolean(true);
        AtomicInteger attempts = new AtomicInteger();

        cache.load(meta, 1L, null, true, () -> {
            if (dbDown.get()) {
                throw new IllegalStateException("db down");
            }
            attempts.incrementAndGet();
            return new UserEntity(1L, "v2");
        });
        // 任务执行：evict 后 load → 回源抛异常 → 被吞掉（failed 计数），键保持已删状态
        executor.tasks.get(0).run();
        assertThat(metrics.failed.get()).isEqualTo(1);
        assertThat(l1.evictCount).isEqualTo(1);

        // L2 命中路径回填 L1（预刷新只在 L1 命中时触发，此路径不触发）
        dbDown.set(false);
        l2.store.put("user_entity:1", "{\"userId\":1,\"userName\":\"v2\"}");
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "v2"));
        assertThat(executor.tasks).hasSize(1);

        // 去重已释放：再次 L1 命中（剩余 TTL 低于窗口）重新触发
        l1.remaining = Duration.ofSeconds(2).toNanos();
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "v2"));
        assertThat(executor.tasks).hasSize(2);
        assertThat(metrics.triggered.get()).isEqualTo(2);
        // 全程没有一次成功的 DB 回源：L1 命中/L2 命中路径都不该碰 loader
        assertThat(attempts.get()).isZero();
    }

    @Test
    void rejectedTaskShouldCountAsDroppedAndReleaseDedup() {
        l1.remaining = Duration.ofSeconds(2).toNanos();
        l1.store.put("user_entity:1", "{\"userId\":1,\"userName\":\"stale\"}");
        executor.reject = true;

        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "fresh"));

        assertThat(metrics.dropped.get()).isEqualTo(1);
        assertThat(metrics.triggered.get()).isZero();
        // 释放去重后下次读可重新触发
        executor.reject = false;
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "fresh"));
        assertThat(metrics.triggered.get()).isEqualTo(1);
    }

    @Test
    void noExecutorShouldDisableRefreshAhead() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL1().setRefreshAhead(Duration.ofSeconds(4));
        TieredEntityCache noExecutorCache = new TieredEntityCache(
                props, l1, l2, new NoopInvalidationPublisher(), null, "", List.of());
        noExecutorCache.setMetricsListener(metrics);
        l1.remaining = Duration.ofSeconds(2).toNanos();
        l1.store.put("user_entity:1", "{\"userId\":1,\"userName\":\"stale\"}");

        noExecutorCache.load(meta, 1L, null, true, () -> new UserEntity(1L, "fresh"));

        assertThat(executor.tasks).isEmpty();
        assertThat(metrics.triggered.get()).isZero();
    }

    @Test
    void l1WithoutRemainingTtlSupportShouldDisableRefreshAhead() {
        // InMemoryChannel 未覆写 remainingTtlNanos（返回 -1）：预刷新自动禁用
        CacheKitProperties props = new CacheKitProperties();
        props.getL1().setRefreshAhead(Duration.ofSeconds(4));
        InMemoryChannel plainL1 = new InMemoryChannel();
        TieredEntityCache plainCache = new TieredEntityCache(
                props, plainL1, l2, new NoopInvalidationPublisher(), null, "", List.of());
        plainCache.setMetricsListener(metrics);
        plainL1.store.put("user_entity:1", "{\"userId\":1,\"userName\":\"stale\"}");

        plainCache.load(meta, 1L, null, true, () -> new UserEntity(1L, "fresh"));

        assertThat(executor.tasks).isEmpty();
        assertThat(metrics.triggered.get()).isZero();
    }

    @Test
    void nullPlaceholderEntryShouldAlsoBeRefreshed() {
        l1.remaining = Duration.ofSeconds(2).toNanos();
        l1.store.put("user_entity:1", "__cache_kit_null__");

        Object v = cache.load(meta, 1L, null, true, () -> null);
        assertThat(v).isNull();

        // 刷新任务：null 占位到期前重探 DB，数据仍不存在则占位续期
        executor.tasks.get(0).run();
        assertThat(l1.store.get("user_entity:1")).isEqualTo("__cache_kit_null__");
        assertThat(metrics.failed.get()).isZero();
    }
}
