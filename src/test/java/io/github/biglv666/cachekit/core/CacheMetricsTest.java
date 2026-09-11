package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 指标埋点验证：用记录型监听器核对 L1/L2 命中、DB 回源、null 占位、失效与广播计数。
 */
class CacheMetricsTest {

    static class RecordingMetrics implements CacheMetricsListener {
        final AtomicInteger l1Hit = new AtomicInteger();
        final AtomicInteger l1Miss = new AtomicInteger();
        final AtomicInteger l2Hit = new AtomicInteger();
        final AtomicInteger l2Miss = new AtomicInteger();
        final AtomicInteger dbLoad = new AtomicInteger();
        final AtomicInteger nullPlaceholder = new AtomicInteger();
        final AtomicInteger evict = new AtomicInteger();
        final AtomicInteger broadcastSent = new AtomicInteger();

        @Override
        public void l1Lookup(boolean hit) {
            (hit ? l1Hit : l1Miss).incrementAndGet();
        }

        @Override
        public void l2Lookup(boolean hit) {
            (hit ? l2Hit : l2Miss).incrementAndGet();
        }

        @Override
        public void dbLoad(int ids) {
            dbLoad.addAndGet(ids);
        }

        @Override
        public void nullPlaceholder() {
            nullPlaceholder.incrementAndGet();
        }

        @Override
        public void evict(int keys) {
            evict.addAndGet(keys);
        }

        @Override
        public void broadcastSent() {
            broadcastSent.incrementAndGet();
        }
    }

    private RecordingMetrics metrics;
    private TieredEntityCache cache;
    private EntityMetadata meta;

    @BeforeEach
    void setUp() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        metrics = new RecordingMetrics();
        cache = new TieredEntityCache(props,
                new CaffeineChannel(1024), new InMemoryChannel(),
                new NoopInvalidationPublisher(),
                new DoubleDeleteScheduler(Duration.ofMillis(50)));
        cache.setMetricsListener(metrics);
        meta = new EntityMetadataRegistry().require(UserEntity.class);
    }

    @Test
    void loadShouldRecordL1L2DbAndNullMetrics() {
        // 首次：L1 miss + L2 miss + DB 回源
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "lv"));
        // 二次：L1 命中
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "lv"));
        // 三次：查不存在的 ID → L1/L2 miss + null 占位
        cache.load(meta, 404L, null, true, () -> null);

        assertThat(metrics.l1Hit.get()).isEqualTo(1);
        assertThat(metrics.l1Miss.get()).isEqualTo(2);
        assertThat(metrics.l2Miss.get()).isEqualTo(2);
        assertThat(metrics.dbLoad.get()).isEqualTo(2);
        assertThat(metrics.nullPlaceholder.get()).isEqualTo(1);
    }

    @Test
    void evictShouldRecordKeysAndBroadcast() throws Exception {
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "lv"));
        cache.evict(meta, 1L);
        // 等延迟双删（50ms）执行完毕
        Thread.sleep(200);

        assertThat(metrics.evict.get()).isEqualTo(2); // 立即一次 + 双删一次
        assertThat(metrics.broadcastSent.get()).isEqualTo(2);
    }

    @Test
    void loadBatchShouldRecordDbLoadPerBatch() {
        cache.loadBatch(meta, List.of(1L, 2L), true, null,
                ids -> List.of(new UserEntity(1L, "lv")));

        assertThat(metrics.dbLoad.get()).as("dbLoad 按 ID 数计").isEqualTo(2);
        assertThat(metrics.nullPlaceholder.get()).isEqualTo(1); // id=2 不存在
    }
}
