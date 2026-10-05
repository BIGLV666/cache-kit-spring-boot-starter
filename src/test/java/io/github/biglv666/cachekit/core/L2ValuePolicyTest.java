package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.annotation.CacheEntity;
import io.github.biglv666.cachekit.annotation.CacheId;
import io.github.biglv666.cachekit.channel.CacheEntry;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.support.JsonCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L2 值策略测试：gzip 压缩（gz: 前缀标记，L1 永远存原文）与单值大小上限（两级跳写 + 计数）。
 */
class L2ValuePolicyTest {

    @CacheEntity(prefix = "big_user")
    static class BigEntity {
        @CacheId
        public Long userId;
        public String userName;

        public BigEntity() {
        }

        BigEntity(Long userId, String userName) {
            this.userId = userId;
            this.userName = userName;
        }
    }

    static class RecordingMetrics implements CacheMetricsListener {
        final AtomicInteger oversized = new AtomicInteger();
        final AtomicInteger compressed = new AtomicInteger();

        @Override
        public void l2ValueOversized() {
            oversized.incrementAndGet();
        }

        @Override
        public void l2ValueCompressed() {
            compressed.incrementAndGet();
        }
    }

    private InMemoryChannel l1;
    private InMemoryChannel l2;
    private RecordingMetrics metrics;
    private EntityMetadataRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new EntityMetadataRegistry();
        metrics = new RecordingMetrics();
    }

    private TieredEntityCache cache(CacheKitProperties props) {
        l1 = new InMemoryChannel();
        l2 = new InMemoryChannel();
        TieredEntityCache cache = new TieredEntityCache(props, l1, l2,
                new NoopInvalidationPublisher(), null, "", java.util.List.of());
        cache.setMetricsListener(metrics);
        return cache;
    }

    private static String bigName(int chars) {
        return "u".repeat(chars);
    }

    @Test
    void compressionShouldStoreGzInL2AndPlainInL1() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setCompressionEnabled(true);
        props.getL2().setCompressionMinKb(1);
        props.getL2().setJitter(Duration.ZERO);
        TieredEntityCache cache = cache(props);
        var meta = registry.require(BigEntity.class);

        cache.load(meta, 1L, null, true, () -> new BigEntity(1L, bigName(4096)));

        // L2 存压缩值（gz: 前缀 + Base64），L1 存原文
        String l2Json = l2.store.get("big_user:1");
        assertThat(l2Json).startsWith(TieredEntityCache.GZ_PREFIX);
        assertThat(l1.store.get("big_user:1")).doesNotStartWith(TieredEntityCache.GZ_PREFIX);
        assertThat(metrics.compressed.get()).isEqualTo(1);

        // 读回等于原文：L2 解压 → decode 出同值
        Object v = cache.load(meta, 1L, null, true, () -> {
            throw new AssertionError("不应回源");
        });
        assertThat(((BigEntity) v).userName).isEqualTo(bigName(4096));
    }

    @Test
    void smallValueShouldStayPlainEvenWhenCompressionEnabled() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setCompressionEnabled(true);
        props.getL2().setCompressionMinKb(32);
        TieredEntityCache cache = cache(props);
        var meta = registry.require(BigEntity.class);

        cache.load(meta, 2L, null, true, () -> new BigEntity(2L, "small"));

        assertThat(l2.store.get("big_user:2")).startsWith("{");
        assertThat(metrics.compressed.get()).isZero();
    }

    @Test
    void compressionOffByDefaultAndLegacyCompressedValueStillReadable() {
        // 默认关闭：写入原文
        CacheKitProperties props = new CacheKitProperties();
        TieredEntityCache cache = cache(props);
        var meta = registry.require(BigEntity.class);
        cache.load(meta, 3L, null, true, () -> new BigEntity(3L, bigName(4096)));
        assertThat(l2.store.get("big_user:3")).startsWith("{");

        // 关闭压缩后，存量 gz 值仍可读（读侧按前缀识别，与配置无关）
        String plainJson = l2.store.get("big_user:3");
        assertThat(plainJson).startsWith("{");
        l1.store.clear();
        l2.store.put("big_user:3", compress(plainJson));
        Object v = cache.load(meta, 3L, null, true, () -> {
            throw new AssertionError("不应回源");
        });
        assertThat(((BigEntity) v).userName).isEqualTo(bigName(4096));
    }

    @Test
    void corruptCompressedValueShouldBeTreatedAsMiss() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setCompressionEnabled(true);
        TieredEntityCache cache = cache(props);
        var meta = registry.require(BigEntity.class);
        cache.load(meta, 4L, null, true, () -> new BigEntity(4L, "ok"));

        l1.store.clear();
        l2.store.put("big_user:4", TieredEntityCache.GZ_PREFIX + "###not-base64###");
        AtomicInteger loads = new AtomicInteger();
        Object v = cache.load(meta, 4L, null, true, () -> {
            loads.incrementAndGet();
            return new BigEntity(4L, "recovered");
        });
        // 坏压缩值按未命中处理：回源覆盖，业务拿到正确数据
        assertThat(loads.get()).isEqualTo(1);
        assertThat(((BigEntity) v).userName).isEqualTo("recovered");
    }

    @Test
    void nullPlaceholderShouldNeverBeCompressed() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setCompressionEnabled(true);
        props.getL2().setCompressionMinKb(0);
        TieredEntityCache cache = cache(props);
        var meta = registry.require(BigEntity.class);

        cache.load(meta, 5L, null, true, () -> null);

        assertThat(l2.store.get("big_user:5")).isEqualTo(JsonCodec.NULL_SENTINEL);
    }

    @Test
    void oversizedValueShouldSkipBothLevelsAndCount() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setMaxValueKb(1);
        props.getL2().setCompressionEnabled(true);
        props.getL2().setCompressionMinKb(1);
        TieredEntityCache cache = cache(props);
        var meta = registry.require(BigEntity.class);
        String name = bigName(4096);

        AtomicInteger loads = new AtomicInteger();
        cache.load(meta, 6L, null, true, () -> {
            loads.incrementAndGet();
            return new BigEntity(6L, name);
        });
        cache.load(meta, 6L, null, true, () -> {
            loads.incrementAndGet();
            return new BigEntity(6L, name);
        });

        // 超限两级都不写：每次读都回源
        assertThat(loads.get()).isEqualTo(2);
        assertThat(l1.store).doesNotContainKey("big_user:6");
        assertThat(l2.store).doesNotContainKey("big_user:6");
        assertThat(metrics.oversized.get()).isEqualTo(2);
        assertThat(metrics.compressed.get()).isZero();
    }

    @Test
    void maxValueKbZeroShouldDisableTheCap() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setMaxValueKb(0);
        TieredEntityCache cache = cache(props);
        var meta = registry.require(BigEntity.class);

        cache.load(meta, 7L, null, true, () -> new BigEntity(7L, bigName(4096)));

        assertThat(l2.store).containsKey("big_user:7");
        assertThat(metrics.oversized.get()).isZero();
    }

    @Test
    void nullSentinelShouldNotCountAsOversized() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setMaxValueKb(1);
        TieredEntityCache cache = cache(props);
        var meta = registry.require(BigEntity.class);

        cache.load(meta, 8L, null, true, () -> null);

        assertThat(l2.store.get("big_user:8")).isEqualTo(JsonCodec.NULL_SENTINEL);
        assertThat(metrics.oversized.get()).isZero();
    }

    /** 与主实现相同的编码算法，构造存量压缩值 */
    private static String compress(String json) {
        try {
            java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
            java.util.zip.GZIPOutputStream gz = new java.util.zip.GZIPOutputStream(buf);
            gz.write(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            gz.close();
            return TieredEntityCache.GZ_PREFIX + Base64.getEncoder().encodeToString(buf.toByteArray());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
