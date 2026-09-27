package io.github.biglv666.cachekit.handle;

import io.github.biglv666.cachekit.core.InMemoryChannel;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EntityCache 批量接口：getBatch 三级 read-through + single-flight 合并回源。
 */
class DelegatingEntityCacheBatchTest {

    @Test
    void getBatchShouldReadThroughAndCache() {
        InMemoryChannel l2 = new InMemoryChannel();
        TieredEntityCache cache = new TieredEntityCache(new io.github.biglv666.cachekit.config.CacheKitProperties(),
                new io.github.biglv666.cachekit.channel.CaffeineChannel(1024), l2,
                new NoopInvalidationPublisher(), new io.github.biglv666.cachekit.core.DoubleDeleteScheduler(Duration.ofMillis(10)));
        DelegatingEntityCache<UserEntity> handle = new DelegatingEntityCache<>(
                UserEntity.class, new EntityMetadataRegistry(), cache);

        AtomicInteger dbCalls = new AtomicInteger();
        List<UserEntity> first = handle.getBatch(List.of(1L, 2L, 3L), ids -> {
            dbCalls.incrementAndGet();
            return List.of(new UserEntity(1L, "a"), new UserEntity(2L, "b")); // 3 不存在
        });

        assertThat(first).hasSize(3);
        assertThat(first.get(0).getUserName()).isEqualTo("a");
        assertThat(first.get(1).getUserName()).isEqualTo("b");
        assertThat(first.get(2)).isNull();
        assertThat(dbCalls.get()).isEqualTo(1);

        // 二次调用：全部命中缓存，不回源
        List<UserEntity> second = handle.getBatch(List.of(1L, 2L, 3L), ids -> {
            dbCalls.incrementAndGet();
            return List.of();
        });
        assertThat(second).hasSize(3);
        assertThat(second.get(0).getUserName()).isEqualTo("a");
        assertThat(second.get(2)).as("null 占位生效").isNull();
        assertThat(dbCalls.get()).as("命中缓存不应再次回源").isEqualTo(1);
    }
}
