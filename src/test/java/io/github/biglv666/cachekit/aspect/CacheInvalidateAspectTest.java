package io.github.biglv666.cachekit.aspect;

import io.github.biglv666.cachekit.annotation.CacheInvalidate;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.DoubleDeleteScheduler;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.core.TieredEntityCache.CachePeek;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code @CacheInvalidate} 主键收集：实体实例、实体集合参数（0.3.0+ 批量失效）、
 * 主键同名的标量参数均可解析；标量集合（无法证明是主键）必须跳过不猜。
 */
class CacheInvalidateAspectTest {

    static class BatchUserService {

        @CacheInvalidate
        public void batchUpdate(List<UserEntity> users) {
        }

        @CacheInvalidate
        public void singleUpdate(UserEntity user) {
        }

        @CacheInvalidate(entity = UserEntity.class)
        public void scalarIds(List<Long> ids) {
        }

        @CacheInvalidate(entity = UserEntity.class)
        public void namedScalar(Long userId) {
        }
    }

    private TieredEntityCache cache;
    private EntityMetadata meta;
    private BatchUserService proxy;

    @BeforeEach
    void setUp() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        cache = new TieredEntityCache(props,
                new CaffeineChannel(1024), null,
                new NoopInvalidationPublisher(),
                new DoubleDeleteScheduler(Duration.ofMillis(50)));
        meta = new EntityMetadataRegistry().require(UserEntity.class);

        AspectJProxyFactory factory = new AspectJProxyFactory(new BatchUserService());
        factory.addAspect(new CacheInvalidateAspect(cache, new EntityMetadataRegistry(), false));
        proxy = factory.getProxy();
    }

    private void preload(Object... ids) {
        for (Object id : ids) {
            cache.load(meta, id, null, true, () -> new UserEntity((Long) id, "v" + id));
        }
    }

    @Test
    void entityCollectionArgumentShouldEvictEachEntity() {
        preload(1L, 2L, 3L);

        proxy.batchUpdate(List.of(new UserEntity(1L, "a"), new UserEntity(2L, "b")));

        assertThat(cache.peek(meta, 1L).state()).as("实体集合参数逐元素失效").isEqualTo(CachePeek.State.MISS);
        assertThat(cache.peek(meta, 2L).state()).isEqualTo(CachePeek.State.MISS);
        assertThat(cache.peek(meta, 3L).state()).as("未涉及的键不受影响").isEqualTo(CachePeek.State.HIT);
    }

    @Test
    void singleEntityArgumentShouldEvict() {
        preload(1L);

        proxy.singleUpdate(new UserEntity(1L, "x"));

        assertThat(cache.peek(meta, 1L).state()).isEqualTo(CachePeek.State.MISS);
    }

    @Test
    void scalarCollectionShouldBeSkippedNotGuessed() {
        // List<Long> 元素无法证明是主键（可能是手机号等条件值）：跳过失效，绝不猜测
        preload(1L);

        proxy.scalarIds(List.of(1L));

        assertThat(cache.peek(meta, 1L).state())
                .as("标量集合不收，保持严格语义")
                .isEqualTo(CachePeek.State.HIT);
    }

    @Test
    void scalarParameterNamedAsIdFieldShouldEvict() {
        preload(2L);

        proxy.namedScalar(2L);

        assertThat(cache.peek(meta, 2L).state())
                .as("注解显式指定实体 + 参数名与主键字段同名 → 可证明是主键")
                .isEqualTo(CachePeek.State.MISS);
    }
}
