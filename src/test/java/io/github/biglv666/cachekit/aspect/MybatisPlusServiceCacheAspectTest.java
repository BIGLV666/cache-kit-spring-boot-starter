package io.github.biglv666.cachekit.aspect;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.DoubleDeleteScheduler;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.core.TieredEntityCache.CachePeek;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.MpUserEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * IService 批量写失效切面：saveBatch / updateBatchById 在 MP 内部经 SqlSession 批量语句执行、
 * 绕过 mapper 代理——本切面必须按实体主键批量失效，否则批量更新后缓存脏到 L2 TTL。
 * 实体类型从 ServiceImpl&lt;M, T&gt; 第二泛型解析。
 */
class MybatisPlusServiceCacheAspectTest {

    interface MpUserMapper extends BaseMapper<MpUserEntity> {
    }

    /** 覆写批量写方法避免真实 SqlSession 依赖：只验证切面的失效行为 */
    static class BatchUserService extends ServiceImpl<MpUserMapper, MpUserEntity> {
        final List<MpUserEntity> saved = new ArrayList<>();
        final List<MpUserEntity> updated = new ArrayList<>();

        @Override
        public boolean saveBatch(Collection<MpUserEntity> entityList, int batchSize) {
            saved.addAll(entityList);
            return true;
        }

        @Override
        public boolean updateBatchById(Collection<MpUserEntity> entityList, int batchSize) {
            updated.addAll(entityList);
            return true;
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
        meta = new EntityMetadataRegistry().require(MpUserEntity.class);

        AspectJProxyFactory factory = new AspectJProxyFactory(new BatchUserService());
        factory.addAspect(new MybatisPlusServiceCacheAspect(cache, new EntityMetadataRegistry(),
                new CacheInvalidateAspect(cache, new EntityMetadataRegistry(), false)));
        // Boot 默认 CGLIB（proxy-target-class=true），与真实宿主一致；JDK 代理无法按实现类转型
        factory.setProxyTargetClass(true);
        proxy = factory.getProxy();
    }

    @Test
    void updateBatchByIdShouldEvictEachEntity() {
        cache.load(meta, 1L, null, true, () -> new MpUserEntity(1L, "lv"));
        cache.load(meta, 2L, null, true, () -> new MpUserEntity(2L, "lv"));
        assertThat(cache.peek(meta, 1L).state()).isEqualTo(CachePeek.State.HIT);

        proxy.updateBatchById(List.of(new MpUserEntity(1L, "lv2"), new MpUserEntity(2L, "lv2")));

        assertThat(cache.peek(meta, 1L).state())
                .as("批量更新后每行缓存必须失效（SqlSession 批量通道绕过 mapper 代理，靠本切面）")
                .isEqualTo(CachePeek.State.MISS);
        assertThat(cache.peek(meta, 2L).state()).isEqualTo(CachePeek.State.MISS);
    }

    @Test
    void saveBatchShouldClearNullPlaceholders() {
        cache.load(meta, 404L, null, true, () -> null);
        assertThat(cache.peek(meta, 404L).state()).isEqualTo(CachePeek.State.HIT_NULL);

        proxy.saveBatch(List.of(new MpUserEntity(404L, "newcomer")));

        assertThat(cache.peek(meta, 404L).state())
                .as("批量插入后既有 null 占位必须被清掉")
                .isEqualTo(CachePeek.State.MISS);
    }
}
