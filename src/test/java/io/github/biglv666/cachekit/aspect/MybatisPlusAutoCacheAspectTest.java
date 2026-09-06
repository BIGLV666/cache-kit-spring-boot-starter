package io.github.biglv666.cachekit.aspect;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.DoubleDeleteScheduler;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.MpUserEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class MybatisPlusAutoCacheAspectTest {

    interface MpUserMapper extends BaseMapper<MpUserEntity> {
    }

    private Map<Long, MpUserEntity> db;
    private AtomicInteger dbHits;
    /** 记录每次 selectBatchIds 实际下推到 DB 的 ID 集合（验证缺失部分是一次批量 IN 回源） */
    private List<Collection<?>> batchQueries;
    private MpUserMapper proxy;

    @BeforeEach
    void setUp() {
        db = new ConcurrentHashMap<>();
        db.put(1L, new MpUserEntity(1L, "lv"));
        dbHits = new AtomicInteger();
        batchQueries = new ArrayList<>();

        // 用动态代理模拟 MapperFactoryBean 产出的 MapperProxy
        MpUserMapper mapper = (MpUserMapper) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{MpUserMapper.class},
                (p, method, args) -> {
                    if ("selectById".equals(method.getName())) {
                        dbHits.incrementAndGet();
                        return db.get(((Number) args[0]).longValue());
                    }
                    if ("selectBatchIds".equals(method.getName())) {
                        dbHits.incrementAndGet();
                        Collection<?> ids = (Collection<?>) args[0];
                        batchQueries.add(new ArrayList<>(ids));
                        return db.entrySet().stream()
                                .filter(e -> ids.contains(e.getKey()))
                                .map(Map.Entry::getValue)
                                .collect(java.util.stream.Collectors.toList());
                    }
                    if ("updateById".equals(method.getName())) {
                        MpUserEntity entity = (MpUserEntity) args[0];
                        db.put(entity.getUserId(), entity);
                        return 1;
                    }
                    return null;
                });

        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        TieredEntityCache cache = new TieredEntityCache(props,
                new CaffeineChannel(1024), null,
                new NoopInvalidationPublisher(),
                new DoubleDeleteScheduler(Duration.ofMillis(50)));

        AspectJProxyFactory factory = new AspectJProxyFactory(mapper);
        factory.addAspect(new MybatisPlusAutoCacheAspect(cache, new EntityMetadataRegistry()));
        proxy = (MpUserMapper) factory.getProxy();
    }

    @Test
    void selectByIdShouldBeCachedWithoutAnyAnnotation() {
        proxy.selectById(1L);
        proxy.selectById(1L);

        assertThat(dbHits.get()).isEqualTo(1);
    }

    @Test
    void updateByIdShouldEvictCache() {
        proxy.selectById(1L);
        assertThat(dbHits.get()).isEqualTo(1);

        proxy.updateById(new MpUserEntity(1L, "lv2"));

        proxy.selectById(1L);
        assertThat(dbHits.get()).isEqualTo(2);
    }

    @Test
    void unknownEntityIdShouldReturnNullEachTime() {
        // null 结果缓存：db 命中计数应为 1
        assertThat(proxy.selectById(404L)).isNull();
        assertThat(proxy.selectById(404L)).isNull();
        assertThat(dbHits.get()).isEqualTo(1);
    }

    @Test
    void selectBatchIdsShouldCachePerIdWithNullPlaceholder() {
        // 首次 [1,2]：2 不存在 → 整批回源一次，2 缓存 null 占位
        assertThat(proxy.selectBatchIds(List.of(1L, 2L))).hasSize(1);
        assertThat(dbHits.get()).isEqualTo(1);

        // 再次 [1,2]：1 命中值 + 2 命中 null 占位 → 零 DB
        assertThat(proxy.selectBatchIds(List.of(1L, 2L))).hasSize(1);
        assertThat(dbHits.get()).isEqualTo(1);

        // [1,2,3]：3 未命中 → 只回源缺失部分（DB 第二次）
        assertThat(proxy.selectBatchIds(List.of(1L, 2L, 3L))).hasSize(1);
        assertThat(dbHits.get()).isEqualTo(2);

        // 全部三态已缓存 → 零 DB
        assertThat(proxy.selectBatchIds(List.of(1L, 2L, 3L))).hasSize(1);
        assertThat(dbHits.get()).isEqualTo(2);
    }

    @Test
    void batchMissShouldGoDownAsSingleBatchQueryNotPerIdQueries() {
        // DB 只有 1；请求 [1,2,3,4,5]（缓存全空，4 个缺失）
        assertThat(proxy.selectBatchIds(List.of(1L, 2L, 3L, 4L, 5L))).hasSize(1);

        // 关键断言：缺失的 4 个 ID 是一次批量 IN 回源，不是 4 次逐 ID 单查
        assertThat(dbHits.get()).as("批量回源次数").isEqualTo(1);
        assertThat(batchQueries).hasSize(1);
        @SuppressWarnings("unchecked")
        List<Long> pushedIds = (List<Long>) batchQueries.get(0);
        assertThat(pushedIds)
                .as("缓存全空时，5 个缺失 ID 应一次批量 IN 回源")
                .containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L);

        // 复查：全部三态已缓存，零 DB
        assertThat(proxy.selectBatchIds(List.of(1L, 2L, 3L, 4L, 5L))).hasSize(1);
        assertThat(dbHits.get()).as("复查不再回源").isEqualTo(1);
    }

    @Test
    void selectBatchIdsPartialMissShouldQueryOnlyMissing() {
        // 先单独缓存 id=1
        assertThat(proxy.selectById(1L)).isNotNull();
        assertThat(dbHits.get()).isEqualTo(1);

        // [1,3]：1 命中，3 miss → 缺失集合只有 [3]
        assertThat(proxy.selectBatchIds(List.of(1L, 3L))).hasSize(1);
        assertThat(dbHits.get()).isEqualTo(2);
    }

    @Test
    void updateByIdShouldEvictBatchMember() {
        assertThat(proxy.selectBatchIds(List.of(1L))).hasSize(1);
        assertThat(proxy.updateById(new MpUserEntity(1L, "lv2"))).isEqualTo(1);
        MpUserEntity fresh = proxy.selectById(1L);
        assertThat(fresh.getUserName()).isEqualTo("lv2");
        // updateById 本身 + selectById 回源 = 2 次 DB 读（1 次批量 + 1 次回源）
        assertThat(dbHits.get()).isEqualTo(2);
    }

    @Test
    void nonBaseMapperMethodsShouldPassThrough() {
        // selectList 等 P2 方法直查（动态代理 handler 返回 null 即可，只验证不抛错）
        assertThat(proxy.selectList(null)).isNull();
    }
}
