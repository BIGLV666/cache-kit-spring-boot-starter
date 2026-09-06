package io.github.biglv666.cachekit.aspect;

import io.github.biglv666.cachekit.annotation.CacheHandle;
import io.github.biglv666.cachekit.annotation.CacheInvalidate;
import io.github.biglv666.cachekit.annotation.CachedQuery;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.CacheKit;
import io.github.biglv666.cachekit.core.DoubleDeleteScheduler;
import io.github.biglv666.cachekit.core.EntityCache;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.handle.DelegatingEntityCache;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 切面测试：目标为 JDK 动态代理（与真实 MyBatis MapperProxy 形态一致），
 * 注解声明在接口方法上——生产中 mapper 接口正是这种匹配路径。
 */
class CachedQueryAspectTest {

    interface UserQueryMapper {

        @CachedQuery
        UserEntity selectByUserId(Long userId);

        /** 主键批量：List&lt;实体&gt; 返回 + ID 集合参数，per-ID 三级链 */
        @CachedQuery
        List<UserEntity> selectByUserIds(Collection<Long> userIds);

        /** condition 恒为 false：始终直查 DB */
        @CachedQuery(condition = "false")
        UserEntity selectWithoutCache(Long userId);

        @CacheInvalidate(entity = UserEntity.class)
        int updateUserName(Long userId);

        UserEntity unannotated(Long userId);
    }

    private Map<Long, UserEntity> db;
    private AtomicInteger dbHits;
    private UserQueryMapper proxy;

    @BeforeEach
    void setUp() {
        db = new ConcurrentHashMap<>();
        db.put(1L, new UserEntity(1L, "lv"));
        dbHits = new AtomicInteger();

        UserQueryMapper mapper = (UserQueryMapper) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{UserQueryMapper.class},
                (p, method, args) -> switch (method.getName()) {
                    case "selectByUserId", "selectWithoutCache", "unannotated" -> {
                        dbHits.incrementAndGet();
                        yield db.get(((Number) args[0]).longValue());
                    }
                    case "selectByUserIds" -> {
                        dbHits.incrementAndGet();
                        Collection<Long> ids = (Collection<Long>) args[0];
                        yield ids.stream().filter(db::containsKey).map(db::get).collect(java.util.stream.Collectors.toList());
                    }
                    case "updateUserName" -> {
                        UserEntity user = db.get(((Number) args[0]).longValue());
                        if (user != null) {
                            user.setUserName("updated");
                        }
                        yield user == null ? 0 : 1;
                    }
                    default -> null;
                });

        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        TieredEntityCache cache = new TieredEntityCache(props,
                new CaffeineChannel(1024), null,
                new NoopInvalidationPublisher(),
                new DoubleDeleteScheduler(Duration.ofMillis(50)));

        AspectJProxyFactory factory = new AspectJProxyFactory(mapper);
        factory.addAspect(new CachedQueryAspect(cache, new EntityMetadataRegistry()));
        factory.addAspect(new CacheInvalidateAspect(cache, new EntityMetadataRegistry()));
        proxy = (UserQueryMapper) factory.getProxy();
    }

    @Test
    void annotatedQueryShouldBeCached() {
        proxy.selectByUserId(1L);
        proxy.selectByUserId(1L);
        proxy.selectByUserId(1L);

        assertThat(dbHits.get()).isEqualTo(1);
    }

    @Test
    void invalidateShouldEvictCache() {
        proxy.selectByUserId(1L);
        assertThat(dbHits.get()).isEqualTo(1);

        proxy.updateUserName(1L);

        proxy.selectByUserId(1L);
        assertThat(dbHits.get()).isEqualTo(2);
    }

    @Test
    void conditionFalseShouldBypassCache() {
        proxy.selectWithoutCache(1L);
        proxy.selectWithoutCache(1L);

        assertThat(dbHits.get()).isEqualTo(2);
    }

    @Test
    void unannotatedMethodShouldHitDbDirectly() {
        proxy.unannotated(1L);
        proxy.unannotated(1L);

        assertThat(dbHits.get()).isEqualTo(2);
    }

    @Test
    void withDbShouldBypassAnnotatedCache() {
        proxy.selectByUserId(1L);
        assertThat(dbHits.get()).isEqualTo(1);

        UserEntity fresh = CacheKit.withDb(() -> proxy.selectByUserId(1L));

        assertThat(dbHits.get()).isEqualTo(2);
        assertThat(fresh).isEqualTo(new UserEntity(1L, "lv"));
    }

    @Test
    void annotatedListQueryShouldCachePerId() {
        // 首次 [1,2]：2 不存在 → 回源一次，2 缓存 null 占位
        assertThat(proxy.selectByUserIds(java.util.List.of(1L, 2L))).hasSize(1);
        assertThat(dbHits.get()).isEqualTo(1);

        // 全部命中（含 null 占位）→ 零 DB
        assertThat(proxy.selectByUserIds(java.util.List.of(1L, 2L))).hasSize(1);
        assertThat(dbHits.get()).isEqualTo(1);

        // [1] 命中 → 零 DB
        assertThat(proxy.selectByUserIds(java.util.List.of(1L))).hasSize(1);
        assertThat(dbHits.get()).isEqualTo(1);

        // 写失效后重新回源
        proxy.updateUserName(1L);
        assertThat(proxy.selectByUserIds(java.util.List.of(1L))).hasSize(1);
        assertThat(dbHits.get()).isEqualTo(2);
    }

    @Test
    void handleShouldShareSameChain() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        TieredEntityCache cache = new TieredEntityCache(props,
                new CaffeineChannel(1024), null,
                new NoopInvalidationPublisher(),
                new DoubleDeleteScheduler(Duration.ofMillis(50)));
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        EntityCache<UserEntity> userCache = new DelegatingEntityCache<>(UserEntity.class, registry, cache);

        UserEntity first = userCache.get(1L, () -> proxy.selectByUserId(1L));
        UserEntity second = userCache.get(1L, () -> proxy.selectByUserId(1L));

        assertThat(dbHits.get()).isEqualTo(1);
        assertThat(first).isEqualTo(second);
    }

    /** 仅供编译期引用 @CacheHandle 注解（Bean 注入由自动装配覆盖） */
    @SuppressWarnings("unused")
    static class CacheHandleHolder {
        @CacheHandle(UserEntity.class)
        EntityCache<UserEntity> cached;
    }
}
