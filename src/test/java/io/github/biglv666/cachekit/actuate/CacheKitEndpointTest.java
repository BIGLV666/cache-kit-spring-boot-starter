package io.github.biglv666.cachekit.actuate;

import io.github.biglv666.cachekit.core.InMemoryChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.CacheStatsCollector;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * /actuator/cachekit 端点单元测试：真实三级链造流量后验证输出形状、
 * 每实体命中率/计数、熔断器缺省展示。不依赖 Redis 与 actuator 服务端。
 */
class CacheKitEndpointTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void endpointShouldExposeEntityStatsAndStates() {
        CacheKitProperties props = new CacheKitProperties();
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.find(UserEntity.class);
        var meta = registry.require(UserEntity.class);

        InMemoryChannel l1 = new InMemoryChannel();
        CacheStatsCollector collector = new CacheStatsCollector();
        TieredEntityCache cache = new TieredEntityCache(props, l1, null,
                new NoopInvalidationPublisher(), null, "", List.of());
        cache.setStatsCollector(collector);

        // 造流量：首读 miss 回源，二读 L1 命中
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "n0"));
        cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "n1"));

        CacheKitEndpoint endpoint = new CacheKitEndpoint(props, registry,
                provider(cache), provider(l1), provider(null), provider(null), provider(null), provider(collector));
        Map<String, Object> out = endpoint.cacheKit();

        assertThat(out.get("keyNamespace")).isEqualTo("");
        assertThat(((Map<String, Object>) out.get("l1")).get("estimatedSize")).isEqualTo(1L);

        // 熔断器未装配时展示 DISABLED 而非缺字段
        Map<String, Object> l2 = (Map<String, Object>) out.get("l2");
        assertThat(((Map<String, Object>) l2.get("circuitBreaker")).get("state")).isEqualTo("DISABLED");

        // 失效后再取快照：evictions 计数入端点
        cache.evictBatch(meta, List.of(1L));
        out = endpoint.cacheKit();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> entities = (List<Map<String, Object>>) out.get("entities");
        assertThat(entities).hasSize(1);
        Map<String, Object> user = entities.get(0);
        assertThat(user.get("prefix")).isEqualTo("user_entity");
        assertThat(user.get("l1Hit")).isEqualTo(1L);
        assertThat(user.get("l1Miss")).isEqualTo(1L);
        assertThat((Double) user.get("l1HitRate")).isEqualTo(0.5);
        assertThat(user.get("dbLoads")).isEqualTo(1L);
        assertThat(user.get("evictions")).isEqualTo(1L);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void endpointShouldWorkWithoutAnyOptionalComponents() {
        // 全部可选组件缺席（纯 L1 关闭等极端装配）时端点不应抛错
        CacheKitEndpoint endpoint = new CacheKitEndpoint(new CacheKitProperties(),
                new EntityMetadataRegistry(), provider(null), provider(null),
                provider(null), provider(null), provider(null), provider(null));
        Map<String, Object> out = endpoint.cacheKit();
        assertThat(out).containsKeys("l1", "l2", "doubleDelete", "entities");
        assertThat((List<?>) out.get("entities")).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private static <T> ObjectProvider<T> provider(T value) {
        ObjectProvider<T> p = mock(ObjectProvider.class);
        when(p.getIfAvailable()).thenReturn(value);
        return p;
    }
}
