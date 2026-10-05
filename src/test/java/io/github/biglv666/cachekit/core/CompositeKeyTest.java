package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.annotation.CacheEntity;
import io.github.biglv666.cachekit.annotation.CacheId;
import io.github.biglv666.cachekit.aspect.PrimaryKeyResolver;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 复合主键（多个 @CacheId）测试：注册表按声明序解析、键段 join ':'、
 * 严格推导（缺一参数即旁路）、歧义值（含 ':'）拒绝落缓存、失效路径跳过不打断业务写。
 */
class CompositeKeyTest {

    @CacheEntity(prefix = "t_item")
    static class CompositeEntity {
        @CacheId
        public Long orderId;
        @CacheId
        public String skuId;
        public int amount;

        public CompositeEntity() {
        }

        CompositeEntity(Long orderId, String skuId, int amount) {
            this.orderId = orderId;
            this.skuId = skuId;
            this.amount = amount;
        }
    }

    /** MP 注解实体声明多个 @TableId：MP 本身不支持复合主键，注册表必须 fail-fast */
    @com.baomidou.mybatisplus.annotation.TableName("t_mp_composite")
    static class MpCompositeEntity {
        @com.baomidou.mybatisplus.annotation.TableId
        public Long partA;
        @com.baomidou.mybatisplus.annotation.TableId
        public Long partB;
    }

    private EntityMetadataRegistry registry;
    private EntityMetadata meta;
    private InMemoryChannel l1;
    private InMemoryChannel l2;
    private TieredEntityCache cache;

    @BeforeEach
    void setUp() {
        registry = new EntityMetadataRegistry();
        meta = registry.require(CompositeEntity.class);
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        l1 = new InMemoryChannel();
        l2 = new InMemoryChannel();
        cache = new TieredEntityCache(props, l1, l2, new NoopInvalidationPublisher(), null, "", List.of());
    }

    @Test
    void registryShouldParseCompositeIdsInDeclarationOrder() {
        assertThat(meta.idFields()).hasSize(2);
        assertThat(meta.idFields().get(0).getName()).isEqualTo("orderId");
        assertThat(meta.idFields().get(1).getName()).isEqualTo("skuId");

        Object joined = meta.idOf(new CompositeEntity(5L, "s1", 100));
        assertThat(joined).isEqualTo("5:s1");
    }

    @Test
    void registryShouldFailFastOnMultipleMpTableIds() {
        assertThatThrownBy(() -> registry.find(MpCompositeEntity.class))
                .isInstanceOf(CacheKitException.class)
                .hasMessageContaining("MyBatis-Plus 不支持复合主键");
    }

    @Test
    void loadShouldUseJoinedKeyAndEvictShouldHitIt() {
        cache.load(meta, List.of(5L, "s1"), null, true, () -> new CompositeEntity(5L, "s1", 100));

        assertThat(l1.store).containsKey("t_item:5:s1");
        assertThat(l2.store).containsKey("t_item:5:s1");

        // 失效：注解解析出的 join 串（PrimaryKeyResolver）与手动 List 形式都能命中同一键
        cache.evict(meta, "5:s1");
        assertThat(l1.store).doesNotContainKey("t_item:5:s1");

        cache.load(meta, List.of(5L, "s1"), null, true, () -> new CompositeEntity(5L, "s1", 100));
        cache.evictBatch(meta, List.of(List.of(5L, "s1")));
        assertThat(l2.store).doesNotContainKey("t_item:5:s1");
    }

    @Test
    void resolverShouldMatchAllIdFieldNamesOrBypass() throws Exception {
        PrimaryKeyResolver resolver = new PrimaryKeyResolver(registry);
        Method ok = CompositeKeyTest.class.getDeclaredMethod("query", Long.class, String.class);
        Method missing = CompositeKeyTest.class.getDeclaredMethod("queryMissing", Long.class, String.class);
        Method entityParam = CompositeKeyTest.class.getDeclaredMethod("queryByEntity", CompositeEntity.class);

        assertThat(resolver.resolve(ok, new Object[]{5L, "s1"}, meta)).isEqualTo("5:s1");
        // 复合主键任一字段无同名参数：旁路（返回 null），绝不猜测
        assertThat(resolver.resolve(missing, new Object[]{5L, "x"}, meta)).isNull();
        // 实体实例参数：idOf join
        assertThat(resolver.resolve(entityParam, new Object[]{new CompositeEntity(7L, "s2", 1)}, meta))
                .isEqualTo("7:s2");
        // 批量失效：实体实例收集为 join 串
        assertThat(resolver.resolveAll(entityParam, new Object[]{new CompositeEntity(7L, "s2", 1)}, meta))
                .containsExactly("7:s2");
    }

    @SuppressWarnings("unused")
    private Object query(Long orderId, String skuId) {
        return null;
    }

    @SuppressWarnings("unused")
    private Object queryMissing(Long orderId, String other) {
        return null;
    }

    @SuppressWarnings("unused")
    private Object queryByEntity(CompositeEntity item) {
        return null;
    }

    @Test
    void ambiguousSegmentValueShouldBeRejectedOnLoadAndSkippedOnEvict() {
        // 读路径：含 ':' 的复合段会造成键歧义，fail-fast 拒绝
        assertThatThrownBy(() -> cache.load(meta, List.of("a:b", "c"), null, true,
                () -> new CompositeEntity(1L, "s", 1)))
                .isInstanceOf(CacheKitException.class)
                .hasMessageContaining("':'");
        assertThat(l1.store).isEmpty();

        // 窥探路径：歧义值不可能已被缓存，按未命中处理
        assertThat(cache.peek(meta, List.of("a:b", "c")).state())
                .isEqualTo(TieredEntityCache.CachePeek.State.MISS);

        // 失效路径：绝不打断业务写，跳过歧义键
        cache.evictBatch(meta, List.of(List.of("a:b", "c"), List.of(5L, "s1")));
        // 无异常即通过；有效键照常组键（store 为空仅表示无缓存可删）
    }

    @Test
    void compositeSegmentWithNullPartShouldBypassCache() {
        // 批量读：null 段按无主键处理，槽直接完成（不抛）
        Object[] out = cache.loadBatch(meta, List.of(java.util.Arrays.asList(5L, null)), true, null,
                ids -> {
                    throw new AssertionError("null 段不应回源");
                });
        assertThat(out[0]).isNull();

        // 单条读：拒绝（与 null 主键同语义）
        assertThatThrownBy(() -> cache.load(meta, java.util.Arrays.asList(5L, null), null, true,
                () -> new CompositeEntity(5L, null, 1)))
                .isInstanceOf(CacheKitException.class)
                .hasMessageContaining("null");
    }

    @Test
    void broadcastKeyMatchingShouldAcceptCompositeKeys() {
        // 广播订阅端按前缀匹配键（主键段本身可能多段），复合键天然兼容
        EntityMetadata found = registry.findByBroadcastKey("ns:t_item:5:s1", "ns");
        assertThat(found).isNotNull();
        assertThat(found.entityType()).isEqualTo(CompositeEntity.class);
    }

    @Test
    void singlePrimaryKeyValuesMayStillContainColon() {
        // 单主键 String 值含 ':' 是既有合法用法（广播键按段匹配处理），复合约束不外溢
        assertThat(TieredEntityCache.idSegment("a:b")).isEqualTo("a:b");
        // 但复合（集合形式）路径的段值含 ':' 依旧拒绝
        assertThatThrownBy(() -> TieredEntityCache.idSegment(List.of("a:b")))
                .isInstanceOf(CacheKitException.class);
    }
}
