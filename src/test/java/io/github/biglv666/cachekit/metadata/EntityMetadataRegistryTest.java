package io.github.biglv666.cachekit.metadata;

import io.github.biglv666.cachekit.annotation.CacheEntity;
import io.github.biglv666.cachekit.annotation.CacheId;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.model.MpUserEntity;
import io.github.biglv666.cachekit.model.PlainDto;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityMetadataRegistryTest {

    private final EntityMetadataRegistry registry = new EntityMetadataRegistry();

    @Test
    void ownAnnotationEntityShouldParsePrefixAndIdAndTtl() {
        EntityMetadata meta = registry.require(UserEntity.class);
        assertThat(meta.prefix()).isEqualTo("user_entity");
        assertThat(meta.idField().getName()).isEqualTo("userId");
        assertThat(meta.ttl()).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    void multipleCacheIdShouldFailFast() {
        // 歧义主键会缓存键错位读回错行数据：多个 @CacheId 必须拒绝启动
        assertThatThrownBy(() -> registry.require(io.github.biglv666.cachekit.model.DualIdEntity.class))
                .isInstanceOf(CacheKitException.class)
                .hasMessageContaining("@CacheId");
    }

    @Test
    void missingTableIdWithNamedIdFieldShouldUseFallback() {
        // 无任何主键注解但有一个名为 id 的字段：兜底生效
        EntityMetadata meta = registry.require(io.github.biglv666.cachekit.model.PlainIdEntity.class);
        assertThat(meta.idField().getName()).isEqualTo("id");
    }

    @Test
    void mybatisPlusEntityShouldParseWithoutOwnAnnotations() {
        EntityMetadata meta = registry.require(MpUserEntity.class);
        assertThat(meta.prefix()).isEqualTo("t_mp_user");
        assertThat(meta.idField().getName()).isEqualTo("userId");
        assertThat(meta.ttl()).isNull();
    }

    @Test
    void plainDtoShouldNotBeCacheable() {
        assertThat(registry.find(PlainDto.class)).isNull();
        assertThatThrownBy(() -> registry.require(PlainDto.class))
                .isInstanceOf(CacheKitException.class);
    }

    @Test
    void resultShouldBeCachedPerClass() {
        assertThat(registry.find(UserEntity.class)).isSameAs(registry.find(UserEntity.class));
    }

    @Test
    void idOfShouldReadFieldValue() {
        EntityMetadata meta = registry.require(UserEntity.class);
        UserEntity user = new UserEntity(7L, "lv");
        assertThat(meta.idOf(user)).isEqualTo(7L);
    }

    @Test
    void duplicatePrefixShouldFailFast() {
        // 同前缀实体共享键空间会互相覆盖缓存值（字段子集静默缺字段）：第二次接入必须抛错
        assertThat(registry.require(DupEntityA.class).prefix()).isEqualTo("dup_table");
        assertThatThrownBy(() -> registry.require(DupEntityB.class))
                .isInstanceOf(CacheKitException.class)
                .hasMessageContaining("dup_table")
                .hasMessageContaining(DupEntityA.class.getName());
        // 冲突实体不缓存否定结果：每次 find 持续 fail-fast
        assertThatThrownBy(() -> registry.find(DupEntityB.class))
                .isInstanceOf(CacheKitException.class);
    }

    @Test
    void findByPrefixShouldUseIndex() {
        assertThat(registry.findByPrefix("user_entity")).isNull();
        registry.require(UserEntity.class);
        assertThat(registry.findByPrefix("user_entity")).isNotNull();
        assertThat(registry.findByPrefix("no_such_table")).isNull();
    }

    @CacheEntity(prefix = "dup_table")
    static class DupEntityA {
        @CacheId
        Long id;
    }

    @CacheEntity(prefix = "dup_table")
    static class DupEntityB {
        @CacheId
        Long id;
    }
}
