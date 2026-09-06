package io.github.biglv666.cachekit.metadata;

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
        assertThat(meta.keyOf(1L)).isEqualTo("user_entity:1");
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
}
