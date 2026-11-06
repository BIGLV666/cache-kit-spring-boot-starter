package io.github.biglv666.cachekit.generic;

import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.core.BroadcastApplier;
import io.github.biglv666.cachekit.core.GenericKeyCodec;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import io.github.biglv666.cachekit.support.JsonCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class GenericBroadcastApplierTest {

    private CaffeineChannel l1;
    private GenericCacheRegistry genericRegistry;
    private BroadcastApplier applier;

    @BeforeEach
    void setUp() {
        l1 = new CaffeineChannel(1024);
        genericRegistry = new GenericCacheRegistry();
        genericRegistry.require(TestValue.class);
        applier = new BroadcastApplier(l1, new EntityMetadataRegistry(), genericRegistry, "ns");
    }

    @Test
    void shouldEvictRegisteredGenericKey() {
        String key = "ns:" + GenericKeyCodec.SEGMENT + ":app-config:k";
        l1.put(key, JsonCodec.write(new TestValue("v", 1)), Duration.ofMinutes(1));

        assertThat(applier.apply(GenericKeyCodec.wrap(key))).isTrue();
        assertThat(l1.get(key).hit()).isFalse();
    }

    @Test
    void shouldIgnoreUnknownGenericPrefix() {
        String key = "ns:" + GenericKeyCodec.SEGMENT + ":unknown:k";
        l1.put(key, JsonCodec.write(new TestValue("v", 1)), Duration.ofMinutes(1));

        assertThat(applier.apply(GenericKeyCodec.wrap(key))).isFalse();
        assertThat(l1.get(key).hit()).isTrue();
    }

    @Test
    void shouldIgnoreWrongNamespace() {
        String key = "other:" + GenericKeyCodec.SEGMENT + ":app-config:k";
        l1.put(key, JsonCodec.write(new TestValue("v", 1)), Duration.ofMinutes(1));

        assertThat(applier.apply(GenericKeyCodec.wrap(key))).isFalse();
        assertThat(l1.get(key).hit()).isTrue();
    }

    @Test
    void entityBroadcastShouldStillWork() {
        EntityMetadataRegistry entityRegistry = new EntityMetadataRegistry();
        entityRegistry.require(UserEntity.class);
        BroadcastApplier entityApplier = new BroadcastApplier(l1, entityRegistry, genericRegistry, "");
        String key = "user_entity:1";
        l1.put(key, JsonCodec.write(new UserEntity(1L, "v")), Duration.ofMinutes(1));

        assertThat(entityApplier.apply(key)).isTrue();
        assertThat(l1.get(key).hit()).isFalse();
    }
}
