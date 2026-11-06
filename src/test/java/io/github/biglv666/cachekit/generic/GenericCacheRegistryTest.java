package io.github.biglv666.cachekit.generic;

import io.github.biglv666.cachekit.annotation.CacheLoad;
import io.github.biglv666.cachekit.annotation.CacheObject;
import io.github.biglv666.cachekit.core.GenericKeyCodec;
import io.github.biglv666.cachekit.exception.CacheKitException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenericCacheRegistryTest {

    @CacheObject(prefix = "app-config")
    static class ConflictValue {
    }

    @CacheObject
    static class DefaultPrefixValue {
    }

    interface Loader {
        @CacheLoad(key = "#id")
        TestValue load(long id);

        @CacheLoad(key = "'all'")
        List<TestValue> loadAll();

        @CacheLoad(key = "'raw'", value = TestValue.class)
        Object loadRaw();

        @CacheLoad(key = "'x'")
        String loadUnsupported();

        @CacheLoad(key = "'void'")
        void loadVoid();
    }

    interface GenericLoader<T> {
        @CacheLoad(key = "#id")
        T load(long id);
    }

    static class TestValueLoader implements GenericLoader<TestValue> {
        @Override
        public TestValue load(long id) {
            return new TestValue("v" + id, 1);
        }
    }

    @CacheObject(prefix = "interface-value")
    interface InterfaceValue {
    }

    @CacheObject(prefix = "abstract-value")
    abstract static class AbstractValue {
    }

    @CacheObject(prefix = "polymorphic-value")
    @com.fasterxml.jackson.annotation.JsonTypeInfo(
            use = com.fasterxml.jackson.annotation.JsonTypeInfo.Id.NAME, property = "type")
    interface PolymorphicValue {
    }

    @Test
    void shouldResolveMetadataAndDefaultPrefix() {
        GenericCacheRegistry registry = new GenericCacheRegistry();

        GenericCacheMetadata meta = registry.require(TestValue.class);
        assertThat(meta.prefix()).isEqualTo("app-config");
        assertThat(meta.ttl()).hasSeconds(60);
        assertThat(meta.cacheNull()).isTrue();

        GenericCacheMetadata defaultPrefix = registry.require(DefaultPrefixValue.class);
        assertThat(defaultPrefix.prefix()).isEqualTo("default_prefix_value");
    }

    @Test
    void shouldResolveContainerAndExplicitValueType() throws Exception {
        GenericCacheRegistry registry = new GenericCacheRegistry();

        Method listMethod = Loader.class.getMethod("loadAll");
        assertThat(registry.register(listMethod, listMethod.getAnnotation(CacheLoad.class)).valueType())
                .isEqualTo(TestValue.class);

        Method rawMethod = Loader.class.getMethod("loadRaw");
        assertThat(registry.register(rawMethod, rawMethod.getAnnotation(CacheLoad.class)).valueType())
                .isEqualTo(TestValue.class);
    }

    @Test
    void shouldFailFastWhenValueTypeCannotBeResolved() throws Exception {
        GenericCacheRegistry registry = new GenericCacheRegistry();
        Method method = Loader.class.getMethod("loadUnsupported");

        assertThatThrownBy(() -> registry.register(method, method.getAnnotation(CacheLoad.class)))
                .isInstanceOf(CacheKitException.class)
                .hasMessageContaining("@CacheLoad 无法从方法返回类型解析出 @CacheObject");
    }

    @Test
    void shouldFailFastOnVoidReturnType() throws Exception {
        GenericCacheRegistry registry = new GenericCacheRegistry();
        Method method = Loader.class.getMethod("loadVoid");

        assertThatThrownBy(() -> registry.register(method, method.getAnnotation(CacheLoad.class)))
                .isInstanceOf(CacheKitException.class)
                .hasMessageContaining("不能是 void");
    }

    @Test
    void shouldFailFastOnPrefixCollision() {
        GenericCacheRegistry registry = new GenericCacheRegistry();
        registry.require(TestValue.class);

        assertThatThrownBy(() -> registry.require(ConflictValue.class))
                .isInstanceOf(CacheKitException.class)
                .hasMessageContaining("通用缓存前缀冲突");
    }

    @Test
    void shouldResolveGenericInterfaceMethodAgainstTargetClass() throws Exception {
        GenericCacheRegistry registry = new GenericCacheRegistry();
        Method method = GenericLoader.class.getMethod("load", long.class);

        GenericCacheMetadata meta = registry.register(
                method, TestValueLoader.class, method.getAnnotation(CacheLoad.class));

        assertThat(meta.valueType()).isEqualTo(TestValue.class);
        assertThat(meta.prefix()).isEqualTo("app-config");
    }

    @Test
    void twoArgRegisterShouldFallBackToDeclaringClass() throws Exception {
        GenericCacheRegistry registry = new GenericCacheRegistry();
        Method method = Loader.class.getMethod("load", long.class);

        GenericCacheMetadata meta = registry.register(method, method.getAnnotation(CacheLoad.class));

        assertThat(meta.valueType()).isEqualTo(TestValue.class);
    }

    @Test
    void shouldRejectInterfaceCacheObjectWithoutJsonTypeInfo() {
        GenericCacheRegistry registry = new GenericCacheRegistry();

        assertThatThrownBy(() -> registry.require(InterfaceValue.class))
                .isInstanceOf(CacheKitException.class)
                .hasMessageContaining("不能标注在接口或抽象类上")
                .hasMessageContaining("@JsonTypeInfo");
    }

    @Test
    void shouldRejectAbstractCacheObjectWithoutJsonTypeInfo() {
        GenericCacheRegistry registry = new GenericCacheRegistry();

        assertThatThrownBy(() -> registry.require(AbstractValue.class))
                .isInstanceOf(CacheKitException.class)
                .hasMessageContaining("不能标注在接口或抽象类上");
    }

    @Test
    void shouldAllowInterfaceCacheObjectWithJsonTypeInfo() {
        GenericCacheRegistry registry = new GenericCacheRegistry();

        GenericCacheMetadata meta = registry.require(PolymorphicValue.class);

        assertThat(meta.prefix()).isEqualTo("polymorphic-value");
        assertThat(meta.valueType()).isEqualTo(PolymorphicValue.class);
    }

    @Test
    void shouldMatchBroadcastKeyWithNamespaceAndCustomizerSegments() {
        GenericCacheRegistry registry = new GenericCacheRegistry();
        registry.require(TestValue.class);

        assertThat(registry.findByBroadcastKey("ns:" + GenericKeyCodec.SEGMENT + ":app-config:k", "ns"))
                .isNotNull();
        assertThat(registry.findByBroadcastKey("tenant:ns:" + GenericKeyCodec.SEGMENT + ":app-config:k", "ns"))
                .isNotNull();
        assertThat(registry.findByBroadcastKey("other:" + GenericKeyCodec.SEGMENT + ":app-config:k", "ns"))
                .isNull();
        assertThat(registry.findByBroadcastKey("ns:" + GenericKeyCodec.SEGMENT + ":unknown:k", "ns"))
                .isNull();
        assertThat(registry.findByBroadcastKey(GenericKeyCodec.SEGMENT + ":app-config:k", ""))
                .isNotNull();
        assertThat(registry.findByBroadcastKey(GenericKeyCodec.SEGMENT + ":app-config:k", "ns"))
                .isNull();
    }

    @Test
    void broadcastMessageShouldRoundTrip() {
        String key = "ns:__gk__:app-config:k";
        String payload = GenericKeyCodec.wrap(key);

        assertThat(GenericKeyCodec.isGenericMessage(payload)).isTrue();
        assertThat(GenericKeyCodec.unwrap(payload)).isEqualTo(key);
        assertThat(GenericKeyCodec.isGenericMessage(key)).isFalse();
    }
}
