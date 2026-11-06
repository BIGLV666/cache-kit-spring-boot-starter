package io.github.biglv666.cachekit.generic;

import io.github.biglv666.cachekit.annotation.CacheLoad;
import io.github.biglv666.cachekit.annotation.CacheObject;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.CacheKit;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.exception.CacheKitException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GenericCacheAspectTest {

    interface ConfigLoader {
        @CacheLoad(key = "#tenant + ':' + #name")
        TestValue load(String tenant, String name);

        @CacheLoad(key = "#id", cacheNull = false)
        TestValue loadNullable(long id);

        @CacheLoad(key = "#id", condition = "#enabled")
        TestValue loadConditional(long id, boolean enabled);

        @CacheLoad(key = "'all'")
        List<TestValue> loadAll();

        @CacheLoad(key = "''")
        TestValue badKey(long id);
    }

    interface GenericLoader<T> {
        @CacheLoad(key = "#id")
        T load(long id);

        @CacheLoad(key = "'all'")
        List<T> loadAll();
    }

    static class TestValueLoader implements GenericLoader<TestValue> {
        final AtomicInteger loads = new AtomicInteger();

        @Override
        public TestValue load(long id) {
            loads.incrementAndGet();
            return new TestValue("v" + id, 1);
        }

        @Override
        public List<TestValue> loadAll() {
            loads.incrementAndGet();
            return List.of(new TestValue("a", 1), new TestValue("b", 2));
        }
    }

    @CacheObject(prefix = "other-value")
    public static class OtherValue {
        public String name;

        public OtherValue() {
        }

        OtherValue(String name) {
            this.name = name;
        }
    }

    static class OtherLoader implements GenericLoader<OtherValue> {
        final AtomicInteger loads = new AtomicInteger();

        @Override
        public OtherValue load(long id) {
            loads.incrementAndGet();
            return new OtherValue("other-" + id);
        }

        @Override
        public List<OtherValue> loadAll() {
            loads.incrementAndGet();
            return List.of(new OtherValue("other"));
        }
    }

    interface ObjectLoader {
        @CacheLoad(key = "#id", value = TestValue.class)
        Object loadRaw(long id);
    }

    static class ObjectLoaderImpl implements ObjectLoader {
        final AtomicInteger loads = new AtomicInteger();

        @Override
        public Object loadRaw(long id) {
            loads.incrementAndGet();
            return new TestValue("raw-" + id, 1);
        }
    }

    /** 泛型类直接实例化（T 在运行期无法解析），只能靠 @CacheLoad.value 兜底 */
    static class UnresolvedListLoader<T> {
        final AtomicInteger loads = new AtomicInteger();

        @CacheLoad(key = "#id", value = TestValue.class)
        @SuppressWarnings("unchecked")
        public List<T> loadList(long id) {
            loads.incrementAndGet();
            return (List<T>) List.of(new TestValue("list-" + id, 1));
        }
    }

    interface FailingLoader {
        @CacheLoad(key = "#id")
        TestValue load(long id) throws IOException;
    }

    static class FailingLoaderImpl implements FailingLoader {
        final AtomicInteger loads = new AtomicInteger();

        @Override
        public TestValue load(long id) throws IOException {
            loads.incrementAndGet();
            throw new IOException("db down");
        }
    }

    private ConfigLoader proxy;
    private AtomicInteger loads;
    private TieredValueCache cache;
    private GenericCacheRegistry registry;
    private GenericCacheAspect aspect;

    @BeforeEach
    void setUp() {
        loads = new AtomicInteger();
        ConfigLoader target = (ConfigLoader) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{ConfigLoader.class},
                (p, method, args) -> switch (method.getName()) {
                    case "load" -> {
                        loads.incrementAndGet();
                        yield new TestValue(args[0] + ":" + args[1], 1);
                    }
                    case "loadNullable" -> {
                        loads.incrementAndGet();
                        yield ((Long) args[0]) == 0L ? null : new TestValue("v", 2);
                    }
                    case "loadConditional" -> {
                        loads.incrementAndGet();
                        yield new TestValue("conditional", 3);
                    }
                    case "loadAll" -> {
                        loads.incrementAndGet();
                        yield List.of(new TestValue("a", 1), new TestValue("b", 2));
                    }
                    case "badKey" -> {
                        loads.incrementAndGet();
                        yield new TestValue("bad", 4);
                    }
                    default -> null;
                });

        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        cache = new TieredValueCache(props, new CaffeineChannel(1024), null,
                new NoopInvalidationPublisher(), null, "", List.of());
        registry = new GenericCacheRegistry();
        aspect = new GenericCacheAspect(cache, registry);

        proxy = proxyFor(target, ConfigLoader.class);
    }

    /** 生产环境由 Advisor 自动代理；测试用 ProxyFactory 显式挂载同一 Advisor */
    private <T> T proxyFor(Object target, Class<T> type) {
        return proxyFor(target, type, false);
    }

    /** proxyTargetClass=true 模拟 Spring Boot 默认的 CGLIB 代理（接口类型调用会走桥接方法） */
    private <T> T proxyFor(Object target, Class<T> type, boolean proxyTargetClass) {
        ProxyFactory factory = new ProxyFactory(target);
        factory.setProxyTargetClass(proxyTargetClass);
        factory.addAdvisor(aspect);
        return type.cast(factory.getProxy());
    }

    @Test
    void hitShouldSkipLoader() {
        TestValue first = proxy.load("t1", "db");
        TestValue second = proxy.load("t1", "db");
        TestValue other = proxy.load("t1", "cache");

        assertThat(first).isEqualTo(second).isEqualTo(new TestValue("t1:db", 1));
        assertThat(other).isEqualTo(new TestValue("t1:cache", 1));
        assertThat(loads.get()).isEqualTo(2);
    }

    @Test
    void conditionFalseShouldBypassCache() {
        proxy.loadConditional(1L, false);
        proxy.loadConditional(1L, false);

        assertThat(loads.get()).isEqualTo(2);
    }

    @Test
    void conditionTrueShouldUseCache() {
        proxy.loadConditional(1L, true);
        proxy.loadConditional(1L, true);

        assertThat(loads.get()).isEqualTo(1);
    }

    @Test
    void nullShouldNotBeCachedWhenDisabled() {
        proxy.loadNullable(0L);
        proxy.loadNullable(0L);

        assertThat(loads.get()).isEqualTo(2);
    }

    @Test
    void genericListReturnTypeShouldBeCached() {
        List<TestValue> first = proxy.loadAll();
        List<TestValue> second = proxy.loadAll();

        assertThat(first).isEqualTo(second).hasSize(2);
        assertThat(loads.get()).isEqualTo(1);
    }

    @Test
    void withDbShouldBypassCache() {
        proxy.load("t1", "db");
        TestValue direct = CacheKit.withDb(() -> proxy.load("t1", "db"));

        assertThat(direct).isEqualTo(new TestValue("t1:db", 1));
        assertThat(loads.get()).isEqualTo(2);
    }

    @Test
    void blankKeyShouldFailFastRatherThanGuess() {
        assertThatThrownBy(() -> proxy.badKey(1L))
                .isInstanceOf(CacheKitException.class)
                .hasMessageContaining("@CacheLoad.key 表达式结果为空字符串");
    }

    @Test
    void genericTypeVariableShouldResolveAgainstConcreteSubclass() {
        TestValueLoader target = new TestValueLoader();
        GenericLoader<TestValue> loader = proxyFor(target, GenericLoader.class, true);

        TestValue first = loader.load(1L);
        TestValue second = loader.load(1L);

        assertThat(first).isEqualTo(second).isEqualTo(new TestValue("v1", 1));
        assertThat(second).isNotInstanceOf(java.util.Map.class);
        assertThat(target.loads).hasValue(1);
    }

    @Test
    void genericListTypeVariableShouldResolveAgainstConcreteSubclass() {
        TestValueLoader target = new TestValueLoader();
        GenericLoader<TestValue> loader = proxyFor(target, GenericLoader.class, true);

        loader.loadAll();
        List<TestValue> second = loader.loadAll();

        assertThat(second).hasSize(2);
        assertThat(second.get(0)).isInstanceOf(TestValue.class).isEqualTo(new TestValue("a", 1));
        assertThat(target.loads).hasValue(1);
    }

    @Test
    void twoTargetBindingsOfSameInterfaceShouldNotSharePlans() {
        TestValueLoader values = new TestValueLoader();
        OtherLoader others = new OtherLoader();
        GenericLoader<TestValue> valueLoader = proxyFor(values, GenericLoader.class, true);
        GenericLoader<OtherValue> otherLoader = proxyFor(others, GenericLoader.class, true);

        valueLoader.load(1L);
        otherLoader.load(1L);
        TestValue cachedValue = valueLoader.load(1L);
        OtherValue cachedOther = otherLoader.load(1L);

        assertThat(cachedValue).isEqualTo(new TestValue("v1", 1));
        assertThat(cachedOther.name).isEqualTo("other-1");
        assertThat(values.loads).hasValue(1);
        assertThat(others.loads).hasValue(1);
        assertThat(registry.prefixes()).contains("app-config", "other-value");
    }

    @Test
    void objectReturnWithExplicitValueShouldDeserializeToValueType() {
        ObjectLoaderImpl target = new ObjectLoaderImpl();
        ObjectLoader loader = proxyFor(target, ObjectLoader.class, true);

        assertThat(loader.loadRaw(1L)).isInstanceOf(TestValue.class);
        Object second = loader.loadRaw(1L);

        assertThat(second).isInstanceOf(TestValue.class).isEqualTo(new TestValue("raw-1", 1));
        assertThat(target.loads).hasValue(1);
    }

    @Test
    void unresolvedListTypeVariableShouldUseExplicitValue() {
        UnresolvedListLoader<TestValue> target = new UnresolvedListLoader<>();
        @SuppressWarnings("unchecked")
        UnresolvedListLoader<TestValue> loader = proxyFor(target, UnresolvedListLoader.class);

        loader.loadList(1L);
        List<TestValue> second = loader.loadList(1L);

        assertThat(second).hasSize(1);
        assertThat(second.get(0)).isInstanceOf(TestValue.class).isEqualTo(new TestValue("list-1", 1));
        assertThat(target.loads).hasValue(1);
    }

    @Test
    void checkedExceptionShouldPropagateAndNotBeCached() {
        FailingLoaderImpl target = new FailingLoaderImpl();
        FailingLoader loader = proxyFor(target, FailingLoader.class, true);

        assertThatThrownBy(() -> loader.load(1L))
                .isInstanceOf(IOException.class)
                .hasMessage("db down");
        assertThatThrownBy(() -> loader.load(1L))
                .isInstanceOf(IOException.class)
                .hasMessage("db down");

        assertThat(target.loads).hasValue(2);
        assertThat(cache.inflightCount()).isZero();
    }
}
