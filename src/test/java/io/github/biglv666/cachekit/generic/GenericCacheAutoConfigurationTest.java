package io.github.biglv666.cachekit.generic;

import io.github.biglv666.cachekit.annotation.CacheHandle;
import io.github.biglv666.cachekit.annotation.CacheLoad;
import io.github.biglv666.cachekit.config.CacheKitAutoConfiguration;
import io.github.biglv666.cachekit.config.CacheKitDisabledConfiguration;
import io.github.biglv666.cachekit.core.CacheStore;
import io.github.biglv666.cachekit.handle.NoopCacheStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class GenericCacheAutoConfigurationTest {

    @Configuration
    @EnableAspectJAutoProxy(proxyTargetClass = true)
    static class Config {
        @Bean
        Loader loader() {
            return new Loader();
        }

        @Bean
        Holder holder() {
            return new Holder();
        }

        @Bean
        ListHolder listHolder() {
            return new ListHolder();
        }

        @Bean
        GenericListHolder genericListHolder() {
            return new GenericListHolder();
        }

        @Bean
        InterfaceApi interfaceApi() {
            return new InterfaceApiImpl();
        }
    }

    static class Loader {
        static final AtomicInteger CALLS = new AtomicInteger();

        @CacheLoad(key = "#id")
        public TestValue load(long id) {
            CALLS.incrementAndGet();
            return new TestValue("v" + id, (int) id);
        }
    }

    static class Holder {
        @CacheHandle(TestValue.class)
        CacheStore<TestValue> store;
    }

    static class ListHolder {
        @CacheHandle(TestValue.class)
        CacheStore<java.util.List<TestValue>> store;
    }

    /** 泛型字段声明在父类：必须按具体子类的实参解析，否则读回 LinkedHashMap */
    static class GenericBase<T> {
        @CacheHandle(TestValue.class)
        CacheStore<java.util.List<T>> store;
    }

    static class GenericListHolder extends GenericBase<TestValue> {
    }

    interface InterfaceApi {
        @CacheLoad(key = "#id")
        TestValue load(long id);
    }

    static class InterfaceApiImpl implements InterfaceApi {
        static final AtomicInteger CALLS = new AtomicInteger();

        @Override
        public TestValue load(long id) {
            CALLS.incrementAndGet();
            return new TestValue("interface-" + id, 1);
        }
    }

    @Configuration
    static class BadConfig {
        @Bean
        BadLoader badLoader() {
            return new BadLoader();
        }
    }

    /** 返回 Object 且未指定 value：无法解析值类型，必须在启动期 fail-fast */
    static class BadLoader {
        @CacheLoad(key = "#id")
        public Object load(long id) {
            return null;
        }
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(AopAutoConfiguration.class,
                        CacheKitAutoConfiguration.class))
                .withUserConfiguration(Config.class);
    }

    @Test
    void shouldWireAspectAndCacheStore() {
        Loader.CALLS.set(0);
        runner().run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ctx).hasSingleBean(GenericCacheRegistry.class);
            assertThat(ctx).hasSingleBean(TieredValueCache.class);

            GenericCacheRegistry registry = ctx.getBean(GenericCacheRegistry.class);
            assertThat(registry.prefixes())
                    .as("启动期扫描（含接口方法）应在单例实例化后完成注册")
                    .contains("app-config");
            assertThat(registry.findByPrefix("app-config")).isNotNull();

            Loader loader = ctx.getBean(Loader.class);
            loader.load(1L);
            loader.load(1L);
            assertThat(Loader.CALLS).hasValue(1);

            Holder holder = ctx.getBean(Holder.class);
            assertThat(holder.store).isNotNull().isNotInstanceOf(NoopCacheStore.class);
            holder.store.put("k", new TestValue("stored", 9));
            assertThat(holder.store.get("k")).isEqualTo(new TestValue("stored", 9));

            ListHolder listHolder = ctx.getBean(ListHolder.class);
            java.util.List<TestValue> list = java.util.List.of(new TestValue("a", 1), new TestValue("b", 2));
            listHolder.store.put("list", list);
            assertThat(listHolder.store.get("list")).isEqualTo(list);

            GenericListHolder genericListHolder = ctx.getBean(GenericListHolder.class);
            genericListHolder.store.put("generic-list", list);
            java.util.List<TestValue> fromGenericField = genericListHolder.store.get("generic-list");
            assertThat(fromGenericField).isEqualTo(list);
            assertThat(fromGenericField.get(0)).isInstanceOf(TestValue.class);
        });
    }

    @Test
    void interfaceDeclaredCacheLoadShouldBeIntercepted() {
        InterfaceApiImpl.CALLS.set(0);
        runner().run(ctx -> {
            assertThat(ctx).hasNotFailed();
            InterfaceApi api = ctx.getBean(InterfaceApi.class);

            assertThat(api.load(1L)).isInstanceOf(TestValue.class);
            TestValue second = api.load(1L);

            assertThat(second).isInstanceOf(TestValue.class).isEqualTo(new TestValue("interface-1", 1));
            assertThat(InterfaceApiImpl.CALLS).hasValue(1);
        });
    }

    @Test
    void unresolvableLoaderShouldFailAtStartupAfterScan() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(AopAutoConfiguration.class,
                        CacheKitAutoConfiguration.class))
                .withUserConfiguration(BadConfig.class)
                .run(ctx -> {
                    assertThat(ctx).hasFailed();
                    assertThat(ctx.getStartupFailure())
                            .as("延迟扫描不得削弱启动期 fail-fast")
                            .isInstanceOf(io.github.biglv666.cachekit.exception.CacheKitException.class);
                });
    }

    @Test
    void fullyDisabledCacheKitShouldInjectNoopStore() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CacheKitDisabledConfiguration.class))
                .withUserConfiguration(Config.class)
                .withPropertyValues("cache-kit.enabled=false")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    Holder holder = ctx.getBean(Holder.class);
                    assertThat(holder.store).isInstanceOf(NoopCacheStore.class);
                    assertThat(holder.store.get("k", () -> new TestValue("direct", 1)))
                            .isEqualTo(new TestValue("direct", 1));
                });
    }

    @Test
    void disabledGenericCacheShouldExecuteMethodAndInjectNoopStore() {
        Loader.CALLS.set(0);
        runner().withPropertyValues("cache-kit.generic.enabled=false").run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ctx).doesNotHaveBean(GenericCacheRegistry.class);
            assertThat(ctx).doesNotHaveBean(TieredValueCache.class);

            Loader loader = ctx.getBean(Loader.class);
            loader.load(1L);
            loader.load(1L);
            assertThat(Loader.CALLS).hasValue(2);

            Holder holder = ctx.getBean(Holder.class);
            assertThat(holder.store).isInstanceOf(NoopCacheStore.class);
            assertThat(holder.store.get("k", () -> new TestValue("direct", 1)))
                    .isEqualTo(new TestValue("direct", 1));
        });
    }
}
