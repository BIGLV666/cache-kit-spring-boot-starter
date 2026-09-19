package io.github.biglv666.cachekit.config;

import io.github.biglv666.cachekit.annotation.CacheHandle;
import io.github.biglv666.cachekit.channel.RedisChannel;
import io.github.biglv666.cachekit.core.EntityCache;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 降级路径：
 * 1. spring-data-redis 在类路径但容器中没有 RedisConnectionFactory Bean 时，必须降级为纯 L1
 *    启动成功（历史缺陷：getObject() 抛 NoSuchBeanDefinitionException 整个上下文起不来）；
 * 2. cache-kit.enabled=false 时 @CacheHandle 字段注入空操作句柄而非留 null（业务首次调用 NPE）；
 * 3. key-namespace 未配置时自动派生自 spring.application.name。
 */
class CacheKitAutoConfigurationDegradationTest {

    @Test
    void classpathWithoutRedisFactoryShouldDegradeToL1Only() {
        // 测试类路径自带 spring-data-redis（类存在），但不注册 RedisAutoConfiguration/工厂 Bean
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CacheKitAutoConfiguration.class))
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.getBeansOfType(RedisChannel.class)).isEmpty();
                    assertThat(ctx.getBean(TieredEntityCache.class)).isNotNull();
                });
    }

    @Test
    void disabledKitShouldInjectNoopHandleInsteadOfNull() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CacheKitDisabledConfiguration.class))
                .withPropertyValues("cache-kit.enabled=false")
                .withUserConfiguration(HandleHost.class)
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    HandleHost host = ctx.getBean(HandleHost.class);
                    AtomicInteger calls = new AtomicInteger();
                    UserEntity loaded = host.handle.get(1L, () -> {
                        calls.incrementAndGet();
                        return new UserEntity(1L, "lv");
                    });
                    assertThat(loaded).isEqualTo(new UserEntity(1L, "lv"));
                    host.handle.evict(1L);
                    assertThat(calls.get()).as("空操作句柄：读直查 DB、失效空操作").isEqualTo(1);
                });
    }

    @Test
    void namespaceShouldDeriveFromApplicationName() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CacheKitAutoConfiguration.class))
                .withPropertyValues("spring.application.name=demo-app")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.getBean(CacheKitProperties.class).getKeyNamespace()).isEqualTo("demo-app");
                });
    }

    @Configuration
    static class HandleHost {
        @CacheHandle(UserEntity.class)
        EntityCache<UserEntity> handle;
    }
}
