package io.github.biglv666.cachekit.config;

import io.github.biglv666.cachekit.exception.CacheKitException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 启动期校验：L1 TTL 倒挂必须 fail-fast，错误信息给出明确的修正指引。
 */
class CacheKitAutoConfigurationValidationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration.class,
                    CacheKitAutoConfiguration.class))
            .withPropertyValues(
                    "spring.data.redis.host=localhost",
                    "spring.data.redis.port=6379",
                    "cache-kit.l1.ttl=60s",
                    "cache-kit.l2.ttl=30s");

    @Test
    void invertedTtlShouldFailFast() {
        runner.run(ctx -> {
            assertThat(ctx).hasFailed();
            assertThat(ctx.getStartupFailure())
                    .hasRootCauseInstanceOf(CacheKitException.class);
            String message = String.valueOf(ctx.getStartupFailure().getCause().getMessage());
            assertThat(message).contains("cache-kit.l1.ttl").contains("cache-kit.l2.ttl");
        });
    }

    @Test
    void correctTtlShouldStart() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration.class,
                        CacheKitAutoConfiguration.class))
                .withPropertyValues(
                        "spring.data.redis.host=localhost",
                        "spring.data.redis.port=6379",
                        "cache-kit.l1.ttl=30s",
                        "cache-kit.l2.ttl=10m")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.getBean(CacheKitProperties.class)).isNotNull();
                });
    }
}
