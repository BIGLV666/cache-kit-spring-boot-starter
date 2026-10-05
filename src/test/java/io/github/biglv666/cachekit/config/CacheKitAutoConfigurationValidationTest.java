package io.github.biglv666.cachekit.config;

import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.support.BootAutoconfigCompat;
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
                    BootAutoconfigCompat.redis(),
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
                        BootAutoconfigCompat.redis(),
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

    @Test
    void nonPositiveL2TtlShouldDisableL2InsteadOfFailingStartup() {
        // l2.ttl 非正值 = 禁用 L2 写入（仅告警），不得被 L1/L2 倒装校验拦截——
        // 否则任何正值的 l1.ttl（含默认 30s）都会让该文档化配置路径启动失败
        for (String ttl : new String[]{"0", "-1"}) {
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(
                            BootAutoconfigCompat.redis(),
                            CacheKitAutoConfiguration.class))
                    .withPropertyValues(
                            "spring.data.redis.host=localhost",
                            "spring.data.redis.port=6379",
                            "cache-kit.l2.ttl=" + ttl)
                    .run(ctx -> {
                        assertThat(ctx).as("l2.ttl=%s 时应正常启动", ttl).hasNotFailed();
                        assertThat(ctx.getBean(CacheKitProperties.class).getL2().getTtl())
                                .as("l2.ttl=%s 应保持非正值语义", ttl).isLessThanOrEqualTo(java.time.Duration.ZERO);
                    });
        }
    }

    @Test
    void refreshAheadNotBelowL1TtlShouldBeDisabledWithWarning() {
        // 预刷新窗口 >= l1.ttl 会导致每次读都触发刷新（等效每次读多一次后台 DB 查询）：
        // 启动告警并禁用（置零），而非 fail-fast 或放行
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        BootAutoconfigCompat.redis(),
                        CacheKitAutoConfiguration.class))
                .withPropertyValues(
                        "spring.data.redis.host=localhost",
                        "spring.data.redis.port=6379",
                        "cache-kit.l1.ttl=10s",
                        "cache-kit.l1.refresh-ahead=10s")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.getBean(CacheKitProperties.class).getL1().getRefreshAhead())
                            .isEqualTo(java.time.Duration.ZERO);
                });
    }

    @Test
    void validRefreshAheadShouldBeKept() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        BootAutoconfigCompat.redis(),
                        CacheKitAutoConfiguration.class))
                .withPropertyValues(
                        "spring.data.redis.host=localhost",
                        "spring.data.redis.port=6379",
                        "cache-kit.l1.ttl=30s",
                        "cache-kit.l1.refresh-ahead=10s")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.getBean(CacheKitProperties.class).getL1().getRefreshAhead())
                            .isEqualTo(java.time.Duration.ofSeconds(10));
                });
    }
}