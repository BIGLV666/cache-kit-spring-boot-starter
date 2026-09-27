package io.github.biglv666.cachekit.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * binlog 防呆：binlog.enabled=true 但类路径缺 connector 时必须快速失败，
 * 绝不允许"以为开了 binlog 失效其实没开"的静默失效（0.3.0 起 connector 为 optional 依赖）。
 */
class BinlogConnectorGuardTest {

    @Test
    void missingConnectorShouldFailFastWithDependencyHint() {
        assertThatThrownBy(() -> CacheKitAutoConfiguration.CacheKitBinlogConnectorGuard
                .assertBinlogConnectorPresent(false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mysql-binlog-connector-java")
                .hasMessageContaining("com.zendesk")
                .hasMessageContaining("cache-kit.binlog.enabled=false");
    }

    @Test
    void presentConnectorShouldPass() {
        assertThatCode(() -> CacheKitAutoConfiguration.CacheKitBinlogConnectorGuard
                .assertBinlogConnectorPresent(true)).doesNotThrowAnyException();
    }

    @Test
    void enabledWithConnectorOnClasspathShouldExposeGuardBean() {
        // 本模块测试类路径自带 connector（optional 依赖对模块自身可见）：enabled=true 时
        // Guard 与 BinlogConfiguration 同时装配，上下文正常启动且 Guard Bean 存在
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CacheKitAutoConfiguration.class))
                .withPropertyValues("cache-kit.binlog.enabled=true",
                        "cache-kit.binlog.host=localhost", "cache-kit.binlog.port=3307",
                        "cache-kit.binlog.database=test", "cache-kit.binlog.username=root",
                        "cache-kit.binlog.password=root")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.containsBean("binlogConnectorPresenceGuard")).isTrue();
                });
    }

    @Test
    void disabledBinlogShouldNotInstallGuard() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CacheKitAutoConfiguration.class))
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.containsBean("binlogConnectorPresenceGuard")).isFalse();
                });
    }
}
