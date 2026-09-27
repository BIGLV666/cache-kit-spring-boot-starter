package io.github.biglv666.cachekit.warmup;

import io.github.biglv666.cachekit.annotation.CacheWarmup;
import io.github.biglv666.cachekit.config.CacheKitAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 预热执行器：就绪后执行一次全部 @CacheWarmup 方法，异常隔离，可用配置关闭。
 */
class CacheWarmupRunnerTest {

    @Configuration
    static class WarmupConfig {

        final AtomicInteger plainInvoked = new AtomicInteger();
        final AtomicInteger failingInvoked = new AtomicInteger();

        @CacheWarmup
        void loadHotData() {
            plainInvoked.incrementAndGet();
        }

        @CacheWarmup
        void brokenWarmup() {
            failingInvoked.incrementAndGet();
            throw new IllegalStateException("warmup failed");
        }
    }

    private static int until(AtomicInteger counter, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (counter.get() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        return counter.get();
    }

    @Test
    void warmupMethodsShouldRunOnceAfterContextStart() throws Exception {
        WarmupConfig config = new WarmupConfig();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CacheKitAutoConfiguration.class))
                .withBean("warmupConfig", WarmupConfig.class, () -> config)
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(until(config.plainInvoked, 1)).as("预热方法应被执行一次").isEqualTo(1);
                    // 异常隔离：抛异常的预热方法不影响其他方法执行
                    assertThat(until(config.failingInvoked, 1)).isEqualTo(1);
                });
    }

    @Test
    void warmupDisabledShouldNotRun() throws Exception {
        WarmupConfig config = new WarmupConfig();
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CacheKitAutoConfiguration.class))
                .withBean("warmupConfig", WarmupConfig.class, () -> config)
                .withPropertyValues("cache-kit.warmup.enabled=false")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.getBean(CacheKitAutoConfiguration.class)).isNotNull();
                    Thread.sleep(300);
                    assertThat(config.plainInvoked.get()).as("关闭预热后不应执行").isZero();
                });
    }
}
