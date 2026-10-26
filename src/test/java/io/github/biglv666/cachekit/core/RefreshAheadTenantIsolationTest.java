package io.github.biglv666.cachekit.core;

import io.github.biglv666.cachekit.channel.CacheEntry;
import io.github.biglv666.cachekit.channel.L1Channel;
import io.github.biglv666.cachekit.config.CacheKitAutoConfiguration;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L1 预刷新与线程上下文隔离：
 * 1. 刷新任务必须复用触发读时捕获的完整键，不能在没有租户 ThreadLocal 的线程池里重新推导，
 *    否则会把租户数据写到无段键（跨租户串数据）；
 * 2. 上下文敏感的 {@link CacheKeyCustomizer} 与 refresh-ahead 同时配置时，自动装配必须告警并禁用；
 * 3. 只有显式声明 {@code contextSensitive()=false} 的常量段实现才允许继续开启。
 */
class RefreshAheadTenantIsolationTest {

    static final ThreadLocal<String> TENANT = new ThreadLocal<>();

    /** 模拟多租户读路径：段值来自 ThreadLocal */
    static class TenantCustomizer implements CacheKeyCustomizer {
        @Override
        public String segment() {
            return TENANT.get();
        }
    }

    /** 常量段：与调用线程无关，可安全放行预刷新 */
    static class ConstantCustomizer implements CacheKeyCustomizer {
        @Override
        public String segment() {
            return "fixed";
        }

        @Override
        public boolean contextSensitive() {
            return false;
        }
    }

    /** 可控 L1：剩余 TTL 由测试设定，便于确定性触发预刷新 */
    static class ControllableL1 implements L1Channel {
        final Map<String, String> store = new ConcurrentHashMap<>();
        volatile long remaining = Duration.ofSeconds(1).toNanos();

        @Override
        public CacheEntry get(String key) {
            String raw = store.get(key);
            return raw == null ? CacheEntry.miss() : CacheEntry.of(raw);
        }

        @Override
        public void put(String key, String json, Duration ttl) {
            store.put(key, json);
        }

        @Override
        public void evict(String key) {
            store.remove(key);
        }

        @Override
        public long remainingTtlNanos(String key) {
            return store.containsKey(key) ? remaining : -1;
        }
    }

    /** 捕获任务不立即执行：在主线程触发刷新，再切换到无租户上下文的线程执行 */
    static class CapturingExecutor implements java.util.concurrent.Executor {
        final List<Runnable> tasks = new CopyOnWriteArrayList<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }
    }

    private ControllableL1 l1;
    private InMemoryChannel l2;
    private CapturingExecutor executor;
    private TieredEntityCache cache;
    private EntityMetadata meta;

    @BeforeEach
    void setUp() {
        CacheKitProperties props = new CacheKitProperties();
        props.getL1().setTtl(Duration.ofSeconds(10));
        props.getL1().setRefreshAhead(Duration.ofSeconds(4));
        props.getL2().setJitter(Duration.ZERO);
        l1 = new ControllableL1();
        l2 = new InMemoryChannel();
        executor = new CapturingExecutor();
        cache = new TieredEntityCache(props, l1, l2, new NoopInvalidationPublisher(), null,
                "", List.of(new TenantCustomizer()));
        cache.setRefreshAheadExecutor(executor);
        meta = new EntityMetadataRegistry().require(UserEntity.class);
    }

    @AfterEach
    void tearDown() {
        TENANT.remove();
    }

    @Test
    void refreshTaskShouldReuseCapturedKeyAndNeverWriteContextlessKey() throws Exception {
        TENANT.set("t1");
        l1.store.put("t1:user_entity:1", "{\"userId\":1,\"userName\":\"stale\"}");
        AtomicInteger loaderRuns = new AtomicInteger();

        Object value = cache.load(meta, 1L, null, true, () -> {
            loaderRuns.incrementAndGet();
            return new UserEntity(1L, "fresh");
        });
        assertThat(((UserEntity) value).getUserName()).as("读路径拿未过期旧值").isEqualTo("stale");
        assertThat(loaderRuns.get()).as("L1 命中不应同步回源").isZero();
        assertThat(executor.tasks).hasSize(1);

        // 模拟线程池：没有调用方租户上下文；旧实现会重新推导成 user_entity:1
        TENANT.remove();
        Thread refresher = new Thread(executor.tasks.get(0), "refresh-ahead-test");
        refresher.start();
        refresher.join(5_000);

        assertThat(l1.store).containsKey("t1:user_entity:1");
        assertThat(l1.store.get("t1:user_entity:1")).contains("fresh");
        assertThat(l1.store).as("绝不能写入无租户段的键").doesNotContainKey("user_entity:1");
        assertThat(l2.store).containsKey("t1:user_entity:1");
        assertThat(l2.store).as("L2 同样不能写入无租户段的键").doesNotContainKey("user_entity:1");
        assertThat(loaderRuns.get()).as("刷新任务应完成一次回源").isEqualTo(1);
    }

    @Test
    void contextSensitiveCustomizerShouldDisableRefreshAheadAtStartup() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CacheKitAutoConfiguration.class))
                .withUserConfiguration(TenantCustomizerConfig.class)
                .withPropertyValues(
                        "cache-kit.l1.ttl=30s",
                        "cache-kit.l1.refresh-ahead=5s")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.getBean(CacheKitProperties.class).getL1().getRefreshAhead())
                            .as("ThreadLocal 租户段 + 预刷新必须自动禁用")
                            .isEqualTo(Duration.ZERO);
                });
    }

    @Test
    void constantCustomizerShouldKeepRefreshAheadEnabled() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CacheKitAutoConfiguration.class))
                .withUserConfiguration(ConstantCustomizerConfig.class)
                .withPropertyValues(
                        "cache-kit.l1.ttl=30s",
                        "cache-kit.l1.refresh-ahead=5s")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.getBean(CacheKitProperties.class).getL1().getRefreshAhead())
                            .as("显式声明 contextSensitive=false 的常量段实现可继续使用预刷新")
                            .isEqualTo(Duration.ofSeconds(5));
                });
    }

    @Configuration
    static class TenantCustomizerConfig {
        @Bean
        CacheKeyCustomizer tenantCustomizer() {
            return new TenantCustomizer();
        }
    }

    @Configuration
    static class ConstantCustomizerConfig {
        @Bean
        CacheKeyCustomizer constantCustomizer() {
            return new ConstantCustomizer();
        }
    }
}
