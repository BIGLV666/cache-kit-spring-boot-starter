package io.github.biglv666.cachekit.generic;

import com.fasterxml.jackson.databind.JavaType;
import io.github.biglv666.cachekit.annotation.CacheLoad;
import io.github.biglv666.cachekit.channel.CacheChannel;
import io.github.biglv666.cachekit.channel.CacheEntry;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.channel.RedisChannel;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.support.JsonCodec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 通用缓存读路径基准（Testcontainers redis:7，非 JMH，量级参考）：
 * L1 命中、L2 命中、miss→loader、@CacheLoad 切面命中（含 SpEL key 解析）。
 * 输出 ops/s 与 P50/P95/P99，供与实体缓存基线对比。
 */
class GenericCacheBenchmarkIntegrationTest {

    private static final int KEYS = 1_000;
    private static final int WARMUP_OPS = 2_000;

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate template;

    interface Loader {
        @CacheLoad(key = "#id")
        TestValue load(long id);
    }

    static class LoaderImpl implements Loader {
        final java.util.concurrent.atomic.AtomicInteger calls =
                new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public TestValue load(long id) {
            calls.incrementAndGet();
            return value(id);
        }
    }

    @BeforeAll
    static void startRedis() {
        boolean dockerAvailable;
        try {
            dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            dockerAvailable = false;
        }
        Assumptions.assumeTrue(dockerAvailable, "无 Docker 环境，跳过通用缓存 benchmark");
        try {
            redis = new GenericContainer<>("redis:7").withExposedPorts(6379);
            redis.start();
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "Redis 容器启动失败，跳过: " + e.getMessage());
        }
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(redis.getHost(), redis.getMappedPort(6379)));
        factory.afterPropertiesSet();
        template = new StringRedisTemplate(factory);
        template.afterPropertiesSet();
    }

    @AfterAll
    static void shutdown() {
        if (factory != null) {
            factory.destroy();
        }
        if (redis != null) {
            redis.stop();
        }
    }

    private static CacheChannel alwaysMiss() {
        return new CacheChannel() {
            @Override
            public CacheEntry get(String key) {
                return CacheEntry.miss();
            }

            @Override
            public void put(String key, String json, Duration ttl) {
            }

            @Override
            public void evict(String key) {
            }
        };
    }

    private static TieredValueCache cache(CacheChannel l1, CacheChannel l2) {
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        return new TieredValueCache(props, l1, l2, new NoopInvalidationPublisher(), null, "", List.of());
    }

    private static TestValue value(long id) {
        return new TestValue("v" + id, (int) id);
    }

    /** 固定操作数测吞吐与延迟分位；返回 ops/s */
    private static double bench(String name, int ops, Runnable op) {
        for (int i = 0; i < Math.min(WARMUP_OPS, ops); i++) {
            op.run();
        }
        long[] latencies = new long[ops];
        long t0 = System.nanoTime();
        for (int i = 0; i < ops; i++) {
            long s = System.nanoTime();
            op.run();
            latencies[i] = System.nanoTime() - s;
        }
        double seconds = (System.nanoTime() - t0) / 1e9;
        double opsPerSec = ops / seconds;
        Arrays.sort(latencies);
        System.out.printf("ROW|%-42s|%,14.0f ops/s|P50=%6.2fus|P95=%6.2fus|P99=%6.2fus%n",
                name, opsPerSec,
                latencies[ops / 2] / 1000.0,
                latencies[(int) (ops * 0.95)] / 1000.0,
                latencies[(int) Math.min(ops - 1, ops * 0.99)] / 1000.0);
        return opsPerSec;
    }

    @Test
    void readPathBenchmark() {
        GenericCacheMetadata meta = new GenericCacheMetadata(TestValue.class, "app-config", null, true);
        JavaType valueType = JsonCodec.constructType(TestValue.class);
        AtomicLong cursor = new AtomicLong();

        // 1) L1 命中（真实 Caffeine，预热全部键）
        TieredValueCache l1Cache = cache(new CaffeineChannel(10_000), new RedisChannel(template));
        for (long id = 1; id <= KEYS; id++) {
            l1Cache.put(meta, String.valueOf(id), value(id), null);
        }
        double l1Ops = bench("generic L1 hit", 200_000, () ->
                l1Cache.load(meta, String.valueOf(cursor.getAndIncrement() % KEYS + 1),
                        valueType, null, true, () -> {
                            throw new AssertionError("L1 不应回源");
                        }));

        // 2) L2 命中（L1 恒 miss：Redis GET + 反序列化）
        TieredValueCache l2Cache = cache(alwaysMiss(), new RedisChannel(template));
        assertThat(l2Cache.load(meta, "1", valueType, null, true, () -> {
            throw new AssertionError("L2 预校验应命中");
        })).isNotNull();
        double l2Ops = bench("generic L2 hit", 20_000, () ->
                l2Cache.load(meta, String.valueOf(cursor.getAndIncrement() % KEYS + 1),
                        valueType, null, true, () -> {
                            throw new AssertionError("L2 不应回源");
                        }));

        // 3) miss → loader（L1/L2 恒 miss：single-flight + 序列化）
        TieredValueCache missCache = cache(alwaysMiss(), alwaysMiss());
        double missOps = bench("generic miss -> loader", 50_000, () ->
                missCache.load(meta, String.valueOf(cursor.getAndIncrement() % KEYS + 1),
                        valueType, null, true, () -> value(1)));

        // 4) @CacheLoad 切面命中（含 SpEL key 解析 + AOP）
        GenericCacheRegistry registry = new GenericCacheRegistry();
        TieredValueCache aspectCache = cache(new CaffeineChannel(10_000), new RedisChannel(template));
        LoaderImpl target = new LoaderImpl();
        Loader targetProxy = (Loader) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{Loader.class},
                (p, method, args) -> method.invoke(target, args));
        ProxyFactory proxyFactory = new ProxyFactory(targetProxy);
        proxyFactory.addAdvisor(new GenericCacheAspect(aspectCache, registry));
        Loader proxy = (Loader) proxyFactory.getProxy();
        proxy.load(9_999L);
        proxy.load(9_999L);
        assertThat(target.calls).as("@CacheLoad 切面应生效（第二次命中不执行方法体）").hasValue(1);
        for (long id = 1; id <= KEYS; id++) {
            proxy.load(id);
        }
        double aspectOps = bench("@CacheLoad aspect hit", 100_000, () ->
                proxy.load(cursor.getAndIncrement() % KEYS + 1));

        assertThat(l1Ops).isGreaterThan(10_000);
        assertThat(l2Ops).isGreaterThan(1_000);
        assertThat(missOps).isGreaterThan(1_000);
        assertThat(aspectOps).isGreaterThan(10_000);
    }
}
