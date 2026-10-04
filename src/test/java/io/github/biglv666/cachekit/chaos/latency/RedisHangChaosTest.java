package io.github.biglv666.cachekit.chaos.latency;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.channel.RedisChannel;
import io.github.biglv666.cachekit.chaos.support.ChaosContainers;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.CacheMetricsListener;
import io.github.biglv666.cachekit.core.L2CircuitBreaker;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Redis "TCP 通但数据挂起"混沌（toxiproxy timeout toxic）：这是比停机更隐蔽的故障——
 * 连接能建立、命令永不返回，验证三件事：
 * ① 降级以"单次调用失败"为边界——每次读阻塞到命令超时（800ms）后按未命中走 DB，业务零异常；
 * ② 熔断器真实价值——连续失败达阈值后短路，业务读从 800ms/次 变为瞬时降级；
 * ③ 故障解除后半开探测恢复，全程无人工干预。
 *
 * <p>网络拓扑（容器间上游必须走共享 Network + 别名，getContainerIpAddress 只代表宿主地址）：
 * 宿主 → toxiproxy（固定映射端口）→ [共享网络别名 redis] → redis:7。</p>
 */
class RedisHangChaosTest {

    private static final int COMMAND_TIMEOUT_MS = 800;

    private static Network network;
    private static GenericContainer<?> redis;
    private static ToxiproxyContainer toxiproxy;
    private static ToxiproxyContainer.ContainerProxy proxy;
    private static LettuceConnectionFactory factory;

    static class RecordingMetrics implements CacheMetricsListener {
        final List<Integer> states = new ArrayList<>();
        int opens;

        @Override
        public void l2CircuitOpened() {
            opens++;
        }

        @Override
        public synchronized void l2CircuitState(int state) {
            states.add(state);
        }
    }

    @BeforeAll
    static void startTopology() {
        Assumptions.assumeTrue(dockerAvailable(), "无 Docker，跳过 Redis 挂起混沌测试");
        network = Network.newNetwork();
        redis = new GenericContainer<>(DockerImageName.parse("redis:7"))
                .withNetwork(network)
                .withNetworkAliases("redis");
        toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0"))
                .withNetwork(network);
        toxiproxy.start();
        redis.start();
        // 上游用别名解析（容器间通信），宿主侧只连 toxiproxy 的映射端口
        proxy = toxiproxy.getProxy("redis", 6379);
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(COMMAND_TIMEOUT_MS)).build();
        factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(proxy.getContainerIpAddress(), proxy.getProxyPort()),
                clientConfig);
        factory.afterPropertiesSet();
    }

    @AfterAll
    static void shutdown() {
        if (factory != null) {
            factory.destroy();
        }
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
        if (redis != null) {
            redis.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    private static boolean dockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            return false;
        }
    }

    @Test
    void hangShouldDegradePerCallUntilBreakerOpensThenRecover() throws Exception {
        RecordingMetrics metrics = new RecordingMetrics();
        CacheKitProperties props = new CacheKitProperties();
        props.getL2().setJitter(Duration.ZERO);
        props.getL2().setDoubleDeleteDelay(Duration.ofMillis(100));
        RedisChannel l2 = new RedisChannel(template());
        l2.setMetricsListener(metrics);
        // 阈值 3 / 开 1s：故障注入后 ~3 次调用（约 2.4s）即熔断，故障解除后 ~1s 探测恢复
        L2CircuitBreaker breaker = new L2CircuitBreaker(3, Duration.ofSeconds(1));
        breaker.setMetricsListener(metrics);
        l2.setCircuitBreaker(breaker);
        CaffeineChannel l1 = new CaffeineChannel(1000);
        TieredEntityCache cache = new TieredEntityCache(props, l1, l2,
                new NoopInvalidationPublisher(), null, "", List.of());
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.find(UserEntity.class);
        EntityMetadata meta = registry.require(UserEntity.class);
        String fullKey = meta.prefix() + ":1";

        // 0) 正常基线：经代理读写成功
        assertThat(((UserEntity) cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "v0"))).getUserName())
                .isEqualTo("v0");
        l1.evict(fullKey);
        assertThat(((UserEntity) cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "v0"))).getUserName())
                .isEqualTo("v0");

        // 1) 故障注入：下游方向数据永久挂起（TCP 通、响应永不到达）
        eu.rekawek.toxiproxy.model.toxic.Timeout hang =
                proxy.toxics().timeout("hang", ToxicDirection.DOWNSTREAM, 0);
        try {
            // 2) 每次降级读阻塞一个命令超时（800ms）后走 DB，业务零异常
            l1.evict(fullKey); // 先清 L1：否则读直接命中本地缓存，根本不触达挂起的 Redis
            long t0 = System.nanoTime();
            assertThat(((UserEntity) cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "v1"))).getUserName())
                    .as("挂起期间读必须降级为 DB 值").isEqualTo("v1");
            long firstMs = (System.nanoTime() - t0) / 1_000_000;
            assertThat(firstMs)
                    .as("首次降级应阻塞到命令超时（%dms），实测 %dms", COMMAND_TIMEOUT_MS, firstMs)
                    .isBetween(COMMAND_TIMEOUT_MS - 200L, COMMAND_TIMEOUT_MS * 5L);
            System.out.printf("===== 挂起混沌：熔断前单次降级 %dms（命令超时 %dms） =====%n", firstMs, COMMAND_TIMEOUT_MS);

            // 再来两次触发阈值（3 次连续失败）
            l1.evict(fullKey);
            cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "v1"));
            l1.evict(fullKey);
            long t2 = System.nanoTime();
            cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "v1"));
            long thirdMs = (System.nanoTime() - t2) / 1_000_000;

            // 3) 熔断器打开后的价值：业务读瞬时降级，不再等待 Redis
            long t3 = System.nanoTime();
            assertThat(((UserEntity) cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "v1"))).getUserName())
                    .as("熔断期间读仍必须是 DB 值").isEqualTo("v1");
            long openMs = (System.nanoTime() - t3) / 1_000_000;
            System.out.printf("===== 挂起混沌：第 3 次降级 %dms（触发熔断），熔断后 %dms =====%n", thirdMs, openMs);
            assertThat(openMs).as("熔断短路必须是瞬时降级（< 命令超时的 1/4）").isLessThan(COMMAND_TIMEOUT_MS / 4);
            assertThat(breaker.state()).isEqualTo(L2CircuitBreaker.STATE_OPEN);
            synchronized (metrics.states) {
                assertThat(metrics.states).as("OPEN 状态必须推送指标").contains(L2CircuitBreaker.STATE_OPEN);
            }
        } finally {
            // 4) 故障解除：移除挂起毒药 + 复位 TCP 连接。
            // 悬挂的连接不会自愈（Lettuce 命令超时不触发重连，毒药吃掉的响应也不再回来），
            // 真实运维对应"服务端恢复后重置连接"——这里用 connectionCut 模拟
            hang.remove();
            proxy.setConnectionCut(true);
            Thread.sleep(300);
            proxy.setConnectionCut(false);
        }

        // 5) 半开探测恢复：openDuration 到期后探测穿透代理成功 → 回 CLOSED。
        // 注意：熔断状态机由流量驱动（无 tryAcquire 就不做 OPEN→HALF_OPEN 迁移），
        // 恢复判定必须持续发读流量；且"读到新值"也可能是熔断短路走 DB 的结果，
        // 所以以"状态回到 CLOSED 且读到新值"为恢复依据
        // 恢复判据：状态回 CLOSED（探测成功）且读延迟正常。注意不能用"读到新值"判据——
        // L2 里合法的基线值未过期会一直命中，而熔断期间 L2 写同样被短路（设计行为），
        // loader 的新值只有 L2 真未命中才会回填
        long deadline = System.currentTimeMillis() + 15_000;
        boolean recovered = false;
        while (System.currentTimeMillis() < deadline) {
            l1.evict(fullKey);
            long t = System.nanoTime();
            cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "v2"));
            long ms = (System.nanoTime() - t) / 1_000_000;
            if (breaker.state() == L2CircuitBreaker.STATE_CLOSED && ms < COMMAND_TIMEOUT_MS / 2) {
                recovered = true;
                break;
            }
            Thread.sleep(300);
        }
        assertThat(recovered).as("故障解除后必须自动恢复（探测成功回 CLOSED，读延迟正常）").isTrue();

        // 终验完整链路：清掉 L2 旧值后，读必须取到 loader 新值（缓存读写完全恢复正常）
        template().delete(fullKey);
        l1.evict(fullKey);
        assertThat(((UserEntity) cache.load(meta, 1L, null, true, () -> new UserEntity(1L, "v2"))).getUserName())
                .isEqualTo("v2");
    }

    private static org.springframework.data.redis.core.StringRedisTemplate template() {
        org.springframework.data.redis.core.StringRedisTemplate template =
                new org.springframework.data.redis.core.StringRedisTemplate(factory);
        template.afterPropertiesSet();
        return template;
    }
}
