package io.github.biglv666.cachekit.chaos.support;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * 混沌测试容器工具。
 *
 * <p><b>固定同号端口绑定</b>（不用动态映射）：Docker Desktop 的动态端口转发偶发
 * "TCP 通但数据挂起"，停机/重启类测试对端口转发可靠性敏感，改用固定端口
 * （宿主 16379 = 容器 16379，redis-server 以 --port 16379 启动实现同号）实测稳定。
 * 宿主 6379 被预置的 cache-kit-redis 容器占用，混沌测试一律避开。</p>
 *
 * <p>停机方式用 docker stop/start（保留容器），而非 Testcontainers 的 stop()/start()
 * （stop 后实例被销毁重建，端到端语义失真）。</p>
 */
public final class ChaosContainers {

    /** 混沌测试专用固定端口段起点：每个混沌测试类用独立端口（同号绑定），避免类间 TIME_WAIT 竞争 */
    public static final int REDIS_PORT = 16379;

    private ChaosContainers() {
    }

    /** 宿主端口是否空闲（多套混沌测试串行使用同一端口，并行运行时会跳过） */
    public static boolean hostPortFree(int port) {
        try (ServerSocket ss = new ServerSocket(port)) {
            ss.setReuseAddress(true);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 固定同号端口绑定的 redis:7 容器（未 start）：宿主 16379 → 容器 16379。
     * 动态映射的偶发挂起会让"停机→恢复"断言假失败，固定绑定是混沌测试的稳定性前提。
     */
    public static GenericContainer<?> redisFixedPort() {
        return redisFixedPort(REDIS_PORT);
    }

    /** 指定端口的固定同号绑定 redis:7 容器（宿主 port = 容器 port，redis-server 以 --port 启动） */
    public static GenericContainer<?> redisFixedPort(int port) {
        return new GenericContainer<>(DockerImageName.parse("redis:7"))
                .withCommand("redis-server", "--port", String.valueOf(port))
                .withCreateContainerCmdModifier((CreateContainerCmd cmd) -> cmd
                        .withExposedPorts(new ExposedPort(port))
                        .withHostConfig(new HostConfig().withPortBindings(
                                new Ports(PortBinding.parse(port + ":" + port)))));
    }

    /** docker stop（保留容器与数据，配合 startContainer 实现"真停机"） */
    public static void stopContainer(GenericContainer<?> container) {
        DockerClientFactory.instance().client()
                .stopContainerCmd(container.getContainerId()).withTimeout(0).exec();
    }

    /** docker start（同一容器恢复运行） */
    public static void startContainer(GenericContainer<?> container) {
        DockerClientFactory.instance().client()
                .startContainerCmd(container.getContainerId()).exec();
    }

    /**
     * 轮询等待 Redis 真实可服务（PING-PONG）：docker userland proxy 在容器进程就绪前
     * 就会接受 TCP（"TCP 通但未服务"），socket 探活会过早放行导致恢复期读竞态。
     */
    public static boolean awaitRedisReady(org.springframework.data.redis.core.StringRedisTemplate template,
                                          long deadlineMillis) {
        long deadline = System.currentTimeMillis() + deadlineMillis;
        while (System.currentTimeMillis() < deadline) {
            try {
                String pong = template.execute(
                        (org.springframework.data.redis.core.RedisCallback<String>) conn -> conn.ping());
                if ("PONG".equals(pong)) {
                    return true;
                }
            } catch (Exception e) {
                // 未就绪：继续轮询
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /** 轮询等待 Redis 可响应（socket 级，供无模板场景） */
    public static boolean awaitRedis(String host, int port, long deadlineMillis) {
        long deadline = System.currentTimeMillis() + deadlineMillis;
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 300);
                return true;
            } catch (Exception e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }
}
