package io.github.biglv666.cachekit;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * binlog 连接生命周期：启动时连接 MySQL 并监听行事件；断线后自动重连（3s 间隔）；
 * 容器关闭时断开。MySQL 未开启 log_bin 时连接会失败，仅记录警告不影响应用启动。
 */
public class BinlogLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(BinlogLifecycle.class);

    private final BinaryLogClient client;
    private final String description;
    private volatile boolean running;
    private Thread connector;

    public BinlogLifecycle(BinaryLogClient client, String description) {
        this.client = client;
        this.description = description;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        connector = new Thread(this::connectLoop, "cache-kit-binlog-connector");
        connector.setDaemon(true);
        connector.start();
    }

    private void connectLoop() {
        while (running) {
            CountDownLatch disconnected = new CountDownLatch(1);
            client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
                @Override
                public void onConnect(BinaryLogClient c) {
                    log.info("binlog 失效监听已连接: {} (serverId={})", description, c.getServerId());
                }

                @Override
                public void onCommunicationFailure(BinaryLogClient c, Exception e) {
                    log.warn("binlog 通信失败: {}", e.getMessage());
                }

                @Override
                public void onEventDeserializationFailure(BinaryLogClient c, Exception e) {
                    log.warn("binlog 事件反序列化失败: {}", e.getMessage());
                }

                @Override
                public void onDisconnect(BinaryLogClient c) {
                    disconnected.countDown();
                }
            });
            try {
                client.connect();
            } catch (Exception e) {
                log.warn("binlog 连接失败（MySQL 需开启 log_bin 且账号具备 REPLICATION SLAVE 权限），5s 后重试: {}",
                        e.getMessage());
            }
            try {
                // connect() 建立后由内部线程监听；此处等待断线信号再重连
                while (running && !disconnected.await(5, TimeUnit.SECONDS)) {
                    // 周期性检查 running 标志
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (running) {
                log.warn("binlog 连接断开，3s 后重连");
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    @Override
    public synchronized void stop() {
        running = false;
        if (connector != null) {
            connector.interrupt();
        }
        try {
            client.disconnect();
        } catch (Exception ignored) {
            // 关停阶段的连接异常无需处理
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
