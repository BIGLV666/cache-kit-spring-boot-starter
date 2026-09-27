package io.github.biglv666.cachekit;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.github.biglv666.cachekit.binlog.BinlogInvalidationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * binlog 连接生命周期：启动时连接 MySQL 并监听行事件；断线后自动重连（3s 间隔）；
 * 容器关闭时断开。MySQL 未开启 log_bin 时连接会失败，仅记录警告不影响应用启动。
 *
 * <p>断点续传：connector 内部随事件流推进 binlog 位点，重连时从最后位点继续
 * （{@code connect()} 仅在从未定位时查询 SHOW MASTER STATUS），断连窗口内的事件
 * 会被自动回放，无需外部记录位点；重复回放的失效是幂等 DEL，无害。</p>
 *
 * <p>位点失效兜底：记录位点对应的 binlog 文件若在长断连期间被服务端清理
 * （binlog 过期 / PURGE BINARY LOGS），重连会报错秒断并无限循环，监听将永久失效。
 * "连上即断且期间未收到任何事件"连续超过阈值时，重置为最新位点继续监听并告警
 * （断连窗口内的事件失效丢失由 L2 TTL 上界兜底）。</p>
 *
 * <p>LifecycleListener 只注册一次（重复注册会让回调/告警随重连次数线性累积）；
 * stop() 与连接中的 connect() 存在竞态，连接循环退出前补一次 disconnect 兜底。</p>
 */
public class BinlogLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(BinlogLifecycle.class);

    /** "连上即秒断且无事件"的连续轮数阈值：达到后重置位点回退到最新 */
    private static final int DEFAULT_MAX_RAPID_FAILURE_ROUNDS = 5;
    /** 一轮连接从建立到断开短于该时长视为"秒断"（正常连接存活远长于此） */
    private static final long RAPID_ROUND_NANOS = 2_000_000_000L;

    private final BinaryLogClient client;
    private final BinlogInvalidationListener listener;
    private final String description;
    private final int maxRapidFailureRounds;
    private final long reconnectDelayMillis;
    private final AtomicReference<CountDownLatch> disconnectSignal = new AtomicReference<>(new CountDownLatch(1));
    private volatile boolean running;
    private Thread connector;

    public BinlogLifecycle(BinaryLogClient client, BinlogInvalidationListener listener, String description) {
        this(client, listener, description, DEFAULT_MAX_RAPID_FAILURE_ROUNDS, 3_000L);
    }

    /** 测试用构造：可注入快速失败阈值与重连间隔，避免真实等待 */
    BinlogLifecycle(BinaryLogClient client, BinlogInvalidationListener listener, String description,
                    int maxRapidFailureRounds, long reconnectDelayMillis) {
        this.client = client;
        this.listener = listener;
        this.description = description;
        this.maxRapidFailureRounds = maxRapidFailureRounds;
        this.reconnectDelayMillis = reconnectDelayMillis;
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
                // 唤醒当前轮次等待的 latch（每轮重连更换新 latch，避免重复注册监听器）
                CountDownLatch latch = disconnectSignal.get();
                if (latch != null) {
                    latch.countDown();
                }
            }
        });
        int rapidFailureRounds = 0;
        while (running) {
            CountDownLatch disconnected = new CountDownLatch(1);
            disconnectSignal.set(disconnected);
            long eventMark = listener == null ? -1L : listener.receivedEventCount();
            long roundStart = System.nanoTime();
            boolean connected = false;
            try {
                client.connect();
                connected = true;
            } catch (Exception e) {
                log.warn("binlog 连接失败（MySQL 需开启 log_bin 且账号具备 REPLICATION SLAVE 权限），{}ms 后重试: {}",
                        reconnectDelayMillis, e.getMessage());
            }
            try {
                // connect() 建立后由内部线程监听；此处等待断线信号再重连
                while (running && !disconnected.await(5, TimeUnit.SECONDS)) {
                    // 周期性检查 running 标志
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (running) {
                rapidFailureRounds = evaluateRapidFailure(connected, roundStart, eventMark, rapidFailureRounds);
                log.warn("binlog 连接断开，{}ms 后重连", reconnectDelayMillis);
                try {
                    TimeUnit.MILLISECONDS.sleep(reconnectDelayMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        // 停机竞态兜底：stop() 的 disconnect 可能发生在 connect() 完成之前，
        // 随后连接建立成功会脱离生命周期管理——退出前再断一次
        if (client.isConnected()) {
            try {
                client.disconnect();
            } catch (Exception ignored) {
                // 关停阶段的连接异常无需处理
            }
        }
    }

    /**
     * 判断本轮是否为"连上即秒断且无事件"，累计到阈值时重置客户端位点回退到最新。
     * 位点文件被服务端清理（过期/PURGE）后，按旧位点重连会被 MySQL 秒断，
     * 无限循环会让 binlog 失效监听永久失效——回退到最新位点后监听恢复，
     * 断连窗口内的事件失效丢失由 L2 TTL 上界兜底。
     *
     * @return 更新后的连续快速失败轮数
     */
    private int evaluateRapidFailure(boolean connected, long roundStartNanos, long eventMark, int rapidFailureRounds) {
        boolean rapid = connected
                && System.nanoTime() - roundStartNanos < RAPID_ROUND_NANOS
                && (listener == null || !listener.receivedEventsSince(eventMark));
        if (!rapid) {
            return 0;
        }
        int rounds = rapidFailureRounds + 1;
        if (rounds >= maxRapidFailureRounds && client.getBinlogFilename() != null) {
            log.warn("binlog 连续 {} 轮连上即断且未收到任何事件，疑似记录位点已被服务端清理"
                            + "（binlog 过期/PURGE BINARY LOGS），重置为最新位点继续监听；"
                            + "断连窗口内的事件失效丢失由 L2 TTL 上界兜底",
                    rounds);
            // filename 置 null 触发 connect() 重新执行 SHOW MASTER STATUS 定位到当前位点
            client.setBinlogFilename(null);
            client.setBinlogPosition(4);
            return 0;
        }
        return rounds;
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
