package io.github.biglv666.cachekit;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.github.biglv666.cachekit.binlog.BinlogInvalidationListener;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.timeout;

/**
 * binlog 连接生命周期：位点被服务端清理后的快速失败回退。
 * 真实断点续传由 connector 原生承担（connect() 复用内部位点），此处只测兜底路径。
 */
class BinlogLifecycleTest {

    @Test
    void purgedBinlogPositionShouldResetToLatestAfterRapidFailureRounds() throws Exception {
        BinaryLogClient client = Mockito.mock(BinaryLogClient.class);
        Mockito.when(client.getBinlogFilename()).thenReturn("binlog.000003");
        AtomicReference<BinaryLogClient.LifecycleListener> lifecycle = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            lifecycle.set(invocation.getArgument(0));
            return null;
        }).when(client).registerLifecycleListener(any());
        BinlogInvalidationListener listener =
                new BinlogInvalidationListener(null, new EntityMetadataRegistry(), null, "db");

        // 阈值 3 轮、重连间隔 10ms：避免真实等待
        BinlogLifecycle lifecycleBean = new BinlogLifecycle(client, listener, "test", 3, 10);
        lifecycleBean.start();
        // LifecycleListener 在连接线程中异步注册：先等注册完成再断言
        Mockito.verify(client, timeout(2_000)).registerLifecycleListener(any());
        assertThat(lifecycle.get()).as("LifecycleListener 应已注册").isNotNull();

        // 模拟位点被清理的表现：connect() 成功后服务端报错秒断，期间无任何事件。
        // 同步点：先等第一轮 connect 建立（latch 就绪），每轮断开后再等下一轮 connect
        Mockito.verify(client, timeout(2_000)).connect();
        for (int i = 1; i <= 3; i++) {
            lifecycle.get().onDisconnect(client);
            Mockito.verify(client, timeout(2_000).times(i + 1)).connect();
        }

        // 达到阈值：重置为最新位点（filename 置 null 触发重新 SHOW MASTER STATUS）
        Mockito.verify(client, timeout(2_000)).setBinlogFilename(Mockito.isNull());
        Mockito.verify(client, timeout(2_000).atLeastOnce()).setBinlogPosition(4L);

        lifecycleBean.stop();
    }

    @Test
    void positionResetShouldRecordMetric() throws Exception {
        BinaryLogClient client = Mockito.mock(BinaryLogClient.class);
        Mockito.when(client.getBinlogFilename()).thenReturn("binlog.000003");
        AtomicReference<BinaryLogClient.LifecycleListener> lifecycle = new AtomicReference<>();
        Mockito.doAnswer(invocation -> {
            lifecycle.set(invocation.getArgument(0));
            return null;
        }).when(client).registerLifecycleListener(any());
        BinlogInvalidationListener listener =
                new BinlogInvalidationListener(null, new EntityMetadataRegistry(), null, "db");

        java.util.concurrent.atomic.AtomicInteger resets = new java.util.concurrent.atomic.AtomicInteger();
        BinlogLifecycle lifecycleBean = new BinlogLifecycle(client, listener, "test", 2, 10);
        lifecycleBean.setMetricsListener(new io.github.biglv666.cachekit.core.CacheMetricsListener() {
            @Override
            public void binlogPositionReset() {
                resets.incrementAndGet();
            }
        });
        lifecycleBean.start();
        Mockito.verify(client, timeout(2_000)).connect();
        for (int i = 1; i <= 2; i++) {
            lifecycle.get().onDisconnect(client);
            Mockito.verify(client, timeout(2_000).times(i + 1)).connect();
        }

        Mockito.verify(client, timeout(2_000)).setBinlogFilename(Mockito.isNull());
        assertThat(resets.get()).as("位点重置应计入 binlog.position.resets").isEqualTo(1);

        lifecycleBean.stop();
    }

    @Test
    void startShouldRegisterSingleLifecycleListenerAndListenerKeepsCounting() {
        BinaryLogClient client = Mockito.mock(BinaryLogClient.class);
        ArgumentCaptor<BinaryLogClient.LifecycleListener> captor =
                ArgumentCaptor.forClass(BinaryLogClient.LifecycleListener.class);
        BinlogInvalidationListener listener =
                new BinlogInvalidationListener(null, new EntityMetadataRegistry(), null, "db");

        BinlogLifecycle lifecycleBean = new BinlogLifecycle(client, listener, "test", 5, 10);
        lifecycleBean.start();
        Mockito.verify(client, timeout(2_000)).registerLifecycleListener(captor.capture());
        lifecycleBean.stop();

        // 事件计数供"连上即秒断"判定：收到事件后 receivedEventsSince 应为 true
        long mark = listener.receivedEventCount();
        assertThat(listener.receivedEventsSince(mark)).isFalse();
    }
}
