package io.github.biglv666.cachekit.binlog;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import io.github.biglv666.cachekit.core.CacheKeyCustomizer;
import io.github.biglv666.cachekit.core.CacheMetricsListener;
import io.github.biglv666.cachekit.core.InMemoryChannel;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * binlog 键段还原单元测试：宿主 CacheKeyCustomizer 覆写 segmentFor 后，
 * 行事件按"自定义段:namespace:前缀:主键"精确失效；无法还原的行保留无段键失效并计数。
 * 合成 binlog 事件驱动，不依赖真实 MySQL；主键列序号经 mock JDBC 链解析。
 */
class BinlogDerivationTest {

    @TableName("t_order")
    static class TenantOrder {
        @TableId
        Long orderId;
        String tenantId;
    }

    /** 典型多租户自定义段：segment() 依赖应用线程上下文（binlog 线程拿不到），segmentFor 从行数据还原 */
    static class TenantCustomizer implements CacheKeyCustomizer {
        @Override
        public String segment() {
            return null;
        }

        @Override
        public String segmentFor(io.github.biglv666.cachekit.metadata.EntityMetadata meta,
                                 java.util.Map<String, Serializable> rowData) {
            Object tenant = rowData.get("tenant_id");
            return tenant == null ? null : String.valueOf(tenant);
        }
    }

    static class RecordingMetrics implements CacheMetricsListener {
        final AtomicInteger deriveSkipped = new AtomicInteger();

        @Override
        public void binlogDeriveSkipped(int rows) {
            deriveSkipped.addAndGet(rows);
        }
    }

    private InMemoryChannel l1;
    private RecordingMetrics metrics;
    private TestListener listener;

    /** 仅覆写列名解析（真实实现查 information_schema，测试直接给定），主键解析走 mock JDBC 链 */
    static class TestListener extends BinlogInvalidationListener {
        final List<String> columns;

        TestListener(TieredEntityCache cache, EntityMetadataRegistry registry,
                     DataSource dataSource, List<String> columns) {
            super(cache, registry, dataSource, "db", "ns", List.of(new TenantCustomizer()));
            this.columns = columns;
        }

        @Override
        protected List<String> orderedColumns(String table) {
            return columns;
        }
    }

    @BeforeEach
    void setUp() {
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.find(TenantOrder.class);
        l1 = new InMemoryChannel();
        metrics = new RecordingMetrics();
        TieredEntityCache cache = new TieredEntityCache(new CacheKitProperties(),
                l1, null, new NoopInvalidationPublisher(), null, "ns", List.of(new TenantCustomizer()));
        listener = new TestListener(cache, registry, pkOrdinalDataSource(), List.of("order_id", "tenant_id", "amount"));
        listener.setMetricsListener(metrics);
    }

    @Test
    void derivedRowShouldEvictExactKeyAndPlainKey() {
        l1.put("t1:ns:t_order:5", "j", Duration.ofSeconds(60));
        l1.put("ns:t_order:5", "j", Duration.ofSeconds(60));
        listener.onEvent(tableMap());
        listener.onEvent(writeRows(List.<Serializable[]>of(new Serializable[]{5L, "t1", 100})));

        // 还原成功：含段精确键失效
        assertThat(l1.get("t1:ns:t_order:5").hit()).isFalse();
        // 无段键同时失效（segment() 为 null 的写入会产生无段键）
        assertThat(l1.get("ns:t_order:5").hit()).isFalse();
        assertThat(metrics.deriveSkipped.get()).isZero();
    }

    @Test
    void nonDerivableRowShouldOnlyEvictPlainKeyAndCount() {
        l1.put("t1:ns:t_order:6", "j", Duration.ofSeconds(60));
        l1.put("ns:t_order:6", "j", Duration.ofSeconds(60));
        listener.onEvent(tableMap());
        // tenant_id 列值为 null：无法还原 → 仅无段键失效，精确键保留
        listener.onEvent(writeRows(List.<Serializable[]>of(new Serializable[]{6L, null, 100})));

        assertThat(l1.get("ns:t_order:6").hit()).isFalse();
        assertThat(l1.get("t1:ns:t_order:6").hit()).isTrue();
        assertThat(metrics.deriveSkipped.get()).isEqualTo(1);
    }

    @Test
    void columnMismatchShouldSkipDerivationWithoutQueryStorm() {
        // 行 3 列，表仅 1 列：列数不匹配 → 重查一次后仍不匹配 → 跳过还原（多行不放大重查）
        EntityMetadataRegistry narrowRegistry = new EntityMetadataRegistry();
        narrowRegistry.find(TenantOrder.class);
        TestListener narrowListener = new TestListener(
                new TieredEntityCache(new CacheKitProperties(), l1, null,
                        new NoopInvalidationPublisher(), null, "ns", List.of(new TenantCustomizer())),
                narrowRegistry,
                pkOrdinalDataSource(), List.of("order_id"));
        narrowListener.setMetricsListener(metrics);
        l1.put("t1:ns:t_order:7", "j", Duration.ofSeconds(60));

        narrowListener.onEvent(tableMap());
        narrowListener.onEvent(writeRows(List.<Serializable[]>of(
                new Serializable[]{7L, "t1", 100},
                new Serializable[]{8L, "t2", 100})));

        assertThat(l1.get("t1:ns:t_order:7").hit()).isTrue();
        assertThat(metrics.deriveSkipped.get()).isEqualTo(2);
    }

    @Test
    void updateEventShouldDeriveBothImages() {
        l1.put("t1:ns:t_order:5", "j", Duration.ofSeconds(60));
        l1.put("t2:ns:t_order:9", "j", Duration.ofSeconds(60));
        listener.onEvent(tableMap());

        UpdateRowsEventData data = new UpdateRowsEventData();
        data.setTableId(108L);
        // 主键被改的 UPDATE：before（t1,5）与 after（t2,9）两幅镜像的精确键都要失效
        data.setRows(List.of(new AbstractMap.SimpleEntry<Serializable[], Serializable[]>(
                new Serializable[]{5L, "t1", 100},
                new Serializable[]{9L, "t2", 100})));
        listener.onEvent(event(EventType.UPDATE_ROWS, data));

        assertThat(l1.get("t1:ns:t_order:5").hit()).isFalse();
        assertThat(l1.get("t2:ns:t_order:9").hit()).isFalse();
        assertThat(metrics.deriveSkipped.get()).isZero();
    }

    private static Event tableMap() {
        TableMapEventData tm = new TableMapEventData();
        tm.setTableId(108L);
        tm.setDatabase("db");
        tm.setTable("t_order");
        return event(EventType.TABLE_MAP, tm);
    }

    private static Event writeRows(List<Serializable[]> rows) {
        WriteRowsEventData data = new WriteRowsEventData();
        data.setTableId(108L);
        data.setRows(rows);
        return event(EventType.WRITE_ROWS, data);
    }

    private static Event event(EventType type, EventData data) {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(type);
        header.setTimestamp(System.currentTimeMillis() / 1000);
        return new Event(header, data);
    }

    /** 主键列查询的 mock JDBC 链：t_order 主键 order_id 在第 1 列（真实实现查 information_schema） */
    private static DataSource pkOrdinalDataSource() throws RuntimeException {
        try {
            DataSource ds = mock(DataSource.class);
            Connection conn = mock(Connection.class);
            PreparedStatement ps = mock(PreparedStatement.class);
            ResultSet rs = mock(ResultSet.class);
            when(ds.getConnection()).thenReturn(conn);
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true, false);
            when(rs.getString(1)).thenReturn("order_id");
            when(rs.getInt(2)).thenReturn(1);
            return ds;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
