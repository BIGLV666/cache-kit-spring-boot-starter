package io.github.biglv666.cachekit.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import io.github.biglv666.cachekit.annotation.CacheEntity;
import io.github.biglv666.cachekit.annotation.CacheId;
import io.github.biglv666.cachekit.core.InMemoryChannel;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * binlog 复合主键测试：多 PRI 列经"字段名↔列名"（驼峰↔蛇形）映射按元数据声明序
 * join 联合键段——列序与字段序不一致也能对齐；映射失败（列缺失/结构漂移）整行跳过。
 * 合成 binlog 事件驱动，不依赖真实 MySQL（pkColumns/orderedColumns 直接覆写）。
 */
class BinlogCompositeKeyTest {

    @CacheEntity(prefix = "t_order_item")
    static class OrderItem {
        @CacheId
        public Long orderId;
        @CacheId
        public String skuId;
        public int amount;

        public OrderItem() {
        }
    }

    /** 覆写主键列与列名解析（真实实现查 information_schema） */
    static class TestListener extends BinlogInvalidationListener {
        final List<BinlogInvalidationListener.PkColumn> pkCols;
        final List<String> columns;

        TestListener(TieredEntityCache cache, EntityMetadataRegistry registry,
                     List<BinlogInvalidationListener.PkColumn> pkCols, List<String> columns) {
            super(cache, registry, (DataSource) null, "db", "", List.of());
            this.pkCols = pkCols;
            this.columns = columns;
        }

        @Override
        protected List<BinlogInvalidationListener.PkColumn> pkColumns(String table) {
            return pkCols;
        }

        @Override
        protected List<String> orderedColumns(String table) {
            return columns;
        }
    }

    private InMemoryChannel l1;
    private TestListener listener;

    @BeforeEach
    void setUp() {
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.find(OrderItem.class);
        l1 = new InMemoryChannel();
        TieredEntityCache cache = new TieredEntityCache(new CacheKitProperties(),
                l1, null, new NoopInvalidationPublisher(), null, "", List.of());
        // 表列序与实体字段声明序不同：sku_id 在前（联合主键声明序不代表行镜像列序）
        listener = new TestListener(cache, registry,
                List.of(new BinlogInvalidationListener.PkColumn("order_id", 1), new BinlogInvalidationListener.PkColumn("sku_id", 2)),
                List.of("sku_id", "order_id", "amount"));
    }

    @Test
    void compositeRowShouldEvictJoinedKeyRegardlessOfColumnOrder() {
        l1.put("t_order_item:5:s1", "j", Duration.ofSeconds(60));

        listener.onEvent(tableMap());
        // 行镜像按表列序：sku_id, order_id, amount（与字段声明序 order_id, sku_id 相反）
        listener.onEvent(writeRows(List.<Serializable[]>of(new Serializable[]{"s1", 5L, 100})));

        assertThat(l1.get("t_order_item:5:s1").hit()).isFalse();
    }

    @Test
    void updateShouldEvictBothImagesOfCompositeKeys() {
        l1.put("t_order_item:5:s1", "j", Duration.ofSeconds(60));
        l1.put("t_order_item:6:s2", "j", Duration.ofSeconds(60));

        listener.onEvent(tableMap());
        com.github.shyiko.mysql.binlog.event.UpdateRowsEventData data =
                new com.github.shyiko.mysql.binlog.event.UpdateRowsEventData();
        data.setTableId(208L);
        // 主键被改：before（5,s1）与 after（6,s2）两幅镜像都要失效
        data.setRows(List.of(java.util.Map.entry(
                new Serializable[]{"s1", 5L, 100},
                new Serializable[]{"s2", 6L, 100})));
        listener.onEvent(event(EventType.UPDATE_ROWS, data));

        assertThat(l1.get("t_order_item:5:s1").hit()).isFalse();
        assertThat(l1.get("t_order_item:6:s2").hit()).isFalse();
    }

    @Test
    void unmappedPkColumnShouldSkipRowWithoutFalseEviction() {
        l1.put("t_order_item:5:s1", "j", Duration.ofSeconds(60));

        // 列名解析不可用（null）：复合键无法对齐 → 整事件跳过，键保留（TTL/双删兜底）
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.find(OrderItem.class);
        TestListener noColumns = new TestListener(
                new TieredEntityCache(new CacheKitProperties(), l1, null,
                        new NoopInvalidationPublisher(), null, "", List.of()),
                registry,
                List.of(new BinlogInvalidationListener.PkColumn("order_id", 1), new BinlogInvalidationListener.PkColumn("sku_id", 2)),
                null);
        noColumns.onEvent(tableMap());
        noColumns.onEvent(writeRows(List.<Serializable[]>of(new Serializable[]{"s1", 5L, 100})));

        assertThat(l1.get("t_order_item:5:s1").hit()).isTrue();
    }

    private static Event tableMap() {
        TableMapEventData tm = new TableMapEventData();
        tm.setTableId(208L);
        tm.setDatabase("db");
        tm.setTable("t_order_item");
        return event(EventType.TABLE_MAP, tm);
    }

    private static Event writeRows(List<Serializable[]> rows) {
        WriteRowsEventData data = new WriteRowsEventData();
        data.setTableId(208L);
        data.setRows(rows);
        return event(EventType.WRITE_ROWS, data);
    }

    private static Event event(EventType type, EventData data) {
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(type);
        header.setTimestamp(System.currentTimeMillis() / 1000);
        return new Event(header, data);
    }
}
