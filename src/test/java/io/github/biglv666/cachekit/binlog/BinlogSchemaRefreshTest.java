package io.github.biglv666.cachekit.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import io.github.biglv666.cachekit.core.CacheMetricsListener;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.model.UserEntity;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * binlog 表结构在线刷新：
 * 1. TABLE_MAP 列数变化（ALTER 加/删列）立即清空该表的 schema 缓存并重查；
 * 2. 列数不变时按 {@code schema-refresh-interval} 定期重载（覆盖 drop+add 等列序变化）；
 * 3. 未注册缓存的表不产生额外 information_schema 查询。
 */
class BinlogSchemaRefreshTest {

    /** 记录 orderedColumns 调用次数；列数可在测试中变更 */
    static class RecordingListener extends BinlogInvalidationListener {
        final AtomicInteger columnQueries = new AtomicInteger();
        volatile List<String> columns = List.of("user_id", "username");

        RecordingListener(EntityMetadataRegistry registry) {
            super(null, registry, null, "cachekit_test");
        }

        @Override
        protected List<String> orderedColumns(String table) {
            columnQueries.incrementAndGet();
            return columns;
        }
    }

    static class RecordingMetrics implements CacheMetricsListener {
        final AtomicInteger countReloads = new AtomicInteger();
        final AtomicInteger intervalReloads = new AtomicInteger();

        @Override
        public void binlogSchemaReload(String reason) {
            if ("count".equals(reason)) {
                countReloads.incrementAndGet();
            } else if ("interval".equals(reason)) {
                intervalReloads.incrementAndGet();
            }
        }
    }

    private static EntityMetadataRegistry registry() {
        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.require(UserEntity.class);
        return registry;
    }

    private static Event tableMap(String table, int columnCount) {
        return tableMap("cachekit_test", table, columnCount);
    }

    private static Event tableMap(String database, String table, int columnCount) {
        TableMapEventData data = new TableMapEventData();
        data.setTableId(1L);
        data.setDatabase(database);
        data.setTable(table);
        data.setColumnTypes(new byte[columnCount]);
        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.TABLE_MAP);
        return new Event(header, data);
    }

    @Test
    void columnCountChangeShouldInvalidateSchemaCacheAndReload() {
        RecordingListener listener = new RecordingListener(registry());
        RecordingMetrics metrics = new RecordingMetrics();
        listener.setMetricsListener(metrics);

        listener.onEvent(tableMap("user_entity", 2));
        assertThat(listener.columnQueries.get()).as("首次 TABLE_MAP 用事件列数建基线，不查库").isZero();

        listener.onEvent(tableMap("user_entity", 2));
        assertThat(listener.columnQueries.get()).as("列数未变不重查").isZero();

        // ALTER 加列：列数 2 → 3
        listener.columns = List.of("extra", "user_id", "username");
        listener.onEvent(tableMap("user_entity", 3));
        assertThat(listener.columnQueries.get()).as("列数变化必须重查").isEqualTo(1);
        assertThat(metrics.countReloads.get()).isEqualTo(1);
        assertThat(metrics.intervalReloads.get()).isZero();
    }

    @Test
    void intervalRefreshShouldReloadEvenWhenColumnCountUnchanged() throws Exception {
        RecordingListener listener = new RecordingListener(registry());
        RecordingMetrics metrics = new RecordingMetrics();
        listener.setMetricsListener(metrics);
        listener.setSchemaRefreshInterval(Duration.ofMillis(1));

        listener.onEvent(tableMap("user_entity", 2));
        assertThat(listener.columnQueries.get()).isZero();

        Thread.sleep(10);
        // 列数不变，但列序可能变化（drop + add）：定期刷新兜底
        listener.onEvent(tableMap("user_entity", 2));
        assertThat(listener.columnQueries.get()).isEqualTo(1);
        assertThat(metrics.intervalReloads.get()).isEqualTo(1);
        assertThat(metrics.countReloads.get()).isZero();
    }

    @Test
    void nonCachedTableShouldNotQueryInformationSchema() {
        RecordingListener listener = new RecordingListener(registry());
        listener.onEvent(tableMap("not_cached_table", 5));
        assertThat(listener.columnQueries.get()).isZero();
    }

    @Test
    void foreignDatabaseTableMapShouldNotTouchSchemaCache() {
        RecordingListener listener = new RecordingListener(registry());
        RecordingMetrics metrics = new RecordingMetrics();
        listener.setMetricsListener(metrics);

        // 同一 MySQL 上另一个库的同名表：不能重查本应用库的 schema，也不能触发重载
        listener.onEvent(tableMap("other_db", "user_entity", 3));
        assertThat(listener.columnQueries.get()).as("非本应用库的事件不查询 information_schema").isZero();
        assertThat(metrics.countReloads.get()).isZero();
        assertThat(metrics.intervalReloads.get()).isZero();
    }
}
