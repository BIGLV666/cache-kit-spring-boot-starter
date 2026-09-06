package io.github.biglv666.cachekit.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * binlog 行事件失效监听器：订阅 MySQL binlog，任何来源（DBA、其他服务、脚本）对
 * 已缓存实体对应表的写入都会按行提取主键并触发 {@link TieredEntityCache#evict}，
 * 弥补"绕过应用的写无法广播失效"这个盲区。
 *
 * <p>表 → 主键列序号通过 information_schema 惰性解析并缓存；单条事件解析失败只打警告，
 * 不中断监听（残余脏数据由 TTL 上界兜底）。</p>
 */
public class BinlogInvalidationListener implements BinaryLogClient.EventListener {

    private static final Logger log = LoggerFactory.getLogger(BinlogInvalidationListener.class);

    private final TieredEntityCache tieredCache;
    private final EntityMetadataRegistry registry;
    private final DataSource dataSource;
    private final String database;

    /** TABLE_MAP 事件维护 tableId → 库/表名 映射 */
    private final Map<Long, TableMapEventData> tableMap = new ConcurrentHashMap<>();
    /** 表 → 主键列序号（1 起算），惰性解析 */
    private final Map<String, Integer> pkOrdinals = new ConcurrentHashMap<>();

    public BinlogInvalidationListener(TieredEntityCache tieredCache,
                                      EntityMetadataRegistry registry,
                                      DataSource dataSource,
                                      String database) {
        this.tieredCache = tieredCache;
        this.registry = registry;
        this.dataSource = dataSource;
        this.database = database;
    }

    @Override
    public void onEvent(Event event) {
        try {
            EventType type = event.getHeader().getEventType();
            if (type == EventType.TABLE_MAP) {
                EventData data = event.getData();
                if (data instanceof TableMapEventData tm) {
                    tableMap.put(tm.getTableId(), tm);
                }
                return;
            }
            switch (type) {
                case WRITE_ROWS, EXT_WRITE_ROWS ->
                        evictRows(((WriteRowsEventData) event.getData()).getTableId(),
                                ((WriteRowsEventData) event.getData()).getRows());
                case DELETE_ROWS, EXT_DELETE_ROWS ->
                        evictRows(((DeleteRowsEventData) event.getData()).getTableId(),
                                ((DeleteRowsEventData) event.getData()).getRows());
                case UPDATE_ROWS, EXT_UPDATE_ROWS -> {
                    UpdateRowsEventData data = (UpdateRowsEventData) event.getData();
                    // UPDATE 取 after 值的主键（主键本身被改时旧键已无效，取新键失效是安全选择）
                    List<Serializable[]> afterRows = new ArrayList<>(data.getRows().size());
                    for (Map.Entry<Serializable[], Serializable[]> row : data.getRows()) {
                        afterRows.add(row.getValue());
                    }
                    evictRows(data.getTableId(), afterRows);
                }
                default -> {
                    // 忽略其他事件
                }
            }
        } catch (Throwable t) {
            // 单条事件解析失败（如未知列类型）不中断监听，TTL 上界兜底
            log.warn("binlog 事件处理失败，type={}", event.getHeader().getEventType(), t);
        }
    }

    /**
     * 整事件合并失效：一个行事件可能包含大量受影响行，逐键立即删除，
     * 延迟双删整个批次只调度一次（避免大事务行事件风暴放大成百万级延迟任务）。
     */
    private void evictRows(long tableId, List<Serializable[]> rows) {
        if (rows.isEmpty()) {
            return;
        }
        TableMapEventData tm = tableMap.get(tableId);
        if (tm == null || !database.equals(tm.getDatabase())) {
            return;
        }
        EntityMetadata meta = registry.findByPrefix(tm.getTable());
        if (meta == null) {
            return;
        }
        Integer ordinal = pkOrdinal(tm.getTable());
        if (ordinal == null || ordinal < 1) {
            return;
        }
        List<Object> ids = new ArrayList<>(rows.size());
        for (Serializable[] row : rows) {
            if (ordinal > row.length) {
                continue;
            }
            Object id = toCacheId(row[ordinal - 1]);
            if (id != null) {
                ids.add(id);
            }
        }
        if (!ids.isEmpty()) {
            tieredCache.evictBatch(meta, ids, true);
        }
    }

    /** 主键列序号（1 起算）惰性查询 information_schema，按表缓存 */
    private Integer pkOrdinal(String table) {
        return pkOrdinals.computeIfAbsent(table, t -> {
            if (dataSource == null) {
                log.warn("未配置 DataSource，无法解析表 {} 的主键列，该表 binlog 失效被跳过", t);
                return -1;
            }
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "SELECT ORDINAL_POSITION FROM information_schema.COLUMNS "
                                 + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI' LIMIT 1")) {
                ps.setString(1, database);
                ps.setString(2, t);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getInt(1) : -1;
                }
            } catch (Exception e) {
                throw new CacheKitException("查询表 " + t + " 主键列失败", e);
            }
        });
    }

    /** binlog 行值 → 缓存键的 id 段：最终都转成字符串，与读路径的键推导保持一致 */
    private Object toCacheId(Serializable value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value;
    }
}
