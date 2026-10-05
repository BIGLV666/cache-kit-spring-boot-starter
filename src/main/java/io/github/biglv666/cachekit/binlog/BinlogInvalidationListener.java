package io.github.biglv666.cachekit.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import io.github.biglv666.cachekit.core.CacheKeyCustomizer;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.exception.CacheKitException;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import io.github.biglv666.cachekit.support.NamingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * binlog 行事件失效监听器：订阅 MySQL binlog，任何来源（DBA、其他服务、脚本）对
 * 已缓存实体对应表的写入都会按行提取主键并触发 {@link TieredEntityCache#evict}，
 * 弥补"绕过应用的写无法广播失效"这个盲区。
 *
 * <p>表 → 主键列序号与列名列表通过 information_schema 惰性解析并缓存；单条事件解析
 * 失败只打警告，不中断监听（残余脏数据由 TTL 上界兜底）。</p>
 *
 * <p>自定义键段还原：宿主注册了 {@link CacheKeyCustomizer} 时（多租户/多数据源），
 * 普通键组装依赖应用线程上下文，binlog 解析线程拿不到。对覆写了
 * {@link CacheKeyCustomizer#segmentFor} 的自定义器，按行数据（列名 → 值）还原键段、
 * 拼出与读路径一致的精确键失效；无法还原的行按既有语义跳过（TTL/双删兜底）
 * 并计入 {@code cache-kit.binlog.derive.skipped} 指标。</p>
 */
public class BinlogInvalidationListener implements BinaryLogClient.EventListener {

    private static final Logger log = LoggerFactory.getLogger(BinlogInvalidationListener.class);

    private final TieredEntityCache tieredCache;
    private final EntityMetadataRegistry registry;
    private final DataSource dataSource;
    private final String database;
    /** 缓存键命名空间：与 TieredEntityCache.key() 的组装规则对齐（自定义段在最前） */
    private final String namespace;
    private final List<CacheKeyCustomizer> keyCustomizers;

    /** TABLE_MAP 事件维护 tableId → 库/表名 映射 */
    private final Map<Long, TableMapEventData> tableMap = new ConcurrentHashMap<>();
    /** 表 → 有序列名列名列表（binlog 行镜像按 ORDINAL_POSITION 排列），键段还原用；空列表 = 无数据源等不可用场景 */
    private final Map<String, List<String>> tableColumns = new ConcurrentHashMap<>();
    /** 表 → 解析失败时刻：DB 抖动时避免每个行事件都重查 information_schema */
    private final Map<String, Long> pkLookupFailedAt = new ConcurrentHashMap<>();
    private final Map<String, Long> columnLookupFailedAt = new ConcurrentHashMap<>();
    private static final long PK_LOOKUP_COOLDOWN_NS = 30_000_000_000L;
    private volatile long lastPkLookupWarnAt;
    private volatile long lastDeriveSkippedWarnAt;
    private volatile long lastCompositeWarnAt;
    /** 已接收事件总数（含非行事件）：供 BinlogLifecycle 判断"连上后是否收到过事件" */
    private final java.util.concurrent.atomic.AtomicLong receivedEvents = new AtomicLong();
    private volatile io.github.biglv666.cachekit.core.CacheMetricsListener metrics =
            new io.github.biglv666.cachekit.core.CacheMetricsListener() {
            };

    /** 挂载指标监听器（失效传播延迟埋点） */
    public void setMetricsListener(io.github.biglv666.cachekit.core.CacheMetricsListener metrics) {
        this.metrics = metrics == null ? new io.github.biglv666.cachekit.core.CacheMetricsListener() {
        } : metrics;
    }

    public BinlogInvalidationListener(TieredEntityCache tieredCache,
                                      EntityMetadataRegistry registry,
                                      DataSource dataSource,
                                      String database) {
        this(tieredCache, registry, dataSource, database, "", List.of());
    }

    /**
     * @param namespace      缓存键命名空间（cache-kit.key-namespace），精确键组装用
     * @param keyCustomizers 宿主注册的键自定义段；其中覆写 {@code segmentFor} 的可在
     *                       binlog 失效时从行数据还原段、拼精确键
     */
    public BinlogInvalidationListener(TieredEntityCache tieredCache,
                                      EntityMetadataRegistry registry,
                                      DataSource dataSource,
                                      String database,
                                      String namespace,
                                      List<CacheKeyCustomizer> keyCustomizers) {
        this.tieredCache = tieredCache;
        this.registry = registry;
        this.dataSource = dataSource;
        this.database = database;
        this.namespace = namespace == null ? "" : namespace;
        this.keyCustomizers = keyCustomizers == null ? List.of() : List.copyOf(keyCustomizers);
    }

    /** 已接收事件总数快照（BinlogLifecycle 重连判定用） */
    public long receivedEventCount() {
        return receivedEvents.get();
    }

    /** 自 {@code mark} 快照后是否收到过新事件 */
    public boolean receivedEventsSince(long mark) {
        return receivedEvents.get() != mark;
    }

    @Override
    public void onEvent(Event event) {
        receivedEvents.incrementAndGet();
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
                        evictRows(event.getHeader().getTimestamp(),
                                ((WriteRowsEventData) event.getData()).getTableId(),
                                ((WriteRowsEventData) event.getData()).getRows());
                case DELETE_ROWS, EXT_DELETE_ROWS ->
                        evictRows(event.getHeader().getTimestamp(),
                                ((DeleteRowsEventData) event.getData()).getTableId(),
                                ((DeleteRowsEventData) event.getData()).getRows());
                case UPDATE_ROWS, EXT_UPDATE_ROWS -> {
                    UpdateRowsEventData data = (UpdateRowsEventData) event.getData();
                    // UPDATE 的 before/after 两幅镜像主键都要失效：主键本身被改时，
                    // 旧主键的缓存条目仍指向已迁移的旧行，只失效 after 会漏清旧键（幽灵行）
                    List<Serializable[]> bothImages = new ArrayList<>(data.getRows().size() * 2);
                    for (Map.Entry<Serializable[], Serializable[]> row : data.getRows()) {
                        bothImages.add(row.getKey());
                        bothImages.add(row.getValue());
                    }
                    evictRows(event.getHeader().getTimestamp(), data.getTableId(), bothImages);
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
     *
     * <p>无自定义段时全部行走精确主键键；有自定义段时逐行尝试还原——还原成功的行
     * 同时失效含段精确键（{@link TieredEntityCache#evictExactKeys}）与无段键
     * （{@code segment()} 为 null 的写入会产生无段键，双保险每批只多一条空 DEL）；
     * 还原失败的行仅无段键（既有行为，多租户键不命中，TTL 兜底）并计数。</p>
     */
    private void evictRows(long eventTimestampSeconds, long tableId, List<Serializable[]> rows) {
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
        List<PkColumn> pkCols = pkColumns(tm.getTable());
        if (pkCols.isEmpty()) {
            return;
        }
        // 复合主键：联合键段顺序必须与元数据字段声明序一致，需要列名把行值按名定位
        boolean composite = pkCols.size() > 1;
        boolean hasCustomizers = !keyCustomizers.isEmpty();
        List<String> columns = (hasCustomizers || composite) ? orderedColumns(tm.getTable()) : null;
        if (composite && columns == null) {
            // 无列名无法还原复合键序：整事件失效跳过（TTL/延迟双删兜底）
            warnCompositeSkipped(tm.getTable(), rows.size(), "无法解析表列名");
            return;
        }

        List<Object> plainIds = new ArrayList<>(rows.size());
        Set<String> derivedKeys = new LinkedHashSet<>();
        int rowsWithoutPk = 0;
        int deriveSkipped = 0;
        int compositeSkipped = 0;
        boolean columnsRetried = false;
        int maxPkOrdinal = pkCols.get(pkCols.size() - 1).ordinal();
        for (Serializable[] row : rows) {
            if (maxPkOrdinal > row.length) {
                // 行镜像里没有主键列（binlog_row_image=MINIMAL 时 UPDATE/DELETE 的镜像可能缺列）
                rowsWithoutPk++;
                continue;
            }
            Object id;
            if (composite) {
                id = compositeId(meta, columns, row);
                if (id == null) {
                    compositeSkipped++;
                    continue;
                }
            } else {
                id = toCacheId(row[pkCols.get(0).ordinal() - 1]);
            }
            if (id == null) {
                continue;
            }
            plainIds.add(id);
            if (hasCustomizers) {
                // 行镜像列数与缓存列数不一致：说明表结构在事件后变更，重查一次列名；
                // 仍不一致则本事件该行跳过还原（不再反复重查，防查询风暴）
                if (columns == null || columns.size() != row.length) {
                    if (!columnsRetried) {
                        columnsRetried = true;
                        tableColumns.remove(tm.getTable());
                        columns = orderedColumns(tm.getTable());
                    }
                }
                String exactKey = columns != null && columns.size() == row.length
                        ? deriveKey(meta, columns, row, id)
                        : null;
                if (exactKey != null) {
                    derivedKeys.add(exactKey);
                } else {
                    deriveSkipped++;
                }
            }
        }
        if (rowsWithoutPk > 0) {
            warnRowImageLimited(tm.getTable(), rowsWithoutPk);
        }
        if (compositeSkipped > 0) {
            warnCompositeSkipped(tm.getTable(), compositeSkipped,
                    "主键列与实体字段（驼峰↔蛇形）无法对齐或键段含 ':'");
        }
        if (!plainIds.isEmpty()) {
            tieredCache.evictBatch(meta, plainIds);
        }
        if (!derivedKeys.isEmpty()) {
            tieredCache.evictExactKeys(derivedKeys);
        }
        if (deriveSkipped > 0) {
            metrics.binlogDeriveSkipped(deriveSkipped);
            warnDeriveSkipped(tm.getTable(), deriveSkipped);
        }
        if (!plainIds.isEmpty() || !derivedKeys.isEmpty()) {
            // 失效传播延迟（MySQL 事件时间 → 本实例失效应用）：受两侧时钟偏差影响，仅作趋势观测
            metrics.invalidationDelayMillis(System.currentTimeMillis() - eventTimestampSeconds * 1000L);
        }
    }

    /**
     * 复合主键行的联合键段：binlog 行值按表列序（ORDINAL_POSITION）排列，而缓存键段按
     * 实体字段声明序——经"字段名 ↔ 列名"（驼峰转蛇形，兼容列名与字段同名）映射对齐。
     * 任一字段映射不到列（结构漂移/命名不符）返回 null，该行失效跳过（TTL/双删兜底）。
     */
    private Object compositeId(EntityMetadata meta, List<String> columns, Serializable[] row) {
        Map<String, Serializable> byColumn = new LinkedHashMap<>(columns.size() * 2);
        for (int i = 0; i < columns.size() && i < row.length; i++) {
            byColumn.put(columns.get(i), row[i]);
        }
        List<Object> segments = new ArrayList<>(meta.idFields().size());
        for (Field field : meta.idFields()) {
            String snake = NamingUtils.camelToSnake(field.getName());
            Serializable v = byColumn.containsKey(snake)
                    ? byColumn.get(snake)
                    : byColumn.get(field.getName());
            if (v == null) {
                // 主键列不可为 null：走到这里说明列缺失或未对齐，按跳过处理
                return null;
            }
            segments.add(toCacheId(v));
        }
        try {
            return EntityMetadata.joinSegments(segments);
        } catch (CacheKitException e) {
            // 段含 ':' 的复合值不可能已被缓存（读路径同样拒绝），跳过该行
            return null;
        }
    }

    /** 复合主键行无法还原联合键段的限频告警（30s）：该批行的失效丢失由 TTL/双删兜底 */
    private void warnCompositeSkipped(String table, int rows, String reason) {
        long now = System.nanoTime();
        if (now - lastCompositeWarnAt > 30_000_000_000L) {
            lastCompositeWarnAt = now;
            log.warn("表 {}（复合主键）有 {} 行无法还原联合键段（{}），"
                    + "这些行的 binlog 失效被跳过（仅 TTL/延迟双删兜底）", table, rows, reason);
        }
    }

    /**
     * 从行数据还原该行的完整缓存键：各自定义段（segmentFor）+ 命名空间 + 实体前缀 + 主键，
     * 与 {@code TieredEntityCache.key()} 的组装顺序一致。任一段无法还原即返回 null
     * （部分还原的键是错键，绝不能用于失效）。
     */
    private String deriveKey(EntityMetadata meta, List<String> columns, Serializable[] row, Object id) {
        Map<String, Serializable> rowData = new LinkedHashMap<>(columns.size() * 2);
        for (int i = 0; i < columns.size(); i++) {
            rowData.put(columns.get(i), row[i]);
        }
        StringBuilder sb = new StringBuilder();
        for (CacheKeyCustomizer customizer : keyCustomizers) {
            String segment;
            try {
                segment = customizer.segmentFor(meta, rowData);
            } catch (Exception e) {
                // 还原异常按"无法还原"处理：部分键段缺失的键会失效不到真实缓存条目
                warnDeriveFailure(customizer, e);
                return null;
            }
            if (segment == null || segment.isBlank()) {
                return null;
            }
            sb.append(segment).append(':');
        }
        if (!namespace.isBlank()) {
            sb.append(namespace).append(':');
        }
        return sb.append(meta.prefix()).append(':').append(id).toString();
    }

    private volatile long lastRowImageWarnAt;

    /** binlog_row_image=MINIMAL 会导致按行失效静默丢失，必须显式告警（30s 限频） */
    private void warnRowImageLimited(String table, int rowsWithoutPk) {
        long now = System.nanoTime();
        if (now - lastRowImageWarnAt > 30_000_000_000L) {
            lastRowImageWarnAt = now;
            log.warn("表 {} 有 {} 行事件不含主键列（binlog_row_image 疑似 MINIMAL），"
                            + "这些行的缓存失效将丢失。请设置 binlog_row_image=FULL",
                    table, rowsWithoutPk);
        }
    }

    /** 键段还原跳过告警（30s 限频）：该行的 binlog 精确失效未执行，由 TTL/双删兜底 */
    private void warnDeriveSkipped(String table, int skipped) {
        long now = System.nanoTime();
        if (now - lastDeriveSkippedWarnAt > 30_000_000_000L) {
            lastDeriveSkippedWarnAt = now;
            log.warn("表 {} 有 {} 行无法还原自定义键段（自定义器未实现 segmentFor 或行结构漂移），"
                    + "这些行的 binlog 精确失效被跳过（仅 TTL/延迟双删兜底）。"
                    + "多租户键请实现 CacheKeyCustomizer.segmentFor 从行数据还原段", table, skipped);
        }
    }

    private void warnDeriveFailure(CacheKeyCustomizer customizer, Exception e) {
        long now = System.nanoTime();
        if (now - lastDeriveSkippedWarnAt > 30_000_000_000L) {
            lastDeriveSkippedWarnAt = now;
            log.warn("CacheKeyCustomizer.segmentFor 抛出异常，该行按无法还原处理（本告警 30s 内不重复）: "
                    + customizer.getClass().getName(), e);
        }
    }

    /** information_schema 里的主键列：列名用于复合主键与元数据字段声明序对齐（供测试覆写解析） */
    protected record PkColumn(String name, int ordinal) {
    }

    private final Map<String, List<PkColumn>> pkColumnsCache = new ConcurrentHashMap<>();

    /**
     * 表的全部主键列（1 起算序号，按 ORDINAL_POSITION 排序）：单主键单元素、复合主键多元素，
     * 空列表表示"无主键/不可解析/冷却中"。供测试覆写。
     */
    protected List<PkColumn> pkColumns(String table) {
        Long failedAt = pkLookupFailedAt.get(table);
        if (failedAt != null && System.nanoTime() - failedAt < PK_LOOKUP_COOLDOWN_NS) {
            return List.of();
        }
        return pkColumnsCache.computeIfAbsent(table, t -> {
            if (dataSource == null) {
                log.warn("未配置 DataSource，无法解析表 {} 的主键列，该表 binlog 失效被跳过", t);
                return List.of();
            }
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "SELECT COLUMN_NAME, ORDINAL_POSITION FROM information_schema.COLUMNS "
                                 + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_KEY = 'PRI' "
                                 + "ORDER BY ORDINAL_POSITION")) {
                ps.setString(1, database);
                ps.setString(2, t);
                try (ResultSet rs = ps.executeQuery()) {
                    List<PkColumn> cols = new ArrayList<>();
                    while (rs.next()) {
                        cols.add(new PkColumn(rs.getString(1), rs.getInt(2)));
                    }
                    pkLookupFailedAt.remove(t);
                    return List.copyOf(cols);
                }
            } catch (Exception e) {
                pkLookupFailedAt.put(t, System.nanoTime());
                long now = System.nanoTime();
                if (now - lastPkLookupWarnAt > 30_000_000_000L) {
                    lastPkLookupWarnAt = now;
                    log.warn("查询表 {} 主键列失败，该表 binlog 失效进入 30s 冷却（TTL 兜底）", t, e);
                }
                throw new CacheKitException("查询表 " + t + " 主键列失败", e);
            }
        });
    }

    /**
     * 表的全部列名（按 ORDINAL_POSITION 排序，与 binlog 行镜像的列序一致），惰性查询并按表缓存。
     * 空列表缓存表示"无数据源、不可解析"；查询失败进入 30s 冷却（返回 null，还原跳过）。
     * 供测试覆写。
     */
    protected List<String> orderedColumns(String table) {
        Long failedAt = columnLookupFailedAt.get(table);
        if (failedAt != null && System.nanoTime() - failedAt < PK_LOOKUP_COOLDOWN_NS) {
            return null;
        }
        List<String> cached = tableColumns.get(table);
        if (cached != null) {
            return cached.isEmpty() ? null : cached;
        }
        if (dataSource == null) {
            tableColumns.put(table, List.of());
            return null;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                             + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")) {
            ps.setString(1, database);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                List<String> cols = new ArrayList<>();
                while (rs.next()) {
                    cols.add(rs.getString(1));
                }
                columnLookupFailedAt.remove(table);
                // 空结果也按表缓存（防止查不到的表每个事件都重查）
                tableColumns.put(table, List.copyOf(cols));
                return cols.isEmpty() ? null : cols;
            }
        } catch (Exception e) {
            columnLookupFailedAt.put(table, System.nanoTime());
            long now = System.nanoTime();
            if (now - lastPkLookupWarnAt > 30_000_000_000L) {
                lastPkLookupWarnAt = now;
                log.warn("查询表 {} 列名失败，binlog 键段还原进入 30s 冷却（失效走无段键，TTL 兜底）", table, e);
            }
            return null;
        }
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
