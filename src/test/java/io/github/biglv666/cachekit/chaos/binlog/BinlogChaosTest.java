package io.github.biglv666.cachekit.chaos.binlog;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.github.biglv666.cachekit.BinlogLifecycle;
import io.github.biglv666.cachekit.binlog.BinlogInvalidationListener;
import io.github.biglv666.cachekit.channel.CaffeineChannel;
import io.github.biglv666.cachekit.chaos.support.ChaosContainers;
import io.github.biglv666.cachekit.config.CacheKitProperties;
import io.github.biglv666.cachekit.core.CacheKeyCustomizer;
import io.github.biglv666.cachekit.core.CacheMetricsListener;
import io.github.biglv666.cachekit.core.NoopInvalidationPublisher;
import io.github.biglv666.cachekit.core.TieredEntityCache;
import io.github.biglv666.cachekit.metadata.EntityMetadata;
import io.github.biglv666.cachekit.metadata.EntityMetadataRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * binlog 混沌（真实 MySQL 3307，绕过应用的直写）：
 * 表结构在线变更（ALTER TABLE ADD COLUMN）后，information_schema 缓存列数与
 * 行镜像不一致 → 设计为"重查一次"，重查后列数对齐则键段还原自愈、精确失效照常命中；
 * 整个过程监听线程不得中断，后续事件照常处理。
 *
 * <p>与单元测试的区别：单元测试覆写 {@code orderedColumns} 验证的是跳过分支，
 * 这里验证的是真实 information_schema 重查后的自愈分支。</p>
 */
class BinlogChaosTest {

    private static final String DB = "cachekit_test";

    /** 测试租户上下文（binlog 解析线程拿不到，靠 segmentFor 从行的 tenant_id 列还原） */
    static final ThreadLocal<String> TENANT = new ThreadLocal<>();

    @TableName("t_drift")
    static class DriftOrder {
        @TableId
        Long id;
        String name;
    }

    static class RecordingMetrics implements CacheMetricsListener {
        final AtomicInteger deriveSkipped = new AtomicInteger();

        @Override
        public void binlogDeriveSkipped(int rows) {
            deriveSkipped.addAndGet(rows);
        }
    }

    private static BinlogLifecycle lifecycle;
    private static JdbcTemplate jdbc;
    private static CaffeineChannel l1;
    private static RecordingMetrics metrics;
    private static EntityMetadata meta;
    private static TieredEntityCache cache;

    @BeforeAll
    static void setUp() throws Exception {
        Assumptions.assumeTrue(mysqlAvailable(), "本地无带 binlog 的 MySQL(3307)，跳过 binlog 混沌测试");
        jdbc = new JdbcTemplate(dataSource());
        l1 = new CaffeineChannel(100);
        metrics = new RecordingMetrics();

        EntityMetadataRegistry registry = new EntityMetadataRegistry();
        registry.find(DriftOrder.class);
        meta = registry.require(DriftOrder.class);

        CacheKeyCustomizer tenantCustomizer = new CacheKeyCustomizer() {
            @Override
            public String segment() {
                return TENANT.get();
            }

            @Override
            public String segmentFor(EntityMetadata m, Map<String, Serializable> rowData) {
                Object tenant = rowData.get("tenant_id");
                return tenant == null ? null : String.valueOf(tenant);
            }
        };
        cache = new TieredEntityCache(new CacheKitProperties(), l1, null,
                new NoopInvalidationPublisher(), null, "", List.of(tenantCustomizer));
        BinlogInvalidationListener listener = new BinlogInvalidationListener(
                cache, registry, dataSource(), DB, "", List.of(tenantCustomizer));
        listener.setMetricsListener(metrics);
        BinaryLogClient client = new BinaryLogClient("localhost", 3307, "root", "root");
        client.setServerId(ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE));
        client.setKeepAlive(true);
        client.registerEventListener(listener);
        lifecycle = new BinlogLifecycle(client, listener, "chaos-binlog-drift");
        lifecycle.start();
    }

    @AfterAll
    static void tearDown() {
        if (lifecycle != null) {
            lifecycle.stop();
        }
    }

    private static DataSource dataSource() {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:mysql://localhost:3307/" + DB, "root", "root");
        ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        return ds;
    }

    private static boolean mysqlAvailable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", 3307), 500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    void schemaDriftShouldSelfHealViaColumnRequeryAndKeepListenerAlive() throws Exception {
        jdbc.execute("DROP TABLE IF EXISTS t_drift");
        jdbc.execute("CREATE TABLE t_drift (id BIGINT PRIMARY KEY, tenant_id VARCHAR(16), name VARCHAR(64))");
        jdbc.update("INSERT INTO t_drift VALUES (1, 't1', 'a0')");

        // 1) 租户上下文首读：键落在 t1:t_drift:1（列名元数据此时按 3 列缓存）
        TENANT.set("t1");
        try {
            assertThat(load(1)).isEqualTo("a0");

            // 2) 在线加列（结构漂移）：此后行事件 4 列 vs 缓存 3 列 → 触发一次重查
            jdbc.execute("ALTER TABLE t_drift ADD COLUMN extra VARCHAR(16)");
            jdbc.update("UPDATE t_drift SET name='a1' WHERE id=1");

            // 3) 自愈断言：重查拿到 4 列 → 还原成功 → 租户键被精确失效 → 读到新值；
            //    绝无跳过计数（跳过只发生在重查后仍不一致的场景）
            long deadline = System.currentTimeMillis() + 15_000;
            String observed;
            while (true) {
                observed = load(1);
                if ("a1".equals(observed) || System.currentTimeMillis() > deadline) {
                    break;
                }
                Thread.sleep(200);
            }
            assertThat(observed).as("加列后键段还原必须经重查自愈、租户键精确失效").isEqualTo("a1");
            assertThat(metrics.deriveSkipped.get()).as("自愈路径不应产生跳过计数").isZero();

            // 4) 监听线程存活：漂移后的后续事件照常处理
            jdbc.update("UPDATE t_drift SET name='a2' WHERE id=1");
            deadline = System.currentTimeMillis() + 15_000;
            while (true) {
                observed = load(1);
                if ("a2".equals(observed) || System.currentTimeMillis() > deadline) {
                    break;
                }
                Thread.sleep(200);
            }
            assertThat(observed).as("漂移事件之后监听必须继续工作").isEqualTo("a2");
        } finally {
            TENANT.remove();
        }
    }

    /** 固定 cache 实例的 read-through：loader 即"查 DB"（JDBC），与业务读路径同构 */
    private String load(long id) {
        DriftOrder order = (DriftOrder) cache.load(meta, id, null, true,
                () -> jdbc.queryForObject("SELECT id, name FROM t_drift WHERE id=?",
                        (rs, i) -> {
                            DriftOrder o = new DriftOrder();
                            o.id = rs.getLong(1);
                            o.name = rs.getString(2);
                            return o;
                        }, id));
        return order == null ? null : order.name;
    }
}
