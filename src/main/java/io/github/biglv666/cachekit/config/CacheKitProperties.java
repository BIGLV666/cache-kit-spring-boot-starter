package io.github.biglv666.cachekit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * cache-kit 配置项，前缀 {@code cache-kit.*}。
 */
@ConfigurationProperties(prefix = "cache-kit")
public class CacheKitProperties {

    private boolean enabled = true;
    /** 缓存键全局命名空间：多套环境/主键类型迁移时整体弃用旧键（键形如 ns:表名:主键） */
    private String keyNamespace = "";

    public String getKeyNamespace() {
        return keyNamespace;
    }

    public void setKeyNamespace(String keyNamespace) {
        this.keyNamespace = keyNamespace;
    }

    private final L1 l1 = new L1();
    private final L2 l2 = new L2();
    private final Broadcast broadcast = new Broadcast();
    private final Mp mp = new Mp();
    private final Binlog binlog = new Binlog();
    private final Tx tx = new Tx();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Tx getTx() {
        return tx;
    }

    public L1 getL1() {
        return l1;
    }

    public L2 getL2() {
        return l2;
    }

    public Broadcast getBroadcast() {
        return broadcast;
    }

    public Mp getMp() {
        return mp;
    }

    public Binlog getBinlog() {
        return binlog;
    }

    /**
     * 事务失效配置
     */
    public static class Tx {
        /**
         * 写方法处于活动事务时，缓存失效延迟到 afterCommit 执行：
         * 消除"删除发生在事务提交前，并发读回填旧值"的窗口。
         * 事务回滚则不失效（数据未变）。
         */
        private boolean evictAfterCommit = true;

        public boolean isEvictAfterCommit() {
            return evictAfterCommit;
        }

        public void setEvictAfterCommit(boolean evictAfterCommit) {
            this.evictAfterCommit = evictAfterCommit;
        }
    }

    /**
     * binlog 直连失效配置：订阅 MySQL binlog 的行事件，任何来源（DBA、其他服务）写入
     * 已缓存实体对应的表都会触发缓存失效，弥补"绕过应用的写"这个广播盲区。
     * host/port/database 缺省时从 spring.datasource.url 解析，账号缺省用数据源的。
     */
    public static class Binlog {
        /** 是否启用（需 mysql-binlog-connector-java 在类路径，且 MySQL 开启 log_bin + ROW 格式） */
        private boolean enabled = false;
        /** MySQL 地址，缺省从 spring.datasource.url 解析 */
        private String host;
        /** MySQL 端口，缺省从 spring.datasource.url 解析 */
        private Integer port;
        /** 库名，缺省从 spring.datasource.url 解析 */
        private String database;
        /** 复制账号（需 REPLICATION SLAVE 权限），缺省用数据源账号 */
        private String username;
        private String password;
        /** binlog 副本 server-id，集群内必须唯一 */
        private Integer serverId = 18365;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public Integer getPort() {
            return port;
        }

        public void setPort(Integer port) {
            this.port = port;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Integer getServerId() {
            return serverId;
        }

        public void setServerId(Integer serverId) {
            this.serverId = serverId;
        }
    }

    /** L1 本地缓存配置 */
    public static class L1 {
        /** 最大条目数 */
        private long maxEntries = 65536;
        /** 权重上限（KB，按序列化后 JSON 长度计）：>0 时启用并取代 maxEntries，防大实体撑爆 L1；0 关闭 */
        private long maxWeightKb = 0;

        public long getMaxWeightKb() {
            return maxWeightKb;
        }

        public void setMaxWeightKb(long maxWeightKb) {
            this.maxWeightKb = maxWeightKb;
        }
        /** L1 TTL：必须显著小于 l2.ttl，作为 pub/sub 丢消息时的脏读上界 */
        private Duration ttl = Duration.ofSeconds(30);

        public long getMaxEntries() {
            return maxEntries;
        }

        public void setMaxEntries(long maxEntries) {
            this.maxEntries = maxEntries;
        }

        public Duration getTtl() {
            return ttl;
        }

        public void setTtl(Duration ttl) {
            this.ttl = ttl;
        }
    }

    /** L2 远程缓存配置 */
    public static class L2 {
        /** L2 TTL 基准值（命中抖动前的基准） */
        private Duration ttl = Duration.ofMinutes(10);
        /** 追加在 TTL 上的随机抖动上限，0 表示关闭（防雪崩） */
        private Duration jitter = Duration.ofSeconds(60);
        /** null 结果（防穿透占位）的 TTL */
        private Duration nullTtl = Duration.ofSeconds(30);
        /** 延迟双删的延迟时长 */
        private Duration doubleDeleteDelay = Duration.ofSeconds(1);

        public Duration getTtl() {
            return ttl;
        }

        public void setTtl(Duration ttl) {
            this.ttl = ttl;
        }

        public Duration getJitter() {
            return jitter;
        }

        public void setJitter(Duration jitter) {
            this.jitter = jitter;
        }

        public Duration getNullTtl() {
            return nullTtl;
        }

        public void setNullTtl(Duration nullTtl) {
            this.nullTtl = nullTtl;
        }

        public Duration getDoubleDeleteDelay() {
            return doubleDeleteDelay;
        }

        public void setDoubleDeleteDelay(Duration doubleDeleteDelay) {
            this.doubleDeleteDelay = doubleDeleteDelay;
        }
    }

    /** 失效广播配置 */
    public static class Broadcast {
        /** 是否启用 pub/sub 失效广播（需 Redis；多实例部署必须开启） */
        private boolean enabled = true;
        /** 广播 topic */
        private String topic = "cache-kit:invalidate";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }
    }

    /** MyBatis-Plus 适配配置 */
    public static class Mp {
        /** BaseMapper 内置方法（selectById/updateById/deleteById/selectBatchIds）是否自动接入缓存 */
        private boolean autoCacheBaseMethods = true;

        public boolean isAutoCacheBaseMethods() {
            return autoCacheBaseMethods;
        }

        public void setAutoCacheBaseMethods(boolean autoCacheBaseMethods) {
            this.autoCacheBaseMethods = autoCacheBaseMethods;
        }
    }
}
