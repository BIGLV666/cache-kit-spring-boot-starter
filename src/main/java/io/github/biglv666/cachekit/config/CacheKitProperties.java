package io.github.biglv666.cachekit.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * cache-kit 配置项，前缀 {@code cache-kit.*}。
 */
@ConfigurationProperties(prefix = "cache-kit")
public class CacheKitProperties {

    private boolean enabled = true;
    /**
     * 缓存键全局命名空间。未配置时自动取 spring.application.name；
     * 多服务共享 Redis 时用于隔离键空间（同名实体会互相命中返回错数据而非未命中），
     * 也用于主键类型/表名迁移时整体弃用旧键。
     */
    private String keyNamespace = "";
    /**
     * 为 true 时：keyNamespace 与 spring.application.name 均为空则启动失败。
     * 多服务/多环境共享 Redis 且无命名空间时，同名实体会跨服务/跨环境互相命中返回错数据，
     * 默认只打警告；对键冲突零容忍的宿主（如测试环境与生产共用 Redis）打开此开关。
     */
    private boolean requireKeyNamespace = false;

    public String getKeyNamespace() {
        return keyNamespace;
    }

    public void setKeyNamespace(String keyNamespace) {
        this.keyNamespace = keyNamespace;
    }

    public boolean isRequireKeyNamespace() {
        return requireKeyNamespace;
    }

    public void setRequireKeyNamespace(boolean requireKeyNamespace) {
        this.requireKeyNamespace = requireKeyNamespace;
    }

    private final L1 l1 = new L1();
    private final L2 l2 = new L2();
    private final Broadcast broadcast = new Broadcast();
    private final Mp mp = new Mp();
    private final Binlog binlog = new Binlog();
    private final Tx tx = new Tx();
    private final Warmup warmup = new Warmup();

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

    public Warmup getWarmup() {
        return warmup;
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
         *
         * <p>关闭的代价：失效立即执行，发生在提交前——并发读会在"删除后、提交前"把旧值回填；
         * 延迟双删按删除时刻 +1s 调度，若事务总耗时超过 double-delete-delay（默认 1s），
         * 第二次删除也发生在提交前，回填的旧值将存活至 L2 TTL（默认 10 分钟）。
         * 长事务场景不要关闭此开关。</p>
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
        /**
         * binlog 副本 server-id，同一 MySQL 上必须唯一（与 MySQL 自身及其他副本都不同）。
         * 缺省自动生成随机值；同一库上部署多个启用 binlog 的实例时建议显式配置以避免
         * 极小概率的随机冲突（冲突表现为复制连接被 MySQL 反复踢掉）。
         */
        private Integer serverId;

        /**
         * GTID 模式（cache-kit.binlog.gtid-enabled）：用 GTID 集替代 file/position 定位位点。
         * 主从切换后 file/position 失效而 GTID 仍连续，且 connector 内置
         * gtidSetFallbackToPurged（位点被 PURGE 后自动回退最新）——生产高可用场景推荐。
         * 未显式给 gtid-set 时启动时查询 @@global.gtid_executed 作为起点
         * （connector 默认从最早可用事件回放历史，会造成启动失效风暴）。
         */
        private boolean gtidEnabled = false;
        /** 初始 GTID 集（gtid-enabled=true 时生效）；缺省启动时以 @@global.gtid_executed 为起点 */
        private String gtidSet;

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

        public boolean isGtidEnabled() {
            return gtidEnabled;
        }

        public void setGtidEnabled(boolean gtidEnabled) {
            this.gtidEnabled = gtidEnabled;
        }

        public String getGtidSet() {
            return gtidSet;
        }

        public void setGtidSet(String gtidSet) {
            this.gtidSet = gtidSet;
        }
    }

    /** L1 本地缓存配置 */
    public static class L1 {
        /** 最大条目数 */
        private long maxEntries = 65536;
        /**
         * 权重上限（单位：K 字符，按序列化 JSON 的 UTF-16 字符数计，非字节）：>0 时启用并取代
         * maxEntries，防大实体撑爆 L1；0 关闭。注意中文等 BMP 字符的 JVM 内存占用约为该值的 2 倍，
         * 实际内存上限 ≈ maxWeightKb × 2KB。
         */
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
        /**
         * L2 TTL 基准值（命中抖动前的基准）。
         * 非正值（0/负数）的统一语义是"该键跳过 L2 写入"（等效禁用 L2），不是"永不过期"——
         * 组件的脏数据安全模型建立在 TTL 上界之上，不提供无 TTL 写入。
         * 配置为非正值时启动会打警告。
         */
        private Duration ttl = Duration.ofMinutes(10);
        /** 追加在 TTL 上的随机抖动上限，0 表示关闭（防雪崩）；基准 TTL 非正时不叠加抖动 */
        private Duration jitter = Duration.ofSeconds(60);
        /** null 结果（防穿透占位）的 TTL；非正值表示不缓存 null 占位（关闭穿透防护） */
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
        /** 是否启用失效广播（需 Redis；多实例部署必须开启） */
        private boolean enabled = true;
        /** 广播通道：pubsub（fire-and-forget，进程内存占用为零）或 streams（消费组 ACK，实例短暂掉线不丢） */
        private String mode = "pubsub";
        /** 广播 topic（streams 模式下为 Stream 键） */
        private String topic = "cache-kit:invalidate";
        /** streams 模式下 Stream 的近似裁剪上界（XADD MAXLEN ~）：防无限增长 */
        private int streamsMaxlen = 10_000;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getStreamsMaxlen() {
            return streamsMaxlen;
        }

        public void setStreamsMaxlen(int streamsMaxlen) {
            this.streamsMaxlen = streamsMaxlen;
        }
    }

    /** 启动预热配置 */
    public static class Warmup {
        /** 是否执行 @CacheWarmup 预热方法 */
        private boolean enabled = true;
        /** 预热线程并行度：1 为顺序执行；>1 时同 order 的方法并发执行 */
        private int parallelism = 1;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getParallelism() {
            return parallelism;
        }

        public void setParallelism(int parallelism) {
            this.parallelism = parallelism;
        }
    }

    /** MyBatis-Plus 适配配置 */
    public static class Mp {
        /**
         * MP 自动缓存总开关，同时控制两个切面：
         * ① BaseMapper 内置方法（selectById/selectBatchIds/updateById/deleteById/deleteByIds/insert）；
         * ② IService 批量写（saveBatch/updateBatchById/saveOrUpdateBatch——这些方法在 MP 内部
         * 经 SqlSession 批量语句执行、绕过 mapper 代理，必须独立切面才能失效）。
         * 条件写 update(Wrapper)/delete(Wrapper) 不在覆盖范围（不带主键值），靠 binlog/TTL 兜底。
         */
        private boolean autoCacheBaseMethods = true;

        public boolean isAutoCacheBaseMethods() {
            return autoCacheBaseMethods;
        }

        public void setAutoCacheBaseMethods(boolean autoCacheBaseMethods) {
            this.autoCacheBaseMethods = autoCacheBaseMethods;
        }
    }
}
