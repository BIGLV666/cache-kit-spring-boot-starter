# cache-kit-spring-boot-starter

实体元数据驱动的三级缓存组件：**Caffeine（L1）→ Redis（L2）→ DB（loader）** read-through。
对 MyBatis-Plus 用户**零注解接入**；binlog 直连失效补齐"绕过应用的写"盲区；一致性语义为**秒级最终一致**。

## 特性

- 三级 read-through：L1 → L2 → 方法体（查 DB），命中逐级回填
- 实体元数据自动推导缓存键（`表名:主键`），无需手写 SpEL 键表达式
- MyBatis-Plus 注解复用：`@TableName` / `@TableId` 即元数据，`selectById` / `selectBatchIds` / `updateById` / `deleteById` 零注解自动接入
- 主键批量查询 per-ID 拆解：命中部分直接用，缺失集合才回源；"已确认不存在"用 null 占位（IN 语义）
- 并发防护四件套：single-flight 防击穿、TTL 随机抖动防雪崩、null 短 TTL 防穿透、L1 短 TTL 兜底 pub/sub 丢消息
- 多实例一致性：写后删缓存 + Redis pub/sub 失效广播 + 延迟双删
- **binlog 直连失效（0.2.0+）**：直连 MySQL binlog，DBA 改库、其他服务写入也能秒级失效，无需部署 Canal
- 强一致读出口：`CacheKit.withDb(...)` 作用域旁路

## 快速开始

### 引入依赖

```xml
<dependency>
    <groupId>io.github.biglv666</groupId>
    <artifactId>cache-kit-spring-boot-starter</artifactId>
    <version>0.2.0</version>
</dependency>
```

要求 JDK 17+、Spring Boot 3.5.x。L2（Redis）与 MyBatis-Plus 适配按类路径自动启用。

### MyBatis-Plus 项目（零注解）

```java
@TableName("t_user")
public class User {
    @TableId
    private Long userId;
    private String userName;
}

userMapper.selectById(1L);    // 自动走三级缓存
userMapper.selectBatchIds(List.of(1L, 2L, 3L));  // per-ID 拆解：命中直接用，缺失才回源
userMapper.updateById(user);  // 自动失效 + 广播 + 延迟双删
```

批量语义：缓存键与单条查询完全相同（`表名:主键`），逐 ID 三态（命中值 / 已缓存空 / 未命中）；
"已确认不存在"缓存 null 占位（短 TTL），IN 查询里自动消失；结果按请求顺序输出、跳过不存在的 ID。
条件写（`update(Wrapper)` 等不带主键的批量写）无法精确失效——开启 binlog 可覆盖，否则靠 TTL 兜底。

### 非 MP 项目（实体注解）

```java
@CacheEntity(ttl = 3600)
public class User {
    @CacheId
    private Long userId;
    private String userName;
}

@Mapper
public interface UserMapper {
    @CachedQuery
    User selectByUserId(@Param("userId") Long userId);

    @CacheInvalidate(entity = User.class)
    int updateStatus(@Param("userId") Long userId, @Param("status") int status);
}
```

主键推导优先级：① 参数中有实体实例（`updateById(User)`）→ ② 参数名与主键字段同名（需 `-parameters` 编译，Boot 父 POM 默认开启）→ ③ 唯一标量参数 → ④ 唯一对象参数。全部失败抛 `CacheKitException`。

### 注入句柄（手动控制）

```java
@CacheHandle(User.class)
private EntityCache<User> userCache;

User u = userCache.get(id, () -> mapper.selectByUserId(id));
userCache.evict(id);
```

## binlog 直连失效（0.2.0+，覆盖"绕过应用的写"）

广播失效的前提是写走应用路径。DBA 改库、其他服务写同一张表时，cache-kit 无法感知——
binlog 直连失效模块补上这个盲区：以 MySQL replica 协议直连 binlog（ROW 格式），
任何来源对已缓存实体表的写入都会按行提取主键并触发失效。

**不需要部署 Canal Server**：类路径引入 `mysql-binlog-connector-java`（随本 starter optional 传递）
+ 开启开关即可。MySQL 需开启 `log_bin` 且账号具备 `REPLICATION SLAVE` 权限。

```yaml
cache-kit:
  binlog:
    enabled: true        # 缺省关闭
    # host/port/database 缺省从 spring.datasource.url 解析，账号缺省用数据源账号
    server-id: 18365     # 集群内必须唯一（与 MySQL server-id 及其他副本不同）
```

注意：MySQL 8.4 移除了 `SHOW MASTER STATUS`，需 connector 0.30.0+（本 starter 默认 0.31.0）。

实测：直写 → 失效 → 下次读到新值，端到端传播延迟 **avg 10.3ms / p99 14.2ms / max 53.7ms**（300 次直写零漏失效，含 5ms 轮询测量粒度）。

## 强一致读（豁免）

缓存是最终一致的。需要强一致的场景按档位选用：

| 档位 | 用法 | 适用 |
|---|---|---|
| 自定义方法不加注解 | `selectByIdFromDb(Long id)` | 已知某方法永远要 DB |
| ThreadLocal 旁路 | `CacheKit.withDb(() -> mapper.selectById(id))` | 调用点临时决定 |
| SpEL 条件 | `@CachedQuery(condition = "!forceDb")` | 由调用方传参决定 |

**库存扣减、余额等强一致判断所依赖的字段不要进缓存**——任何缓存方案（双删、binlog 均不例外）都是最终一致。

## 配置全参考（前缀 `cache-kit.*`）

```yaml
cache-kit:
  enabled: true            # 总开关
  l1:                      # 本地缓存（Caffeine）
    max-entries: 65536     # 最大条目数
    ttl: 30s               # L1 TTL：必须显著小于 l2.ttl，是 pub/sub 丢消息时的脏读上界
  l2:                      # 远程缓存（Redis，类路径无 spring-data-redis 时自动退化为 L1-only）
    ttl: 10m               # L2 TTL 基准
    jitter: 60s            # TTL 随机抖动上限（防雪崩），0 关闭
    null-ttl: 30s          # null 占位的短 TTL（防穿透）
    double-delete-delay: 1s  # 延迟双删间隔；写极热键时可调小或评估回源放大
  broadcast:               # 失效广播（多实例部署必须开启）
    enabled: true
    topic: cache-kit:invalidate
  mp:                      # MyBatis-Plus 适配
    auto-cache-base-methods: true  # BaseMapper 内置方法（selectById/updateById/deleteById）自动接入
  binlog:                  # binlog 直连失效（0.2.0+，缺省关闭）
    enabled: false
    host:                  # 缺省从 spring.datasource.url 解析
    port:
    database:
    username:              # 缺省用数据源账号
    password:
    server-id: 18365       # 集群内唯一
```

## 一致性机制与性能实测

写路径固定**先写 DB 后删缓存**（Cache-Aside 标准序）。删除动作 = 本地 L1 → DEL L2 → pub/sub 广播 → 延迟双删。

脏数据的核心窗口是**回填竞态**（读线程查到旧值、写线程删除后、读线程回填脏值）。延迟双删压缩该窗口，TTL 上界（L1 30s ≪ L2 10m）是唯一保证有上界的兜底。

以下为 PaperWise 宿主项目上的实测数据（单机 4~10 实例 + Docker Redis/MySQL，Java 17）：

| 场景 | 实测 |
|---|---|
| 缓存命中读 | 微秒级；20,249 个 HTTP 请求期间 MySQL InnoDB 行读取零增长 |
| 应用内写失效 | 每写增加约 1~2ms（DEL L2 + 广播 + 双删调度）；4.6 万次广播零丢失 |
| 广播最终一致性 | 10 实例、716 写/s 并发、4.6 万次广播：全实例最终版本校验 100% 通过 |
| 脏读窗口（应用内写） | 最大 119ms~505ms（含单 JVM 调度噪声），远小于 L1 TTL 30s 设计上界 |
| 脏读窗口（binlog 直写） | p99 14.2ms，300 次直写零漏失效 |
| 外部 HTTP 全链路 | 单实例饱和 ~1700 req/s；延迟基线由鉴权 Redis 往返主导，缓存命中本身微秒级 |
| 回源放大 | 热键上每次写约 3 次 DB 回源（广播删 + 双删删 + 在途回读），写极热键需评估 |

## MVP 边界（0.2.0）

- 仅支持**按主键定位**的查询（单条 + 主键批量 `selectBatchIds` / `@CachedQuery` 返回 `List<实体>` 且参数为 ID 集合）；
  条件查询（按手机号、状态等）**永久不缓存**——匹配集合未知且不可失效，恒回 DB
- 批量缺失 ID 无法逐键 single-flight：并发相同批量请求各回源一次（单条 IN 语句，风暴有界）
- 不做 write-through / write-behind：写库由业务代码完成，本组件只负责失效
- 不接管持久层：与 MyBatis/MP 的关系仅是元数据 + AOP
- 复合主键不支持
- `@CacheInvalidate` 所在方法若为事务方法，删除发生在方法返回时（事务提交前）——延迟双删覆盖绝大多数窗口，严格场景请确保删除在提交后
- binlog 模式下主键列序号来自 `information_schema`，改表结构会自动重新解析（按表缓存的序号在进程生命周期内有效）

## FAQ

**Mapper XML 打包后报 `Invalid bound statement`？**
这是 MyBatis-Plus 路径大小写问题，与本组件无关但高频踩坑：XML 放在 `resources/Mapper/`（大写）时，`classpath*:/mapper/*.xml` 在 IDE 目录模式能碰巧匹配（NTFS 大小写不敏感），打 jar 后必失败。显式配置 `mybatis-plus.mapper-locations: classpath*:/Mapper/*.xml`。

**binlog 连接失败？**
依次检查：MySQL 是否开启 `log_bin` 且 `binlog_format=ROW`；账号是否有 `REPLICATION SLAVE` 权限；`server-id` 是否与 MySQL 及其他副本冲突；MySQL 8.4 需要 connector 0.30.0+。

**多 Redis 实例部署时广播失效？**
广播 topic 活在单个 Redis 上，所有实例必须订阅同一个 Redis。用 Redis Cluster 分片扛 L2 流量时，失效 topic 需收敛在专用节点或使用 Redis 7 sharded pub/sub。

**宿主没有 AOP 自动代理（切面不生效）？**
正常 Spring Boot 应用默认启用。若用 `ApplicationContextRunner` 手动装配测试，记得加 `AopAutoConfiguration`。

## 构建

```bash
mvn test          # 本地无 Redis/带 binlog 的 MySQL(3307) 时对应集成测试自动跳过
mvn verify deploy -Prelease   # 发布（打 v* 标签由 CI 触发）
```

本地起压测依赖容器：

```bash
docker run -d --name cache-kit-mysql -p 3307:3306 \
  -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=cachekit_test \
  mysql:8.4 --log-bin=mysql-bin --binlog-format=ROW --server-id=1
```
