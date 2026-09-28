# 边界、已知限制与 FAQ

## MVP 边界（0.3.0）

- 仅支持**按主键定位**的查询（单条 + 主键批量 `selectBatchIds` / `@CachedQuery` 返回 `List<实体>` 且参数为 ID 集合）；
  条件查询（按手机号、状态等）**永久不缓存**——匹配集合未知且不可失效，恒回 DB
- 不做 write-through / write-behind：写库由业务代码完成，本组件只负责失效
- 不接管持久层：与 MyBatis/MP 的关系仅是元数据 + AOP
- 复合主键不支持
- 写方法处于活动事务时，失效延迟到 **afterCommit** 执行（`cache-kit.tx.evict-after-commit`，默认开），事务回滚不失效。
  注意语义：同事务内先写后读同键会读到缓存里的**提交前旧值**（失效被延迟了），需要同事务立即可见时用 `CacheKit.withDb` 旁路读取
- binlog 模式下主键列序号来自 `information_schema`，**按表缓存于进程生命周期内**——改表结构需重启进程后重新解析
- binlog 与 `CacheKeyCustomizer` 组合存在已知限制：binlog 解析线程无法还原租户键段，租户键的 binlog 失效不会命中
  （仅 TTL/双删兜底），多租户场景建议暂不启用 binlog

## 已知限制与说明

- **binlog 与事务时序**：MySQL 的 binlog 事件在 **COMMIT 时**才写入（事务内只进 binlog cache），监听端天然只见提交后数据，
  不存在"提交前触发失效"的竞态；残余的读回填竞态由双删 + TTL 兜底
- **null 占位与 INSERT**：应用内 `mapper.insert()` 自动清除对应"已确认不存在"占位（新数据立即可见）；
  binlog 模式下 INSERT 行事件同样覆盖；两者皆无时，占位由 `null-ttl`（默认 30s）兜底
- **批量缺失回源**：per-ID single-flight 保证每个缺失 ID 至多回源一次；无法保证多个**不同**批量请求合并为一条更大的 IN
- **主键类型/表名迁移**：旧键成为孤儿由 TTL/L1 上限自然淘汰；如需立即切换，修改 `cache-kit.key-namespace` 整体弃用旧键
- **L1 可插拔（0.3.1+）**：默认 CaffeineChannel；实现 `L1Channel` Bean 可替换（必须为进程内本地缓存——广播失效只删本实例键，
  远端存储实现会破坏广播语义）。L2 通道支持定义自定义 `RedisChannel` Bean 覆盖
- **单飞等待者共享数据**：single-flight 合并回源时，等待方各自拿到独立解码实例（不共享可变对象）；唯"实体不可序列化"的罕见兜底路径会共享实例
- **同前缀实体 fail-fast（0.3.0+）**：两个实体解析出相同缓存前缀（如两个类都标 `@TableName("user")`）会在第二次接入时抛异常——
  同前缀共享键空间会互相覆盖缓存值，字段子集会静默缺字段
- **`@CacheInvalidate` 严格语义**：实体实例参数与实体集合参数（0.3.0+）可直接解析主键；标量参数需参数名与主键字段同名；
  **标量集合（`List<Long>` 等）不收**——无法证明元素是主键，防条件值误当主键（MP 的 deleteByIds 路径由自动切面覆盖，无需注解）
- **`@CachedQuery` 列表查询空结果**：严格守卫下回源结果为空（请求值全部查不到）按误用旁路处理、不缓存——无法证明请求值是主键集合；
  存在的行照常缓存。误判旁路统一用原始参数**全参重查**保证结果完整（并发重叠请求下不会返回他人线程的回源子集），代价是误用路径多一次 DB 查询

## FAQ

**Mapper XML 打包后报 `Invalid bound statement`？**
这是 MyBatis-Plus 路径大小写问题，与本组件无关但高频踩坑：XML 放在 `resources/Mapper/`（大写）时，
`classpath*:/mapper/*.xml` 在 IDE 目录模式能碰巧匹配（NTFS 大小写不敏感），打 jar 后必失败。
显式配置 `mybatis-plus.mapper-locations: classpath*:/Mapper/*.xml`。

**binlog 连接失败？**
依次检查：MySQL 是否开启 `log_bin` 且 `binlog_format=ROW`；`binlog_row_image` 是否为 `FULL`（MINIMAL 时行镜像缺主键列，
按行失效会静默丢失）；账号是否有 `REPLICATION SLAVE` 权限；`server-id` 是否与 MySQL 及其他副本冲突；
MySQL 8.4 需要 connector 0.30.0+。

**多 Redis 实例部署时广播失效？**
广播 topic 活在单个 Redis 上，所有实例必须订阅同一个 Redis。用 Redis Cluster 分片扛 L2 流量时，
失效 topic 需收敛在专用节点或使用 Redis 7 sharded pub/sub。

**宿主没有 AOP 自动代理（切面不生效）？**
正常 Spring Boot 应用默认启用。若用 `ApplicationContextRunner` 手动装配测试，记得加 `AopAutoConfiguration`。
