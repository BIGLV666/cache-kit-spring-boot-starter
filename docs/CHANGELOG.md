# 变更记录

## 0.3.3（未发布）

- **新增**：L1 预刷新（`cache-kit.l1.refresh-ahead`，默认关）——L1 命中且剩余 TTL 低于窗口时返回当前值
  并异步刷新（走 L2/DB 回填），把"过期后首次读的回源延迟"提前消化。刻意不做"过期后供旧值"的经典 SWR：
  读永远拿未过期值，"L1 TTL = 脏读上界"的一致性承诺不变；null 占位同样被预刷新（到期前重探 DB）。
  刷新任务经 single-flight 与并发读合并回源，压测实证 32 线程 × 2s 读热点键仅 1 次回源；
  失败吞掉（键照常 TTL 过期）、队列满丢弃下次读重触发；窗口 ≥ l1.ttl 启动告警并禁用。
  指标 `cache-kit.l1.refreshahead.triggered/dropped/failed`
- **新增**：L2 单值大小上限（`cache-kit.l2.max-value-kb`，默认 512KB，0 关闭）——超过的值两级都不写缓存
  （与"序列化失败不缓存"同语义），防少量大字段撑爆 Redis 内存与网络；
  指标 `cache-kit.l2.value.oversized` + 限频告警。**默认开启是行为变更**：默认配置下超过 512KB 的值
  将不再进缓存（读每次回源 DB）
- **新增**：L2 值压缩（`cache-kit.l2.compression-enabled`，默认关；阈值 `compression-min-kb` 默认 32KB）——
  gzip + Base64 + `"gz:"` 前缀存 L2，省 Redis 内存与网络传输；L1 永远存原文，解压在 L2 读出口统一完成，
  解压失败按未命中兜底；关闭后存量压缩值仍可读。压测：32KB JSON 编解码 ~150µs/op
- **新增**：复合主键支持（0.3.3+，仅注解路径）——多个 `@CacheId` 按声明顺序 join ':' 组成键段；
  `@CachedQuery` 严格推导（每个主键字段都要有同名标量参数，缺一即旁路）、实体实例参数、
  `EntityCache` 手动句柄（`get(List.of(v1, v2))`）与 binlog 失效按联合键处理；段值含 ':' 读路径拒绝、
  失效路径跳过（键段歧义防错位）；binlog 侧全部 PRI 列按"字段名↔列名"（驼峰↔蛇形）映射对齐字段声明序，
  映射失败该行跳过。MP 不支持复合主键，BaseMapper 自动切面对复合主键实体整体旁路（LIMITATIONS 已改写）
- **文档**：CONFIG/OBSERVABILITY/RESILIENCE/LIMITATIONS 增补上述特性；topic 文档英文化
  （新增 CONFIG_EN / OBSERVABILITY_EN / RESILIENCE_EN / LIMITATIONS_EN / BOOT4-NATIVE_EN）
- **工程**：L1Channel SPI 新增 default 方法 `remainingTtlNanos(key)`（返回 -1 的实现自动禁用预刷新，向后兼容）；
  Grafana 面板模板新增预刷新与 L2 值策略面板

## 0.3.2（未发布）

- **新增**：sharded pub/sub 广播模式（`cache-kit.broadcast.mode=sharded-pubsub`）——
  Redis 7.0+ 的 SSUBSCRIBE/SPUBLISH 分片广播，Cluster 下失效消息只达 topic 所属分片节点
  （替代全节点广播，连接与广播开销 O(节点数) → O(1)）；spring-data-redis 未封装该命令，
  实现走 Lettuce 原生 API（新增 optional 依赖 `io.lettuce:lettuce-core`），仅支持 Lettuce 客户端
  （Boot 默认），非 Lettuce 或订阅失败（Redis<7 等）自动回退 pub/sub 并告警；每 30s 幂等重订阅
  覆盖重连窗口。Cluster 模式建议按发布流程跑冒烟验证
- **新增**：L2 熔断器（`cache-kit.l2.circuit-breaker.*`，默认开）——连续失败 20 次短路 10s，
  故障期间 L2 调用零开销降级（不再逐次阻塞到命令超时），到期单探测恢复；指标
  `cache-kit.l2.circuit.opened` + 状态 gauge `cache-kit.l2.circuit.state`
- **新增**：`/actuator/cachekit` 运维端点（spring-boot-actuator 在类路径时注册）——实体级
  L1/L2 命中率与计数、DB 回源、null 占位、L1 条数估计、双删积压、streams 滞后、熔断状态；
  只读，不含缓存键值。新增实体维度统计收集器（与全局 Micrometer 指标并行，`CacheStatsCollector`）
- **验证**：混沌测试套件（`src/test/java/.../chaos/`，按子包隔离）——真实停机/网络挂起
  （toxiproxy）/恶意输入/binlog 表结构漂移四类故障注入，实证降级与 TTL 兜底承诺。
  实测发现（已写入 RESILIENCE.md）：Lettuce 默认断连缓冲会把"失败"的失效 DEL 在重连后
  补投成功，失效丢失窗口经常优于文档承诺的最坏情况
- **修复/增强**：多租户键的 binlog 失效不命中——`CacheKeyCustomizer` 新增
  `segmentFor(meta, rowData)` SPI，覆写后 binlog 从行数据（如 tenant_id 列）还原键段、
  按含段精确键失效（同时保留无段键失效兜底）；未实现 SPI 的自定义段跳过并计
  `cache-kit.binlog.derive.skipped`（LIMITATIONS 相应改写）

- **新增**：binlog GTID 位点模式（`cache-kit.binlog.gtid-enabled`）——GTID 集替代 file/position，
  主从切换后位点仍连续；connector 内置 `gtidSetFallbackToPurged`（扫 PURGE 后自动回退）。
  未显式配置 gtid-set 时启动查询 `@@global.gtid_executed` 为起点（connector 默认从最早事件回放
  会造成启动失效风暴）；宿主无数据源或 MySQL 未开 GTID 时 fail-fast
- **新增**：streams 消费组滞后 gauge（`cache-kit.broadcast.streams.lag.seconds`）——
  本组已读事件与 Stream 最新事件的时间戳差，0 表示已追平；多实例测试断言"全部送达后回落为 0"
- **修复**：`@AutoConfigureAfter` 的 Boot 4 Redis autoconfigure FQN 写错——Boot 4 的类名是
  `DataRedisAutoConfiguration`（包名与类名都改了），修正后 Boot 4 下装配顺序条件真实生效
- **验证**：新增 CI `boot4-compat` job——Spring Boot 4.0.0 依赖树下跑全部非容器测试（111 例全绿）；
  测试套件经 `BootAutoconfigCompat` 反射双 FQN 探测，同一测试源码 Boot 3.5/4 双跑
- **工程**：testcontainers 显式声明版本（Boot 4 parent 不再管理）

## 0.3.1（2026-09-29）

- **修复**：`cache-kit.l2.ttl` 配置非正值（文档承诺的"禁用 L2 写入"）会被 L1/L2 TTL 倒装校验拦截导致启动失败——非正值时跳过倒装校验，仅告警
- **修复**：`cache-kit.binlog.enabled=true` 但类路径缺 mysql-binlog-connector 时装配静默跳过（以为开了 binlog 失效其实没开）——改为直接拒绝启动并提示补依赖
- **修复**：`@CachedQuery` 列表查询误判旁路在并发重叠请求下可能返回他人线程的回源子集（静默缺数据）——统一改为原始参数全参重查；误判时共享 in-flight future 改为按真实数据正常完成，非 strict 路径（MP `selectBatchIds`）的并发等待者不再收到 `IdMisfireException`
- **增强**：binlog 断线重连位点被服务端清理（binlog 过期/PURGE）时会无限快速重连直至重启——检测到"连上即秒断且无事件"连续 5 轮后自动回退最新位点并告警（断点续传本身由 connector 原生支持）
- **增强**：L2 装配时检查 Redis 命令超时（>5s 告警），避免 Redis 抖动时业务读线程被阻塞到超时才降级
- **增强**：失效丢失/降级可观测——新增 `cache-kit.l2.fallbacks{op}`、`cache-kit.evict.retries.exhausted`、
  `cache-kit.doubledelete.skipped`、`cache-kit.binlog.position.resets` 计数器（重试耗尽与位点重置建议配置告警）
- **增强**：可观测性深化——`cache-kit.l1.hit.rate` / `cache-kit.l2.hit.rate` 命中率 Gauge、
  `cache-kit.invalidation.delay` 失效传播延迟 Timer（p50/p99，含时钟偏差，趋势观测用）、
  Grafana 面板模板（`docs/grafana-dashboard.json`）
- **测试**：Testcontainers 端到端覆盖 binlog 断线重连回放与位点被 PURGE 后自动重置恢复失效
- **新增**：Redis Streams 消费组失效通道（`cache-kit.broadcast.mode=streams`）——消费组 ACK，
  实例短暂掉线不丢失效、恢复后补投；多实例端到端实测 3 实例 × 500 键 157ms 零丢失
- **新增**：`@CacheWarmup` 启动预热（`order` 排序、`cache-kit.warmup.parallelism` 并发度）；
  `EntityCache` 批量接口 `getBatch`（single-flight 合并回源）/ `evictBatch`
- **新增**：`L1Channel` SPI——自定义 Bean 替换默认 Caffeine 本地缓存
- **新增**：Spring Boot 4 前向兼容（`DataSourceProperties` 双 FQN 反射解析）与
  GraalVM native-image RuntimeHints；读路径对比 benchmark（cache-kit vs 裸 Redis vs Spring Cache vs JetCache）

## 0.3.0

- **修复**：`IService.saveBatch` / `updateBatchById` / `saveOrUpdateBatch` 绕过 mapper 代理导致批量更新后缓存脏到 L2 TTL——新增独立 service 切面覆盖
- **修复**：同前缀双实体互相覆盖缓存值且静默缺字段——注册表 fail-fast
- **修复**：`@CachedQuery` 列表查询在"请求值全部查不到"时误写错键 null 占位——严格守卫扩展到空结果
- **增强**：`@CacheInvalidate` 支持实体集合参数（批量写一个注解整体失效）
- **增强**：L2 删除失败（Redis 闪断）自动重试（复用双删调度器，上限 3 次），降低失效丢失窗口
- **增强**：批量失效键去重 + Redis 单命令多键 DEL + 管道化 PUBLISH（binlog 大事务命令数从 O(2N) 降为 O(1) 往返）
- **增强**：`@CachedQuery` 列表空结果旁路、binlog 主键列查询失败 30s 冷却、`peek()` 指标埋点、双删调度器双线程
- **文档**：L1 内存上界算式（`max-weight-kb × 2KB`）、跨实例击穿回源放大量化、TTL 非正值语义澄清（= 跳过写入，非"永不过期"）、`evict-after-commit=false` 的长事务风险
