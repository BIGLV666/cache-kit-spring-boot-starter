# 变更记录

## 0.3.1（未发布）

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
