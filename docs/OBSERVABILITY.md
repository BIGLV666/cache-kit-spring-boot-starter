# 可观测性（Micrometer，自动装配）

宿主类路径有 micrometer-core（spring-boot-starter-actuator 自带）时自动注册 `cache-kit.*` 计数器，
无需任何配置；也可自行实现 `CacheMetricsListener` Bean 接管。

## 指标一览

| 指标 | 含义 |
|---|---|
| `cache-kit.l1.requests{result=hit\|miss}` | L1 命中率 |
| `cache-kit.l2.requests{result=hit\|miss}` | L2 命中率 |
| `cache-kit.db.loads` | DB 回源（批量按 ID 数计） |
| `cache-kit.null.placeholders` | null 占位写入（穿透防护触发） |
| `cache-kit.evict.keys` | 失效键数（含双删第二次） |
| `cache-kit.broadcast.sent` / `received{applied=true\|false}` | 广播收发——**sent 持续大于 received 说明存在广播丢失**（L1 TTL 兜底） |
| `cache-kit.l2.fallbacks{op}` | L2 降级次数（op=get/put/multiGet/evictAll），持续增长说明 Redis 不健康 |
| `cache-kit.evict.retries.exhausted` | L2 删除重试耗尽——**失效丢失**（旧值滞留 L2 至 TTL），建议告警 |
| `cache-kit.doubledelete.skipped` | 双删积压跳过（脏数据由 TTL 上界兜底），持续增长说明写入压力超调度能力 |
| `cache-kit.binlog.position.resets` | binlog 位点重置——**断连窗口内失效丢失**（位点被服务端清理时触发），建议告警 |
| `cache-kit.l1.hit.rate` / `cache-kit.l2.hit.rate` | 命中率 Gauge（hit/(hit+miss)，由计数器实时计算） |
| `cache-kit.invalidation.delay`（Timer，p50/p99） | binlog 行事件 MySQL 时间戳 → 本实例失效应用的传播延迟；**含两侧时钟偏差，趋势观测用** |
| `cache-kit.broadcast.streams.lag.seconds`（Gauge） | streams 模式本实例消费组滞后（最新失效事件 - 本组已读事件的时间戳差）；持续 >0 说明消费方处理不过来或断连 |
| `cache-kit.l2.circuit.opened` | L2 熔断器打开次数（0.3.2+）；频繁增长说明 Redis 持续抖动，应检查网络与服务端 |
| `cache-kit.l2.circuit.state`（Gauge） | L2 熔断器当前状态：0=CLOSED 1=HALF_OPEN 2=OPEN（0.3.2+） |
| `cache-kit.binlog.derive.skipped` | binlog 键段还原跳过的行数（0.3.2+，自定义段无法从行数据还原时）；持续增长说明自定义器应实现 `segmentFor` |
| `cache-kit.l1.refreshahead.triggered` | L1 预刷新提交数（0.3.3+，`l1.refresh-ahead` 开启时）；速率 ≈ 热点键数 × 1/TTL |
| `cache-kit.l1.refreshahead.dropped` | 预刷新任务因队列满被丢弃（0.3.3+）；下次读会重新触发，仅是刷新延迟，持续增长说明预热线程不够 |
| `cache-kit.l1.refreshahead.failed` | 预刷新任务执行失败（0.3.3+）；键保持原值到 TTL 自然过期，不影响正确性 |
| `cache-kit.l2.value.oversized` | 超过 `l2.max-value-kb` 被两级跳写的值数（0.3.3+）；持续增长说明实体含大字段，读每次回源 DB |
| `cache-kit.l2.value.compressed` | L2 值压缩写入数（0.3.3+，`l2.compression-enabled` 开启时） |

## 告警建议

`evict.retries.exhausted`、`doubledelete.skipped`、`binlog.position.resets` 三者对应真实的数据丢失/陈旧窗口，
持续增长即应触发排查；`l2.fallbacks{op}` 是 Redis 健康度的直接信号；`l2.circuit.state` 频繁离开 0
说明 Redis 持续抖动；`binlog.derive.skipped` 持续增长说明自定义键段无法从行数据还原；
`l2.value.oversized` 持续增长说明有大字段实体频繁绕过缓存（读每次回源 DB），应收缩实体或调大上限。

## 运维端点（/actuator/cachekit，0.3.2+）

宿主引入 spring-boot-actuator 后自动注册只读端点 `/actuator/cachekit`
（暴露范围由宿主 `management.endpoints.web.exposure.include` 管理）：

- **l1**：条目估计值、TTL、容量上限；
- **l2**：TTL、null 占位 TTL、熔断器开关与状态（CLOSED/HALF_OPEN/OPEN/DISABLED）；
- **doubleDelete**：当前积压任务数 / 上限（1 万）；
- **streams**（streams 模式）：消费组名、最近量测的滞后秒数（-1 = 尚未量测）；
- **entities**：按实体前缀合并的注册元数据（TTL、实体类型）与累计统计——
  L1/L2 命中数与命中率、DB 回源、null 占位、失效计数（命中率 = hit/(hit+miss)，
  -1 表示无请求）。

安全边界：只输出前缀、计数与状态，**不含任何缓存键值**；不含 binlog 凭据等敏感配置。

## Grafana 面板

面板模板见 [`grafana-dashboard.json`](grafana-dashboard.json)（导入后选择 Prometheus 数据源即可，
命中率、请求/回源速率、失效丢失告警、L2 降级、失效传播延迟 7 组面板开箱即用）。
