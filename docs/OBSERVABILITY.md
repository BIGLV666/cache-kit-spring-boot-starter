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

## 告警建议

`evict.retries.exhausted`、`doubledelete.skipped`、`binlog.position.resets` 三者对应真实的数据丢失/陈旧窗口，
持续增长即应触发排查；`l2.fallbacks{op}` 是 Redis 健康度的直接信号。

## Grafana 面板

面板模板见 [`grafana-dashboard.json`](grafana-dashboard.json)（导入后选择 Prometheus 数据源即可，
命中率、请求/回源速率、失效丢失告警、L2 降级、失效传播延迟 7 组面板开箱即用）。
