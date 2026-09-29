# 三级缓存的两个失效盲区，我用 binlog 和消费组广播补上了

> 组件地址：<https://github.com/BIGLV666/cache-kit-spring-boot-starter>（已发布 Maven Central：io.github.biglv666:cache-kit-spring-boot-starter:0.3.1）
> 5 分钟可跑的最小示例：<https://github.com/BIGLV666/cache-kit-sample>

Cache-Aside（旁路缓存）是读多写少场景的主流方案：读走缓存，写先落库再删缓存。它简单好用，但有两个盲区在实际生产里反复出现：

1. **绕过应用的写**：DBA 直接改库、另一个服务写同一张表——应用层的"写后删缓存"压根没触发，脏数据能活到 TTL 到期。
2. **丢消息**：多实例部署时，本地缓存（L1）的一致性靠失效广播，而 Redis pub/sub 是 fire-and-forget——订阅方短暂掉线、网络抖动，消息就丢了。

cache-kit 是一个三级缓存组件（Caffeine L1 → Redis L2 → DB read-through，对 MyBatis-Plus 零注解接入）。这篇文章讲它怎么补这两个盲区，以及和 Spring Cache / JetCache 的压测对比里一次有意思的根因排查。

## 盲区一：绕过应用的写——binlog 直连失效（免 Canal）

业界标准答案是 Canal：伪装成 MySQL 从库消费 binlog，转发到 MQ，应用消费 MQ 做失效。方案成熟，但**运维成本不小**——多一套 Canal Server 集群要部署、监控、扩容。

cache-kit 的做法是**直接内嵌 binlog 消费**：以 MySQL replica 协议直连（基于 mysql-binlog-connector-java），任何来源对已缓存实体表的写入，按行提取主键触发失效。不引入 Canal，启用只需要一个依赖加一个开关：

```yaml
cache-kit:
  binlog:
    enabled: true
    server-id: 9001   # 同一 MySQL 上必须唯一
```

直连意味着**断线重连要自己处理好**，这里有两个真实世界的问题，文档里都如实披露了语义：

- **断连窗口**：connector 原生支持位点续传，断连期间的事件会回放（失效是幂等 DEL，重复无害）。
- **位点文件被清理**：长断连期间 binlog 过期或 `PURGE BINARY LOGS` 后，重连会被 MySQL 秒断、无限循环。cache-kit 检测到"连上即秒断且无事件"连续 5 轮后，自动重置为最新位点继续监听并告警（该窗口的丢失由 L2 TTL 兜底）。这段逻辑用 Testcontainers 真起 mysql:8.4 做过端到端验证：KILL dump 连接 + PURGE 位点文件后，失效监听能自动恢复。

实测端到端传播延迟（直写 → 失效 → 下次读到新值）：**avg 10.3ms / p99 14.2ms / max 53.7ms**（300 次直写零漏失效）。

## 盲区二：丢消息——从 pub/sub 到 Streams 消费组

多实例下，A 实例写了数据删缓存，B 实例的 L1 怎么清？pub/sub 广播的语义是 fire-and-forget：B 实例重启、GC 停顿、网络抖动期间的消息全部丢失，只能靠 L1 的短 TTL 兜底。

cache-kit 0.3.1 提供了第二条通道（`cache-kit.broadcast.mode=streams`），把失效消息写进 Redis Stream，**每个实例一个独立消费组全量消费**，带 ACK 语义：

- 实例短暂掉线不丢失效——掉线期间的条目留在组内待读，恢复后补投；
- 新组从 `$`（最新）起读——实例启动时 L1 为空，不需要历史消息；
- 优雅停机自毁消费组，崩溃残留的组由存活实例周期清理（纪元超 30 分钟且无活跃消费者才删，健康消费者闲置至多 5 秒，不会误杀）；
- `XADD MAXLEN ~` 近似裁剪防无限增长，代价是实例离线过久（落后超 maxlen）时退化为 pub/sub 语义。

多实例端到端实测（Testcontainers）：**streams 3 实例 × 500 键 157ms 零丢失；pub/sub 2 实例 × 200 键 202ms 全送达**。单键失效传播 p50 约 8ms、streams 的尾延迟（p99 ~22ms）比 pub/sub（p99 ~45ms）更稳。

选型建议没变：单实例或可容忍 TTL 兜底用默认 pub/sub（Redis 侧零开销）；多实例且对"重启窗口丢失效"敏感才上 streams。

## 压测对比：cache-kit vs Spring Cache vs JetCache

既然都要做对比，就按真实使用形态做（不是玩具 benchmark）：Testcontainers 真起 redis:7，各实现取生产典型配置（Spring Cache = RedisCacheManager + Lettuce 池 8 + JSON 序列化；JetCache = RedisLettuceCache，多级档为 Caffeine+Redis），线程数从半核扫到四倍核取最优档。工作负载：2 字段实体（JSON ~30B）、1000 键轮转全部容于 L1——**这是纯命中物理上限，生产吞吐由命中率支配**（工作集超出 L1 时跌回远程量级，见下表场景 1b）。

| 场景 | cache-kit | JetCache | Spring Cache |
|---|---|---|---|
| 并发读吞吐上限 | **~1450 万 ops/s**（p99 ~1µs） | ~690 万（多级，p99 ~190µs） | ~2.8 万（仅 Redis，p99 ~4.8ms） |
| 单冷键击穿（200 并发） | **DB 回源 1 次** | 默认 200 次；开 penetrationProtect 后 1 次 | 200 次（默认无互斥） |
| 冷启动回源放大（64 并发 × 100 键） | **100 次**（每键 1 次） | 默认 ~6400 次；protect 后 100 次 | ~6300 次 |
| 场景 1b：L1 装不下时吞吐 | 均匀 6.8% 命中 → 2.7 万；Zipf 30% 命中 → 3.6 万 | —— | —— |

三层结论：有无本地层差 ~550 倍；防击穿是否默认开启决定冷启动时 DB 承压（1× vs 63×）；命中率才是生产容量的第一变量。

### 一次有意思的根因排查：为什么 JetCache 多级缓存高并发下尾延迟劣化？

压测里发现一个反直觉现象：JetCache 本地层存的是对象引用（cache-kit 存 JSON 串、每次命中还要 Jackson 反序列化出新实例），**单次命中明明 JetCache 更快**（单线程均值实测 ~166ns vs ~494ns），但 16 线程以上它的聚合吞吐先到顶、p99 从 35µs 一路恶化到 427µs，而 cache-kit 反序列化做得更多，p99 反而稳在 1µs。

最后定位到实现细节：**JetCache 的 Caffeine 本地层为了支持 `expireAfterAccess` 语义用了自定义 `Expiry`——Caffeine 对这种策略每次读都会回调 `expireAfterRead`**，内部调 `System.currentTimeMillis()` 重算剩余时间，返回值随时间连续漂移，导致 Caffeine 对热点条目频繁重排定时器（共享写）。热键被几十个线程打，这些条目就在 CPU 核心之间来回弹。

隔离实验验证（16 线程裸 Caffeine，唯一变量是过期策略）：变量 Expiry 比固定 `expireAfterWrite` 吞吐低 ~25%，max 尾部出现 300ms 级尖刺。cache-kit 用固定 TTL 纯读路径，每次命中分配还更多（880 B/op vs 306 B/op），p99 依然稳定——说明这个量级上**每读回调的共享写才是尾延迟主导因素，分配/GC 不是**。

（单次命中 JetCache 更快这一点同样如实记录：低并发低 QPS 下它更优；两者的差异在亚微秒档，业务不可感知，不构成选型依据。）

## 上手

```xml
<dependency>
    <groupId>io.github.biglv666</groupId>
    <artifactId>cache-kit-spring-boot-starter</artifactId>
    <version>0.3.1</version>
</dependency>
```

```yaml
cache-kit:
  l1:
    ttl: 30s
  l2:
    ttl: 10m
  # binlog:
  #   enabled: true      # 覆盖"绕过应用的写"
```

MP 项目实体保持 `@TableName` / `@TableId` 即完成接入；非 MP 项目用 `@CacheEntity` / `@CacheId` + `@CachedQuery`。最小可跑示例见 [cache-kit-sample](https://github.com/BIGLV666/cache-kit-sample)（docker compose 一键起 Redis + 带 binlog 的 MySQL，五条 curl 验证全部效果）。

## 写在最后

组件的一致性语义是**最终一致**——常态秒级（双删窗口内），最坏受 L2 TTL 上界约束。所有丢消息窗口、TTL 兜底边界、回源放大效应都在文档里如实标注，没有对一致性敏感（库存、余额）的字段请走 `CacheKit.withDb` 旁路或直接不进缓存。

压测全部可复现：`mvn test -Dtest=CacheStressComparisonIntegrationTest`（仓库自带）。欢迎试用与反馈。
