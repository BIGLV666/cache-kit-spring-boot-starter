# 一致性机制与性能实测

## 强一致读（豁免）

缓存是最终一致的。需要强一致的场景按档位选用：

| 档位 | 用法 | 适用 |
|---|---|---|
| 自定义方法不加注解 | `selectByIdFromDb(Long id)` | 已知某方法永远要 DB |
| ThreadLocal 旁路 | `CacheKit.withDb(() -> mapper.selectById(id))` | 调用点临时决定 |
| SpEL 条件 | `@CachedQuery(condition = "!forceDb")` | 由调用方传参决定 |

**库存扣减、余额等强一致判断所依赖的字段不要进缓存**——任何缓存方案（双删、binlog 均不例外）都是最终一致。

## 一致性机制

写路径固定**先写 DB 后删缓存**（Cache-Aside 标准序）。删除动作 = **DEL L2 → 本地 L1 → 广播 → 延迟双删**。
顺序关键：必须先删 L2——若先删 L1，间隙内并发读会 L1 miss 后从 L2 读到旧值并回填 L1，制造可复现脏数据。

脏数据的核心窗口是**回填竞态**（读线程查到旧值、写线程删除后、读线程把旧值回填进 L1/L2）。
延迟双删压缩该窗口；窗口之外（慢读/长 GC 跨过双删间隔、双删积压丢弃、Redis 宕机期间失效丢失）
旧值可能复活在 L2 并持续回填各节点 L1，此类场景的真实上界是 **L2 TTL（默认 10m + 抖动）**，
而非秒级——这是 Cache-Aside 的固有竞态，本组件未用版本号/CAS 消除它，依赖 TTL 上界兜底。
对一致性敏感的字段请用 `CacheKit.withDb` 旁路或直接不进缓存。

## 失效广播通道选择（0.3.1+）

| | `pubsub`（默认） | `streams` |
|---|---|---|
| 投递语义 | fire-and-forget：订阅方掉线/网络抖动即丢失，靠 L1 TTL 兜底 | 消费组 ACK：实例短暂掉线不丢失效，恢复后补投 |
| Redis 侧开销 | 零存储 | 一份 Stream 数据（`MAXLEN ~ streams-maxlen` 近似裁剪，默认 10000） |
| 每实例状态 | 无 | 一个独立消费组（优雅停机自毁；崩溃残留由存活实例周期清理） |
| 送达延迟实测 | 2 实例 × 200 键 202ms 全送达 | 3 实例 × 500 键 157ms 零丢失 |

单实例（无 L1 一致性需求）或可容忍秒级 TTL 兜底时用默认 pubsub；多实例且对"重启窗口丢失效"敏感时用 streams。

## 压测对比：cache-kit vs Spring Cache vs JetCache（0.3.1+）

`CacheStressComparisonIntegrationTest`（Testcontainers redis:7，16 逻辑核机器）。
对比对象均取各自生产典型配置：Spring Cache = RedisCacheManager + Lettuce 连接池（8）+ JSON 序列化；
JetCache = RedisLettuceCache（多级档为 Caffeine+Redis）。数值随环境波动，**量级与相对关系**才是重点。

术语：**op = 一次读缓存调用**（cache-kit `load` / Spring Cache `get` / JetCache `get`），
ops/s 为每秒完成的读调用次数（多线程聚合值）。

### 场景 1：并发读吞吐上限（线程数从 cores/2 扫到 4×cores，每档 0.3s 预热 + 2s 计时，取最优档）

| 实现 | 吞吐上限 | 最优并发 | p99 @最优 |
|---|---|---|---|
| cache-kit 三级（L1 命中） | **~1485 万 ops/s** | 32 线程 | ~1µs |
| JetCache 多级（Caffeine+Redis） | ~683 万 ops/s | 32 线程 | ~177µs |
| Spring Cache（仅 Redis，池 8） | ~2.5 万 ops/s | 32 线程 | ~2.8ms |
| JetCache 仅远程 | ~2.8 万 ops/s | 64 线程 | ~4.9ms |
| cache-kit 仅 L2（单连接） | ~2.7 万 ops/s | 64 线程 | ~5.3ms |
| 裸 Redis GET（池 8） | ~2.8 万 ops/s | 64 线程 | ~4.8ms |

有本地层的实现上限高出 **~550 倍**；仅远程各实现同为"Redis 往返 + 连接池"瓶颈，彼此相近——
三级架构的价值在于**本地层挡掉绝大部分往返**，而不是远程读更快。

**归因说明（别过度解读上一行）**：cache-kit 的 L1 存 JSON 串、每次命中都反序列化出**独立实例**
（防共享可变对象被业务污染），单次命中成本其实**更高**。单线程逐段实测
（`CacheHitPathAttributionTest`，中位数）：纯 Caffeine get ~100ns、Jackson 反序列化 ~200ns、
cache-kit load 命中全路径 ~300ns（mean 430ns）、JetCache 本地 get ~100ns（mean 149ns）。
多线程聚合吞吐 cache-kit 反超且扩展更平（8→32 线程 981 万→1486 万，64 线程开始回落；
JetCache 多级 8 线程即到顶 645 万，线程加倍吞吐不动、p99 从 34µs 恶化到 396µs）——
这是并发扩展性差异，不是单次读更快。本地命中层两者同属亚微秒~微秒档，
**该行差异不构成选型依据**；真正的分野是有无本地层（~550 倍）与默认防击穿（场景 2）。

**尾延迟劣化的机制（为什么 JetCache 多级 p99 随并发恶化）**：
1. 普遍规律：并发升高使资源利用率逼近饱和，等待时间非线性放大——所有实现都有，只是幅度不同。
2. JetCache 本地缓存为支持 `expireAfterAccess` 语义，用了 Caffeine 自定义 `Expiry`——Caffeine
   **每次读都回调 `expireAfterRead`**（内部 `System.currentTimeMillis()` + 随时间连续变化的剩余时间重算），
   返回值漂移导致 Caffeine 对热点条目频繁重排定时器（共享写）。隔离实验
   （`CacheExpiryTailTest`，16 线程裸 Caffeine 对比）：变量 Expiry 比固定 expireAfterWrite
   吞吐低 ~25%，max 尾部出现 300ms 级尖刺。
3. 叠加多级包装的每次读分配（CacheGetResult/两级迭代/holder 检查），高并发下争用放大。
4. cache-kit 反例佐证主导因素：同样 Caffeine、同样 1000 键、同样线程数，用固定
   `expireAfterWrite` 的纯读路径 p99 稳定 ~1µs——它甚至分配更多（每次命中 Jackson 反序列化），
   说明分配/GC 不是尾延迟的主导，**每读回调 + 条目重排的共享写才是**。

### 场景 2：缓存击穿（200 并发打同一冷键，模拟 50ms DB）

| 实现 | DB 回源次数 | 总耗时 |
|---|---|---|
| cache-kit（single-flight 内置） | **1** | ~70ms |
| Spring Cache（默认，无互斥） | 200 | ~175ms |
| JetCache（默认） | 200 | ~143ms |
| JetCache（penetrationProtect=true） | **1** | ~66ms |

cache-kit 的 single-flight 与 null 占位是**默认开启**的；JetCache 需显式开启 penetrationProtect，
Spring Cache 需自行加锁或换 locking writer，否则击穿时回源次数 = 并发数。

### 场景 2b：冷启动回源放大（64 并发 × 100 冷键，每线程顺序请求全部键，模拟 20ms DB）

| 实现 | DB 总回源次数 | 相对倍数 | 总耗时 |
|---|---|---|---|
| cache-kit | **100**（每键 1 次） | 1× | ~3.1s |
| JetCache（penetrationProtect=true） | **100** | 1× | ~2.8s |
| Spring Cache（默认） | 6,287 | ~63× | ~3.0s |
| JetCache（默认） | 6,400 | ~64× | ~3.0s |

应用重启/缓存清空后首批流量是回源峰值：无单飞语义的实现，DB 承压 ≈ 并发 × 键数；
single-flight 把每键收敛为 1 次回源，DB 承压 ≈ 键数。

### 场景 3：单键失效传播（A 实例 evict → B 实例 L1 清除，100 轮）

| 通道 | p50 | p99 | max |
|---|---|---|---|
| cache-kit pubsub | ~8.2ms | ~45ms | ~45ms |
| cache-kit streams | ~8.8ms | ~22ms | ~22ms |

跨实例失效在**几十毫秒级**完成；JetCache 的跨实例本地缓存失效依赖其 CacheManager 广播装配
（本压测未含），Spring Cache 无本地层、天然无此问题但每次读都付 Redis 往返（见场景 1）。

复现：`mvn test -Dtest=CacheStressComparisonIntegrationTest`（另有单线程微基准
`CacheBenchmarkIntegrationTest` 可作量级参考）。

## 生产实测（PaperWise 宿主项目）

单机 4~10 实例 + Docker Redis/MySQL，Java 17：

| 场景 | 实测 |
|---|---|
| 缓存命中读 | 微秒级；20,249 个 HTTP 请求期间 MySQL InnoDB 行读取零增长 |
| 应用内写失效 | 每写增加约 1~2ms（DEL L2 + 广播 + 双删调度）；4.6 万次广播零丢失 |
| 广播最终一致性 | 10 实例、716 写/s 并发、4.6 万次广播：全实例最终版本校验 100% 通过 |
| 脏读窗口（应用内写） | 最大 119ms~505ms（含单 JVM 调度噪声），远小于 L1 TTL 30s 设计上界 |
| 脏读窗口（binlog 直写） | p99 14.2ms，300 次直写零漏失效 |
| 外部 HTTP 全链路 | 单实例饱和 ~1700 req/s；延迟基线由鉴权 Redis 往返主导，缓存命中本身微秒级 |
| 回源放大 | 热键上每次写约 3 次 DB 回源（广播删 + 双删删 + 在途回读），写极热键需评估 |
| 跨实例击穿放大（0.3.0 量化） | single-flight 仅单 JVM 生效：N 实例同时击穿同一未热键 = N 次 DB 回源（每实例 1 次）；
  冷启动场景 N×键数 即回源峰值，DB 按 `N × 每秒新增键数` 估算容量；若需全局互斥可外挂分布式锁，组件刻意未内置（锁的开销与死锁面 > 冷启动回源放大） |
