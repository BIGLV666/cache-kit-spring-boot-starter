# 配置全参考（前缀 `cache-kit.*`）

```yaml
cache-kit:
  enabled: true            # 总开关
  key-namespace:           # 键命名空间：未配置时自动取 spring.application.name（多服务共享 Redis 必要隔离）
  require-key-namespace: false  # true 时 key-namespace 与 spring.application.name 均为空 → 启动失败（键冲突零容忍场景）
  l1:                      # 本地缓存（默认 Caffeine；0.3.1+ 可自定义 L1Channel Bean 替换）
    max-entries: 65536     # 最大条目数（条目数上限，不限制单条大小）
    max-weight-kb: 0       # 权重上限（K 字符）：>0 时取代 max-entries，防大实体撑爆堆内存。
                           # 实际内存上限 ≈ max-weight-kb × 2KB（BMP 字符 UTF-16 双字节，中文场景）；
                           # 建议生产环境启用，例如 65536 ≈ 128MB 上限。单条 JSON 按长度/1024 计权重，最小 1
    ttl: 30s               # L1 TTL：必须显著小于 l2.ttl，是广播丢消息时的脏读上界
    refresh-ahead: 0s      # L1 预刷新窗口（0.3.3+，默认关）：L1 命中且剩余 TTL 低于该值时，
                           # 返回当前值 + 异步刷新（走 L2/DB 回填），把"过期后首次读的回源延迟"
                           # 提前消化。读永远拿未过期值，"L1 TTL = 脏读上界"承诺不变；
                           # null 占位同样会被预刷新（到期前重探 DB）。必须 < l1.ttl，
                           # 否则启动告警并禁用；仅作用于单条读，批量查询不触发
  l2:                      # 远程缓存（Redis，类路径有 spring-data-redis 且存在 RedisConnectionFactory 时启用；
                           # 只有类没有工厂 Bean 时自动降级为 L1-only，不影响启动）
    ttl: 10m               # L2 TTL 基准；非正值（0/负数）= 跳过 L2 写入（等效禁用 Redis 缓存，启动打警告），
                           # 不是"永不过期"——组件的脏数据安全模型依赖 TTL 上界
    jitter: 60s            # TTL 随机抖动上限（防雪崩），0 关闭；基准 TTL 非正时不叠加抖动
    null-ttl: 30s          # null 占位的短 TTL（防穿透）；非正值 = 不缓存 null 占位（关闭穿透防护）
    double-delete-delay: 1s  # 延迟双删间隔；写极热键时可调小或评估回源放大
    max-value-kb: 512      # 单值大小上限（0.3.3+，K 字符近似）：超过则该值两级都不写缓存
                           #（读每次回源 DB，计 cache-kit.l2.value.oversized + 限频告警），
                           # 防少量大字段撑爆 Redis 内存与网络；0 关闭上限
    compression-enabled: false  # L2 值 gzip 压缩（0.3.3+，默认关）：达到 compression-min-kb 的值
                           # 以 "gz:" 前缀 + Base64 存 L2，省 Redis 内存与网络传输，代价是 CPU；
                           # L1 永远存原文；关闭后存量压缩值仍可正常读取
    compression-min-kb: 32 # 压缩阈值（K 字符）：低于该值的压缩得不偿失，存原文
    circuit-breaker:       # L2 熔断器（0.3.2+）：连续失败后短路，省掉故障期间阻塞到命令超时的无效重试
      enabled: true        # 关闭后恢复"每次调用都真实触达 Redis、失败按未命中降级"的语义
      failure-threshold: 20  # 连续失败达该次数后熔断（成功清零）
      open-duration: 10s   # 熔断持续时长，到期放行单个探测请求（成功恢复，失败重新熔断）。
                           # 注意：熔断不能消除前 N 次失败"阻塞到命令超时"的等待，
                           # spring.data.redis.timeout 的调整建议依旧成立
  broadcast:               # 失效广播（多实例部署必须开启）
    enabled: true
    mode: pubsub           # pubsub（fire-and-forget）| streams（0.3.1+，消费组 ACK，
                           # 实例短暂掉线不丢失效、恢复后补投；每实例一个消费组，
                           # 崩溃残留组由存活实例周期清理；Redis 侧多一份 Stream 数据）
                           # | sharded-pubsub（0.3.2+，Redis 7.0+ 分片广播，Cluster 下
                           # SSUBSCRIBE/SPUBLISH 替代全节点订阅/广播；仅 Lettuce 客户端，
                           # 非 Lettuce 或订阅失败时自动回退 pubsub 并告警）
    topic: cache-kit:invalidate  # streams 模式下为 Stream 键
    streams-maxlen: 10000  # streams 模式 Stream 近似裁剪上界（XADD MAXLEN ~）
  mp:                      # MyBatis-Plus 适配
    auto-cache-base-methods: true  # 自动接入总开关：BaseMapper 六方法 + IService 批量写
                                   # （saveBatch/updateBatchById/saveOrUpdateBatch）；条件写不在范围
  tx:                     # 事务感知失效（spring-tx 在类路径时生效）
    evict-after-commit: true       # 写方法处于活动事务时，失效延迟到 afterCommit；回滚不失效。
                                   # 关闭后若事务耗时超过 double-delete-delay，脏值存活至 L2 TTL（长事务勿关）
  warmup:                  # 启动预热（0.3.1+）
    enabled: true          # false 时 @CacheWarmup 方法不执行
    parallelism: 1         # 预热线程并行度：1 顺序执行；>1 时同 order 方法并发、跨 order 保序
  binlog:                  # binlog 直连失效（0.2.0+，缺省关闭），详见 BINLOG.md
    enabled: false
    host:                  # 缺省从 spring.datasource.url 解析
    port:
    database:
    username:              # 缺省用数据源账号
    password:
    server-id:             # 缺省自动生成随机值；同一 MySQL 上必须唯一
```

## 环境变量 / 属性来源说明

- `key-namespace` 未配置时自动取 `spring.application.name`；多服务共享 Redis 时必须保证命名空间隔离
- L2 仅在类路径同时具备 spring-data-redis 类与 `RedisConnectionFactory` Bean 时启用
- binlog 的 host/port/database/账号优先用显式配置，否则从 `spring.datasource.url`（Boot 3/4 双包名反射解析）回退

## Redis 连接池与容量规划（生产必读）

**为什么需要**：压测实测（`CacheStressComparisonIntegrationTest`）仅远程读路径的所有实现都顶在
**~2.5~2.8 万 ops/s**——那是 Lettuce **单条共享连接**同步命令串行的上限，与缓存组件无关。
生产 L2 QPS 需求超过该量级时必须开连接池：

```yaml
spring:
  data:
    redis:
      timeout: 2s                    # 命令超时 1~5s（装配时 >5s 告警：抖动会把读线程阻塞到超时才降级）
      lettuce:
        pool:
          enabled: true              # 需类路径有 commons-pool2（Boot 自动启用）
          max-active: 8              # 连接数 ≈ 预期 L2 QPS / 1.5万，向上取整，一般 8~32
          max-wait: 200ms            # 池耗尽等待上限：快速失败进入 L2 降级，而不是挂住业务线程
```

**容量估算三步**：

1. **L2 承载量 = 业务 QPS × (1 - L1 命中率)**。例：1 万 QPS、L1 命中 95% → L2 只需扛 ~500 QPS（单连接就够）；
   命中率掉到 60% → L2 要扛 4000 QPS（需要 1~2 条连接；留裕量配 4~8）。
2. 每连接约 **1.5 万 ops/s**（本地 Redis 单线程实测），跨机房按 RTT 折算。
3. 命中率（= L2 负载的第一变量）由 **L1 容量 / TTL / 访问倾斜**决定，见
   [CONSISTENCY.md 场景 1b](CONSISTENCY.md#场景-1b工作集超出-l1l1-容量压到-100--1000-键32-线程吞吐由命中率支配)。

**注意**：连接池解决吞吐，不解决尾延迟——`max-wait` 建议 ≤ 命令超时，保证池耗尽时快速进入
L2 降级（按未命中处理，业务不阻塞）；批量化场景（`selectBatchIds`）走 MGET 管道，一次往返摊多键，
对连接数需求远低于逐键读。
