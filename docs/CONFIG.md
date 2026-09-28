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
  l2:                      # 远程缓存（Redis，类路径有 spring-data-redis 且存在 RedisConnectionFactory 时启用；
                           # 只有类没有工厂 Bean 时自动降级为 L1-only，不影响启动）
    ttl: 10m               # L2 TTL 基准；非正值（0/负数）= 跳过 L2 写入（等效禁用 Redis 缓存，启动打警告），
                           # 不是"永不过期"——组件的脏数据安全模型依赖 TTL 上界
    jitter: 60s            # TTL 随机抖动上限（防雪崩），0 关闭；基准 TTL 非正时不叠加抖动
    null-ttl: 30s          # null 占位的短 TTL（防穿透）；非正值 = 不缓存 null 占位（关闭穿透防护）
    double-delete-delay: 1s  # 延迟双删间隔；写极热键时可调小或评估回源放大
  broadcast:               # 失效广播（多实例部署必须开启）
    enabled: true
    mode: pubsub           # pubsub（fire-and-forget）| streams（0.3.1+，消费组 ACK，
                           # 实例短暂掉线不丢失效、恢复后补投；每实例一个消费组，
                           # 崩溃残留组由存活实例周期清理；Redis 侧多一份 Stream 数据）
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
