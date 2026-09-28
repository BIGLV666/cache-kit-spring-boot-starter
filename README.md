# cache-kit-spring-boot-starter

[![Maven Central](https://img.shields.io/maven-central/v/io.github.biglv666/cache-kit-spring-boot-starter)](https://central.sonatype.com/artifact/io.github.biglv666/cache-kit-spring-boot-starter) [![CI](https://github.com/BIGLV666/cache-kit-spring-boot-starter/actions/workflows/ci.yml/badge.svg)](https://github.com/BIGLV666/cache-kit-spring-boot-starter/actions/workflows/ci.yml)

**中文** | [English](README_EN.md)

实体元数据驱动的三级缓存组件：**Caffeine（L1）→ Redis（L2）→ DB（loader）** read-through。
对 MyBatis-Plus 用户**零注解接入**；binlog 直连失效补齐"绕过应用的写"盲区；
一致性语义为**最终一致**——常态秒级（双删窗口内），最坏受 L2 TTL 上界约束（[机制与实测](docs/CONSISTENCY.md)）。

## 特性

- 三级 read-through：L1 → L2 → 方法体（查 DB），命中逐级回填
- 缓存键由实体元数据自动推导（`表名:主键`），无需手写 SpEL 键表达式
- MyBatis-Plus 注解复用：`@TableName` / `@TableId` 即元数据，读写作自动接入
- 批量 per-ID 拆解：命中直接用、缺失才回源；single-flight 防击穿、TTL 抖动防雪崩、null 占位防穿透
- 多实例一致性：写后删缓存 + 失效广播（pub/sub 或 Streams 消费组）+ 延迟双删
- binlog 直连失效：DBA 改库、其他服务写入也能秒级失效，无需 Canal（[文档](docs/BINLOG.md)）
- `@CacheWarmup` 启动预热、`EntityCache` 手动句柄、`L1Channel` 可插拔 SPI
- 可观测性：Micrometer 计数器/命中率 Gauge/传播延迟 Timer + Grafana 面板（[文档](docs/OBSERVABILITY.md)）
- 强一致读出口：`CacheKit.withDb(...)` 作用域旁路

## 快速开始

### 引入依赖

```xml
<dependency>
    <groupId>io.github.biglv666</groupId>
    <artifactId>cache-kit-spring-boot-starter</artifactId>
    <version>0.3.1</version>
</dependency>
```

要求 JDK 17+、Spring Boot 3.5.x（Boot 4 前向兼容，见 [docs/BOOT4-NATIVE.md](docs/BOOT4-NATIVE.md)）。
L2（Redis）与 MyBatis-Plus 适配按类路径自动启用。

### MyBatis-Plus 项目（零注解）

```java
@TableName("t_user")
public class User {
    @TableId
    private Long userId;
    private String userName;
}

userMapper.selectById(1L);                       // 自动走三级缓存
userMapper.selectBatchIds(List.of(1L, 2L, 3L));  // per-ID 拆解：命中直接用，缺失才回源
userMapper.updateById(user);                     // 自动失效 + 广播 + 延迟双删
```

写覆盖范围（自动失效，无需注解）：

- `updateById` / `deleteById` / `deleteByIds` / `insert`：经 mapper 代理，切面直接拦截
- **`IService.saveBatch` / `updateBatchById` / `saveOrUpdateBatch`（0.3.0+）**：SqlSession 批量通道绕过
  mapper 代理，由独立 service 切面覆盖
- **条件写（`update(Wrapper)` 等）无法精确失效**——开启 binlog 可覆盖，否则靠 TTL 兜底

### 非 MP 项目（实体注解）

```java
@CacheEntity(ttl = 3600)
public class User {
    @CacheId
    private Long userId;
    private String userName;
}

@CachedQuery
User selectByUserId(@Param("userId") Long userId);

@CacheInvalidate(entity = User.class)
int updateStatus(@Param("userId") Long userId, @Param("status") int status);
```

主键推导是**严格模式**：参数含实体实例，或标量参数名与主键字段同名；两条路都走不通（条件字段查询）
会警告并直查 DB，绝不猜测——[边界与 FAQ](docs/LIMITATIONS.md)。

### 手动控制与预热

```java
@CacheHandle(User.class)
private EntityCache<User> userCache;   // get / getBatch / evict / evictBatch

@CacheWarmup(order = 10)               // 上下文就绪后自动执行一次，回填热点数据
void loadHotUsers() { userMapper.selectBatchIds(List.of(1L, 2L, 3L)); }
```

### 最小配置

```yaml
cache-kit:
  l1:
    ttl: 30s
  l2:
    ttl: 10m
  # broadcast:
  #   mode: streams     # 多实例且对"重启窗口丢失效"敏感时启用（默认 pubsub）
  # binlog:
  #   enabled: true     # 覆盖"绕过应用的写"
```

完整参数见 [docs/CONFIG.md](docs/CONFIG.md)。

## 文档

| 文档 | 内容 |
|---|---|
| [docs/BINLOG.md](docs/BINLOG.md) | binlog 直连失效：配置、断线重连语义、排查 |
| [docs/CONSISTENCY.md](docs/CONSISTENCY.md) | 一致性机制、广播通道选择（pubsub/streams）、对比 benchmark、生产实测 |
| [docs/CONFIG.md](docs/CONFIG.md) | 配置全参考 |
| [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) | Micrometer 指标、告警建议、Grafana 面板 |
| [docs/RESILIENCE.md](docs/RESILIENCE.md) | 稳健性设计：降级、重试、积压保护、安全边界 |
| [docs/LIMITATIONS.md](docs/LIMITATIONS.md) | MVP 边界、已知限制、FAQ |
| [docs/BOOT4-NATIVE.md](docs/BOOT4-NATIVE.md) | Spring Boot 4 兼容、GraalVM native-image |
| [docs/CHANGELOG.md](docs/CHANGELOG.md) | 版本变更记录 |
| [docs/DEV.md](docs/DEV.md) | 构建、本地容器、发布流程、测试布局 |

English: see [README_EN.md](README_EN.md)（topic docs 目前为中文）。
