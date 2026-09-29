# Spring Boot 4 / GraalVM Native

## Spring Boot 4 兼容（0.3.2 起，实测验证）

组件在 **Boot 3.5 基线上构建发布**；CI 额外有一个 `boot4-compat` job，在 Spring Boot 4.0.0
依赖树下跑全部非容器测试（装配校验、MP 切面、指标、binlog 生命周期等 111 例，全绿）。

Boot 4 适配点的真实变化（全部已在代码中处理）：

| 变化 | Boot 3 | Boot 4 | 处理 |
|---|---|---|---|
| AOP starter | `spring-boot-starter-aop` | `spring-boot-starter-aspectj` | 主 pom 保持 Boot 3 基线；Boot 4 宿主引入其自身依赖管理即可，无需组件侧动作 |
| Redis autoconfigure | `org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration` | `org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration`（**包名与类名都改了**） | `@AutoConfigureAfter` 双 FQN 声明 |
| DataSourceProperties | `org.springframework.boot.autoconfigure.jdbc.DataSourceProperties` | `org.springframework.boot.jdbc.autoconfigure.DataSourceProperties` | 反射双 FQN 解析（binlog 参数回退路径） |
| testcontainers BOM | Boot parent 管理 | Boot 4 parent 不再管理 | pom 显式声明版本（仅 test scope） |

测试套件同样双跑：集成测试引用的 Boot 功能 autoconfigure 类经
`BootAutoconfigCompat`（测试 support）反射双 FQN 探测——Boot 3.5 与 Boot 4 依赖树下
**同一测试源码**均可编译运行。

已知边界：boot4-compat job 覆盖非容器测试（111 例）；Testcontainers 集成测试
（binlog/多实例广播/压测）仍在 Boot 3.5 基线运行——它们依赖的 spring-data-redis 4.x
与 MP 的 Boot 4 行为将随三方依赖的 Boot 4 支持逐步纳入。

## GraalVM native-image

自动装配通过 `RuntimeHintsRegistrar`（`CacheKitRuntimeHints`）注册了组件自身的反射提示：
`@CacheEntity` / `@CacheId` / `@CacheHandle` / `@CacheWarmup` 注解、`CacheKitProperties` 配置绑定、
`EntityCache` JDK 代理、`CacheKitException`。

**宿主实体类的 Jackson 序列化反射需使用方自行注册**（native 下 Jackson 依赖运行时元数据，
组件在构建期无法枚举宿主实体）：

```java
@RegisterReflectionForBinding(UserEntity.class)   // 每个进入缓存的实体各注册一次
@Configuration
class NativeHints {}
```

binlog 模块依赖 mysql-binlog-connector-java，其 native 支持无官方声明——binlog 场景建议先以
tracing agent 验证，或暂留 JVM 模式。
