# Spring Boot 4 / GraalVM Native（0.3.1+）

## Spring Boot 4 前向兼容

binlog 参数回退所依赖的 `DataSourceProperties` 已改为按 Boot 3 / Boot 4 双 FQN 反射解析：

- Boot 3：`org.springframework.boot.autoconfigure.jdbc.DataSourceProperties`
- Boot 4：`org.springframework.boot.jdbc.autoconfigure.DataSourceProperties`

两代宿主均可启动；`AutoConfigureAfter` 同步兼容了 Boot 4 的 `RedisAutoConfiguration` 新包名。

组件当前仍在 **Boot 3.5 基线**上构建与测试。Boot 4 宿主建议升级后跑一遍本组件集成测试并反馈问题。

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
