# Spring Boot 4 / GraalVM native

## Spring Boot 4 compatibility (verified since 0.3.2)

The component builds and ships on the **Boot 3.5 baseline**; CI additionally runs a
`boot4-compat` job that executes the full non-container test suite (assembly validation, MP
aspects, metrics, binlog lifecycle, etc.) under a Spring Boot 4.0.0 dependency tree, all green.

Actual Boot 4 deltas (all already handled in code):

| Change | Boot 3 | Boot 4 | Handling |
|---|---|---|---|
| AOP starter | `spring-boot-starter-aop` | `spring-boot-starter-aspectj` | the main pom stays on the Boot 3 baseline; a Boot 4 host brings its own dependency management, no component-side action needed |
| Redis autoconfigure | `org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration` | `org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration` (**package and class renamed**) | dual-FQN declaration on `@AutoConfigureAfter` |
| DataSourceProperties | `org.springframework.boot.autoconfigure.jdbc.DataSourceProperties` | `org.springframework.boot.jdbc.autoconfigure.DataSourceProperties` | dual-FQN reflective resolution (binlog parameter fallback path) |
| testcontainers BOM | managed by the Boot parent | no longer managed by the Boot 4 parent | explicit version in the pom (test scope only) |

The test suite itself runs on both trees: Boot autoconfigure classes referenced by integration
tests are probed through `BootAutoconfigCompat` (test support) with dual-FQN reflection — the
**same test sources** compile and run under Boot 3.5 and Boot 4.

Known boundary: the boot4-compat job covers non-container tests; Testcontainers integration
tests (binlog / multi-instance broadcast / stress) still run on the Boot 3.5 baseline — their
spring-data-redis 4.x dependencies and MP's Boot 4 behavior will be folded in as third-party
Boot 4 support lands.

## GraalVM native-image

Auto-configuration registers the component's own reflection hints through a
`RuntimeHintsRegistrar` (`CacheKitRuntimeHints`): the `@CacheEntity` / `@CacheId` / `@CacheHandle`
/ `@CacheWarmup` annotations, `CacheKitProperties` configuration binding, the `EntityCache` JDK
proxy, and `CacheKitException`.

**Reflection hints for host entity classes (Jackson serialization) must be registered by the
consumer** (Jackson needs runtime metadata in native; the component cannot enumerate host
entities at build time):

```java
@RegisterReflectionForBinding(UserEntity.class)   // once per entity entering the cache
@Configuration
class NativeHints {}
```

The binlog module depends on mysql-binlog-connector-java, which has no official native support
statement — for binlog scenarios, verify with the tracing agent first or stay on the JVM mode.
