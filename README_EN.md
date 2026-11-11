# cache-kit-spring-boot-starter

[![Maven Central](https://img.shields.io/maven-central/v/io.github.biglv666/cache-kit-spring-boot-starter)](https://central.sonatype.com/artifact/io.github.biglv666/cache-kit-spring-boot-starter) [![CI](https://github.com/BIGLV666/cache-kit-spring-boot-starter/actions/workflows/ci.yml/badge.svg)](https://github.com/BIGLV666/cache-kit-spring-boot-starter/actions/workflows/ci.yml)

[中文](README.md) | **English**

An entity-metadata-driven three-tier cache: **Caffeine (L1) → Redis (L2) → DB (loader)** read-through. **Zero-annotation integration** for MyBatis-Plus users; direct binlog invalidation covers the blind spot of "writes that bypass your application"; consistency semantics are **eventually consistent** — typically sub-second (within the double-delete window), worst case bounded by the L2 TTL (see [mechanism](docs/CONSISTENCY_EN.md)).

## Features

- Three-tier read-through: L1 → L2 → method body (DB), hits back-fill level by level
- Cache keys derived automatically from entity metadata (`table:id`) — no hand-written SpEL keys; composite keys (multiple `@CacheId`) join in declaration order (0.3.3+)
- MyBatis-Plus annotation reuse: `@TableName` / `@TableId` are the metadata; reads and writes integrate automatically
- Per-ID batch decomposition: cached hits used directly, only the missing set loads; single-flight (stampede), TTL jitter (avalanche), null placeholders (penetration)
- Multi-instance consistency: write-then-evict + invalidation broadcast (pub/sub or Streams consumer group) + delayed double delete
- Direct binlog invalidation: DBA updates and other services' writes invalidate within seconds, no Canal; runtime DDL changes are detected automatically ([docs/BINLOG_EN.md](docs/BINLOG_EN.md))
- `@CacheWarmup` startup warmup, `EntityCache` manual handle, pluggable `L1Channel` SPI
- Generic value cache (0.4.0+): `@CacheObject` + `@CacheLoad` + `CacheStore` for arbitrary JSON-serializable values (DTOs, config, `List<T>`, `Map`), reusing the tiered chain and invalidation broadcast; lifecycle is controlled by `put/evict/refresh` ([docs/GENERIC_EN.md](docs/GENERIC_EN.md))
- L1 refresh-ahead (0.3.3+, opt-in) and L2 value policy (size cap + optional gzip, 0.3.3+)
- Observability: Micrometer metrics + Grafana dashboard + `/actuator/cachekit`; strong-consistency escape hatch `CacheKit.withDb(...)` ([docs/OBSERVABILITY_EN.md](docs/OBSERVABILITY_EN.md))

## Quick start

### Dependency

```xml
<dependency>
    <groupId>io.github.biglv666</groupId>
    <artifactId>cache-kit-spring-boot-starter</artifactId>
    <version>0.4.0</version>
</dependency>
```

Requires JDK 17+ and Spring Boot 3.5.x (Boot 4 forward-compatible, see [docs/BOOT4-NATIVE_EN.md](docs/BOOT4-NATIVE_EN.md)). L2 (Redis) and the MyBatis-Plus adapter are enabled automatically by classpath conditions. Minimal runnable example: [cache-kit-sample](https://github.com/BIGLV666/cache-kit-sample).

### MyBatis-Plus projects (zero annotations)

```java
@TableName("t_user")
public class User {
    @TableId
    private Long userId;
    private String userName;
}

userMapper.selectById(1L);                       // three-tier cache automatically
userMapper.selectBatchIds(List.of(1L, 2L, 3L));  // per-ID: cached hits used, only missing loaded
userMapper.updateById(user);                     // automatic invalidation + broadcast + double delete
```

Write coverage (automatic invalidation, no annotations):

- `updateById` / `deleteById` / `deleteByIds` / `insert`: intercepted at the mapper proxy
- `IService.saveBatch` / `updateBatchById` / `saveOrUpdateBatch` (0.3.0+): covered by a dedicated service aspect (SqlSession batch channel bypasses the mapper proxy)
- Conditional writes (`update(Wrapper)` etc.) cannot be precisely invalidated — enable binlog to cover them, otherwise TTL bounds staleness

### Non-MP projects (entity annotations)

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

Primary-key derivation is **strict**: an entity instance parameter, or a scalar parameter named exactly like the key field; when neither holds (condition-field lookups) it warns and goes to the DB — never guesses. See [limitations & FAQ](docs/LIMITATIONS_EN.md).

### Manual control & warmup

```java
@CacheHandle(User.class)
private EntityCache<User> userCache;   // get / getBatch / evict / evictBatch

@CacheWarmup(order = 10)               // runs once once the context is ready
void loadHotUsers() { userMapper.selectBatchIds(List.of(1L, 2L, 3L)); }
```

### Generic value cache (arbitrary objects)

```java
@CacheObject(prefix = "app-config", ttl = 300)
public class AppConfig { /* any JSON-serializable DTO */ }

@CacheLoad(key = "#tenant + ':' + #name")          // runs only on a miss, then back-fills
public AppConfig load(String tenant, String name) { return remoteConfig.get(tenant, name); }

@CacheHandle(AppConfig.class)
private CacheStore<AppConfig> configStore;          // business-controlled lifecycle
// configStore.get / put / evict / refresh
```

No binlog integration — invalidate explicitly or rely on TTL. See [docs/GENERIC_EN.md](docs/GENERIC_EN.md).

### Minimal configuration

```yaml
cache-kit:
  l1:
    ttl: 30s
  l2:
    ttl: 10m
  # broadcast:
  #   mode: streams     # for multi-instance setups sensitive to invalidation loss on restart
  # binlog:
  #   enabled: true     # cover writes that bypass your application
```

Full reference: [docs/CONFIG_EN.md](docs/CONFIG_EN.md).

## Documentation

All topic docs have English versions under [`docs/`](docs/) (CHANGELOG remains Chinese):

| Doc | Content |
|---|---|
| [docs/BINLOG_EN.md](docs/BINLOG_EN.md) | direct binlog invalidation: config, reconnect semantics, troubleshooting |
| [docs/CONSISTENCY_EN.md](docs/CONSISTENCY_EN.md) | consistency mechanism, stale-read windows, broadcast channel choice |
| [docs/CONFIG_EN.md](docs/CONFIG_EN.md) | full configuration reference |
| [docs/GENERIC_EN.md](docs/GENERIC_EN.md) | generic value cache: `@CacheObject`/`@CacheLoad`/`CacheStore` |
| [docs/OBSERVABILITY_EN.md](docs/OBSERVABILITY_EN.md) | Micrometer metrics, `/actuator/cachekit`, alerting, Grafana |
| [docs/RESILIENCE_EN.md](docs/RESILIENCE_EN.md) | resilience: degradation, retries, backlog protection, security |
| [docs/LIMITATIONS_EN.md](docs/LIMITATIONS_EN.md) | MVP boundaries, known limitations, FAQ |
| [docs/BOOT4-NATIVE_EN.md](docs/BOOT4-NATIVE_EN.md) | Spring Boot 4 compatibility, GraalVM native-image |
| [docs/CHANGELOG.md](docs/CHANGELOG.md) | release notes (Chinese) |

## License

Apache License 2.0
