# cache-kit-spring-boot-starter

[![Maven Central](https://img.shields.io/maven-central/v/io.github.biglv666/cache-kit-spring-boot-starter)](https://central.sonatype.com/artifact/io.github.biglv666/cache-kit-spring-boot-starter) [![CI](https://github.com/BIGLV666/cache-kit-spring-boot-starter/actions/workflows/ci.yml/badge.svg)](https://github.com/BIGLV666/cache-kit-spring-boot-starter/actions/workflows/ci.yml)

[中文](README.md) | **English**

An entity-metadata-driven three-tier cache: **Caffeine (L1) → Redis (L2) → DB (loader)** read-through.
**Zero-annotation integration** for MyBatis-Plus users; direct binlog invalidation covers the blind
spot of "writes that bypass your application"; consistency semantics are **eventually consistent** —
typically sub-second (within the double-delete window), worst case bounded by the L2 TTL
(see [mechanism & measurements](docs/CONSISTENCY.md), Chinese).

## Features

- Three-tier read-through: L1 → L2 → method body (DB), hits back-fill level by level
- Cache keys derived automatically from entity metadata (`table:id`) — no hand-written SpEL keys
- MyBatis-Plus annotation reuse: `@TableName` / `@TableId` are the metadata; reads and writes
  integrate automatically
- Per-ID batch decomposition: cached hits used directly, only the missing set loads; single-flight
  (stampede), TTL jitter (avalanche), null placeholders (penetration)
- Multi-instance consistency: write-then-evict + invalidation broadcast (pub/sub or Streams
  consumer group) + delayed double delete
- Direct binlog invalidation: DBA updates and other services' writes invalidate within seconds,
  no Canal ([docs/BINLOG.md](docs/BINLOG.md), Chinese)
- `@CacheWarmup` startup warmup, `EntityCache` manual handle, pluggable `L1Channel` SPI
- Observability: Micrometer counters / hit-rate gauges / propagation timer + Grafana dashboard
  ([docs/OBSERVABILITY.md](docs/OBSERVABILITY.md), Chinese)
- Strong-consistency escape hatch: `CacheKit.withDb(...)` scope bypass

## Quick start

### Dependency

```xml
<dependency>
    <groupId>io.github.biglv666</groupId>
    <artifactId>cache-kit-spring-boot-starter</artifactId>
    <version>0.3.1</version>
</dependency>
```

Requires JDK 17+ and Spring Boot 3.5.x (Boot 4 forward-compatible, see
[docs/BOOT4-NATIVE.md](docs/BOOT4-NATIVE.md), Chinese). L2 (Redis) and the MyBatis-Plus adapter
are enabled automatically by classpath conditions.

> Minimal runnable example: [cache-kit-sample](https://github.com/BIGLV666/cache-kit-sample)
> (one `docker compose up` for Redis + binlog-enabled MySQL; five curl commands verify
> every effect).

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
- **`IService.saveBatch` / `updateBatchById` / `saveOrUpdateBatch` (0.3.0+)**: covered by a
  dedicated service aspect (SqlSession batch channel bypasses the mapper proxy)
- **Conditional writes (`update(Wrapper)` etc.) cannot be precisely invalidated** — enable binlog
  to cover them, otherwise TTL bounds staleness

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

Primary-key derivation is **strict**: an entity instance parameter, or a scalar parameter named
exactly like the key field; when neither holds (condition-field lookups) it warns and goes to the
DB — never guesses. See [limitations & FAQ](docs/LIMITATIONS.md) (Chinese).

### Manual control & warmup

```java
@CacheHandle(User.class)
private EntityCache<User> userCache;   // get / getBatch / evict / evictBatch

@CacheWarmup(order = 10)               // runs once once the context is ready, back-fills hot data
void loadHotUsers() { userMapper.selectBatchIds(List.of(1L, 2L, 3L)); }
```

### Minimal configuration

```yaml
cache-kit:
  l1:
    ttl: 30s
  l2:
    ttl: 10m
  # broadcast:
  #   mode: streams     # for multi-instance setups sensitive to invalidation loss during restart
  #                     # windows (default pubsub)
  # binlog:
  #   enabled: true     # cover writes that bypass your application
```

Full reference: [docs/CONFIG.md](docs/CONFIG.md) (Chinese).

## Documentation

Topic docs live under [`docs/`](docs/) and are currently written in Chinese:

| Doc | Content |
|---|---|
| [docs/BINLOG.md](docs/BINLOG.md) | direct binlog invalidation: config, reconnect semantics, troubleshooting |
| [docs/CONSISTENCY.md](docs/CONSISTENCY.md) | consistency mechanism, broadcast channel choice, benchmark, production measurements |
| [docs/CONFIG.md](docs/CONFIG.md) | full configuration reference |
| [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) | Micrometer metrics, alerting, Grafana dashboard |
| [docs/RESILIENCE.md](docs/RESILIENCE.md) | resilience design: degradation, retries, backlog protection, security |
| [docs/LIMITATIONS.md](docs/LIMITATIONS.md) | MVP boundaries, known limitations, FAQ |
| [docs/BOOT4-NATIVE.md](docs/BOOT4-NATIVE.md) | Spring Boot 4 compatibility, GraalVM native-image |
| [docs/CHANGELOG.md](docs/CHANGELOG.md) | release notes |
