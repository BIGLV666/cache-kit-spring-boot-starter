# Generic value cache: @CacheObject / @CacheLoad / CacheStore

A key-value cache for **arbitrary JSON-serializable values**, parallel to the entity cache
(`@CacheEntity` / `@CachedQuery`). It reuses the same L1 (Caffeine) → L2 (Redis) → loader chain,
single-flight, null placeholders, TTL jitter, delayed double delete, L2 circuit breaker and
invalidation broadcast (pub/sub / streams / sharded-pubsub).

## Entity cache vs generic cache

| | Entity cache | Generic value cache |
|---|---|---|
| Value | Entity | Any JSON-serializable value (DTO, config, `List<T>`, `Map<K,V>`) |
| Key | Derived from entity metadata (`table:id`) | SpEL on `@CacheLoad` or explicit `CacheStore` key |
| Invalidation | Write methods / MP aspects / binlog | `CacheStore.evict` or TTL |
| Binlog | Supported | **Not supported** (no table/row mapping) |
| Use cases | Primary-key entity reads | Remote config, aggregates, dictionaries, computed results |

## Quick start

```java
@CacheObject(prefix = "app-config", ttl = 300)
public class AppConfig {
    private String name;
    private Map<String, Object> values;
    // getters / setters / no-arg constructor
}

@Component
public class AppConfigService {

    @CacheLoad(
        key = "#tenant + ':' + #name",
        condition = "#tenant != null",
        cacheNull = true
    )
    public AppConfig load(String tenant, String name) {
        return remoteConfigClient.get(tenant, name); // only runs on a cache miss
    }
}
```

On a hit the method body is skipped; on a miss it runs and the result is written to L1/L2.
`CacheKit.withDb(...)` bypasses the cache for the scope.

## Annotations

### @CacheObject (on the value type)

| Attribute | Default | Description |
|---|---|---|
| `prefix` | simple name → snake_case | Key prefix; must not be empty or contain `:` / reserved `__gk__` |
| `ttl` | -1 | Default L2 TTL in seconds; ≤0 uses `cache-kit.l2.ttl` |
| `cacheNull` | true | Cache null results; `@CacheLoad.cacheNull=false` can disable |

One prefix can only be bound to one value type; collisions fail fast at startup/first registration.

The value type must be a concrete class Jackson can instantiate. Interfaces and abstract classes are
rejected at startup/first registration unless they carry
`@com.fasterxml.jackson.annotation.JsonTypeInfo` for polymorphic deserialization — otherwise writes
succeed but every read fails to decode, which looks like a permanent miss.

### @CacheLoad (on the loader method)

| Attribute | Default | Description |
|---|---|---|
| `key` | required | Key SpEL; parameter names are available, e.g. `"#id"` |
| `value` | void | Metadata source type (`@CacheObject` type); required when the return type cannot be resolved, and used as fallback for unresolved generic positions |
| `ttl` | -1 | Per-read L2 TTL override in seconds |
| `condition` | empty | SpEL condition; false bypasses the cache |
| `cacheNull` | true | Cache null (also requires `@CacheObject.cacheNull=true`) |

**Strict keys**: a null/blank expression result throws; the framework never guesses a key.
If no unique `@CacheObject` type can be resolved, startup fails and `value = Xxx.class` is required.

**Type resolution (target-class aware)**: generic variables in the return type are resolved against
the **actual implementation class** — `T`, `List<T>`, `Map<K,V>`, arrays and `T[]` all become
concrete arguments (e.g. `AppConfigService extends Base<AppConfig>` resolves `T` → `AppConfig`),
so hits never degrade to `LinkedHashMap`. Positions that remain unresolved (top-level `Object`,
unbound type variables, unbounded wildcards) fall back to `value = Xxx.class`. A raw `List`/`Set`
uses `value` as the element type; a raw `Map` / `Map<?, V>` keys by `String` (JSON's natural object
key type), while an explicit `Map<String,Object>` keeps its nested `Object`.

**Annotation placement**: `@CacheLoad` may be declared on an interface or generic base method. The
aspect resolves it against the target class (method → concrete override → interfaces → superclass),
so implementations need no duplicate annotation; startup scanning covers interface methods too, so
the prefix whitelist and fail-fast do not depend on the first call.

**Exception semantics**: checked exceptions declared by the method propagate unchanged to the
caller (single-flight waiters receive the same exception instance) and are never cached. Runtime
exceptions also propagate unchanged instead of being wrapped in `CacheKitException`.

## Lifecycle: CacheStore

```java
@CacheHandle(AppConfig.class)
private CacheStore<AppConfig> configStore;

configStore.get("t1:db");                     // peek only
configStore.get("t1:db", () -> load(...));    // read-through
configStore.put("t1:db", updated);            // update
configStore.put("t1:db", updated, ttl);       // update with TTL
configStore.evict("t1:db");                   // L2 + local L1 + broadcast + double delete
configStore.refresh("t1:db", () -> load(...));// force delete then reload
```

`CacheStore` fields may use container generics such as `CacheStore<List<AppConfig>>`; the field
generic type drives deserialization, while `@CacheHandle.value()` still points to the
`@CacheObject` metadata type. Fields declared on a generic base class are resolved against the
concrete subclass (`class Holder<T> { CacheStore<List<T>> store; }` + `Holder<AppConfig>` →
`List<AppConfig>`); a top-level `CacheStore<Object>` falls back to `@CacheHandle.value()`.

Transaction semantics match the entity cache: `evict` inside an active transaction is deferred to
afterCommit when `cache-kit.tx.evict-after-commit` is enabled; `refresh` is an explicit immediate
delete-and-reload.

Write semantics: when serialization fails or the value exceeds `l2.max-value-kb`, `put(key, value)`
**invalidates the old value** before returning instead of silently keeping it (otherwise the caller
sees a successful update while reads keep returning the old value until TTL). `put(key, null)` is
treated as an invalidation when `cacheNull=false` or `l2.null-ttl` is non-positive (negative caching
disabled) — L2/L1 deletion, broadcast, delayed double delete and the `evictions` counter all apply,
matching `evict(key)`.

## Keys and broadcast

```text
[CacheKeyCustomizer segment:]namespace:__gk__:prefix:user key
```

- `__gk__` is reserved to isolate generic keys from entity keys.
- User keys longer than `cache-kit.generic.max-key-length` (default 512) are SHA-256 normalized.
- Invalidation messages share the entity topic/stream but carry a reserved message prefix. The
  subscriber validates the prefix against registered `@CacheObject` prefixes before evicting local
  L1; unknown prefixes are ignored.
- `pubsub`, `streams` and `sharded-pubsub` modes all work without extra configuration.

## Configuration

```yaml
cache-kit:
  generic:
    enabled: true       # false: @CacheLoad runs the method directly; CacheStore becomes a no-op
    max-key-length: 512 # longer keys are SHA-256 normalized
```

## Boundaries

- **No binlog integration**: arbitrary values may be aggregates/computed results with no table-row
  mapping. Call `evict`/`refresh` on writes, or rely on TTL.
- **No `evictAll`/prefix wipe in the MVP** to avoid production `SCAN` risks.
- **Self-invocation bypasses the aspect**, same as Spring AOP.
- **Prefix collisions fail fast**: two types sharing a prefix would deserialize into wrong data.
- **Values must be JSON-serializable**: serialization failures skip caching; values above
  `l2.max-value-kb` are not cached at either level.
- **Null ambiguity**: `CacheStore.get(key)` returns null for both a miss and a cached null.
- **Type evolution**: Jackson tolerates unknown fields; incompatible changes fall back to a miss
  and reload. Use a new `key-namespace` for large migrations.
- **Multi-tenancy**: `CacheKeyCustomizer.segment()` participates in generic keys. Broadcast does
  not validate customizer segments (deleting a key that does not exist locally is harmless).
