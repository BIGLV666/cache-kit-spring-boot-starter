# Full configuration reference (prefix `cache-kit.*`)

```yaml
cache-kit:
  enabled: true            # master switch
  key-namespace:           # key namespace: defaults to spring.application.name when unset
                           # (required isolation when multiple services share one Redis)
  require-key-namespace: false  # true → startup fails when both key-namespace and
                                # spring.application.name are empty (zero tolerance for key collisions)
  l1:                      # local cache (Caffeine by default; replaceable via a custom
                           # L1Channel Bean since 0.3.1)
    max-entries: 65536     # max entries (entry-count bound; no per-entry size limit)
    max-weight-kb: 0       # weight bound (K chars): >0 replaces max-entries, keeping large
                           # entities from blowing up heap memory. Actual memory bound ≈
                           # max-weight-kb × 2KB (BMP chars are 2 bytes in UTF-16, e.g. CJK);
                           # recommended in production, e.g. 65536 ≈ 128MB. Per-entry weight is
                           # json length / 1024, minimum 1
    ttl: 30s               # L1 TTL: must be significantly smaller than l2.ttl — it is the
                           # dirty-read upper bound when broadcast messages are lost
    refresh-ahead: 0s      # L1 refresh-ahead window (0.3.3+, off by default): when an L1 hit has
                           # remaining TTL below this value, the current value is returned and a
                           # background refresh (L2/DB back-fill) is scheduled, absorbing the
                           # post-expiry reload spike. Reads always see unexpired values — the
                           # "L1 TTL = dirty-read upper bound" promise is unchanged; null
                           # placeholders are refreshed too (re-probes the DB before expiry).
                           # Must be < l1.ttl (otherwise a startup warning disables it);
                           # applies to single-key reads only, batch lookups never trigger it
  l2:                      # remote cache (Redis; enabled when spring-data-redis is on the
                           # classpath AND a RedisConnectionFactory bean exists — class without
                           # factory bean degrades to L1-only without failing startup)
    ttl: 10m               # L2 TTL base; a non-positive value (0/negative) = skip all L2 writes
                           # (effectively disables Redis caching, warns at startup) — NOT
                           # "never expires": the staleness safety model relies on TTL bounds
    jitter: 60s            # random TTL jitter cap (anti-avalanche), 0 disables; no jitter when
                           # the base TTL is non-positive
    null-ttl: 30s          # short TTL for null placeholders (anti-penetration); non-positive =
                           # do not cache null placeholders (penetration protection off)
    double-delete-delay: 1s  # delayed double-delete interval; shrink for very hot written keys
                           # or weigh the reload amplification
    max-value-kb: 512      # per-value size cap (0.3.3+, K chars approx): values beyond it skip
                           # BOTH cache levels (reads hit the DB every time; counted by
                           # cache-kit.l2.value.oversized + rate-limited warning), keeping a few
                           # large fields from blowing up Redis memory and the network; 0 disables
    compression-enabled: false  # gzip-compress L2 values (0.3.3+, off by default): values at or
                           # above compression-min-kb are stored as "gz:" prefix + Base64 — saves
                           # Redis memory and network at the cost of CPU; L1 always stores the
                           # plain text; already-compressed values remain readable after turning
                           # it off
    compression-min-kb: 32 # compression threshold (K chars): compressing smaller values costs
                           # more than it saves, so they stay plain
    circuit-breaker:       # L2 circuit breaker (0.3.2+): short-circuits after consecutive
                           # failures, skipping the futile retries that block until command timeout
      enabled: true        # turning it off restores "every call reaches Redis; failures degrade
                           # as misses"
      failure-threshold: 20  # consecutive failures before opening (successes reset the counter)
      open-duration: 10s   # open-state duration; a single probe is allowed after expiry
                           # (success closes, failure re-opens). Note: the breaker cannot remove
                           # the "block until command timeout" wait of the first N failures —
                           # the spring.data.redis.timeout advice still stands
  broadcast:               # invalidation broadcast (mandatory for multi-instance deployments)
    enabled: true
    mode: pubsub           # pubsub (fire-and-forget) | streams (0.3.1+, consumer-group ACK —
                           # instances briefly offline do not lose invalidations and catch up on
                           # recovery; one consumer group per instance, leftover groups of crashed
                           # instances are periodically reclaimed; one extra Stream on Redis side)
                           # | sharded-pubsub (0.3.2+, Redis 7.0+ sharded pub/sub — under Cluster,
                           # SSUBSCRIBE/SPUBLISH replaces all-node subscribe/broadcast; Lettuce
                           # client only, falls back to pubsub with a warning otherwise)
    topic: cache-kit:invalidate  # broadcast topic (the Stream key in streams mode)
    streams-maxlen: 10000  # approximate Stream trim bound in streams mode (XADD MAXLEN ~)
  mp:                      # MyBatis-Plus adapter
    auto-cache-base-methods: true  # master switch: the six BaseMapper methods + IService batch
                                   # writes (saveBatch/updateBatchById/saveOrUpdateBatch);
                                   # conditional writes are not covered
  tx:                     # transaction-aware invalidation (effective when spring-tx is present)
    evict-after-commit: true       # when the write method runs in an active transaction,
                                   # invalidation is deferred to afterCommit; rollback doesn't
                                   # invalidate. If disabled and the transaction outlives
                                   # double-delete-delay, stale values live up to L2 TTL
                                   # (do not disable for long transactions)
  warmup:                  # startup warmup (0.3.1+)
    enabled: true          # false skips @CacheWarmup methods entirely
    parallelism: 1         # warmup parallelism: 1 = sequential; >1 runs same-order methods
                           # concurrently while keeping cross-order sequencing
  binlog:                  # direct binlog invalidation (0.2.0+, off by default), see BINLOG.md
    enabled: false
    host:                  # resolved from spring.datasource.url when unset
    port:
    database:
    username:              # falls back to the datasource credentials
    password:
    server-id:             # a random value is generated when unset; must be unique per MySQL
```

## Environment variables / property sources

- `key-namespace` defaults to `spring.application.name`; namespace isolation is mandatory when
  multiple services share Redis
- L2 activates only when both the spring-data-redis classes and a `RedisConnectionFactory` bean exist
- binlog host/port/database/credentials prefer explicit config, falling back to
  `spring.datasource.url` (reflection works on both Boot 3/4 property class names)

## Redis connection pool & capacity planning (production must-read)

**Why it matters**: stress measurements (`CacheStressComparisonIntegrationTest`) show every
remote-read-only implementation tops out at **~25–28k ops/s** — the ceiling of Lettuce's
single shared synchronous connection, unrelated to the caching component. When your L2 QPS
demand exceeds that order of magnitude, enable a connection pool:

```yaml
spring:
  data:
    redis:
      timeout: 2s                    # command timeout 1–5s (a startup warning fires above 5s:
                                     # jitter would block reader threads until the timeout)
      lettuce:
        pool:
          enabled: true              # requires commons-pool2 on the classpath (Boot auto-enables)
          max-active: 8              # connections ≈ expected L2 QPS / 15k, round up; usually 8–32
          max-wait: 200ms            # pool-exhaustion wait cap: fail fast into L2 degradation
                                     # instead of blocking business threads
```

**Capacity in three steps**:

1. **L2 load = business QPS × (1 − L1 hit rate)**. Example: 10k QPS at 95% L1 hits → L2 handles
   ~500 QPS (a single connection suffices); at 60% → 4000 QPS (1–2 connections; provision 4–8).
2. Each connection sustains about **15k ops/s** (measured against a local single-threaded Redis);
   derate by RTT across data centers.
3. The hit rate (the first-order variable of L2 load) is driven by **L1 capacity / TTL / access
   skew** — see the CONSISTENCY doc (Chinese) for measured scenarios.

**Note**: pooling solves throughput, not tail latency — keep `max-wait` ≤ command timeout so pool
exhaustion degrades fast (treated as a miss, business never blocks); batch paths (`selectBatchIds`)
use MGET pipelining — one round trip amortizes many keys, needing far fewer connections than
per-key reads.
