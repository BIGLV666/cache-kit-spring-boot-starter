# Consistency mechanism and measured performance

## Strong-consistency reads (exemption)

The cache is eventually consistent. For strong-consistency needs, by tier:

| Tier | Usage | When |
|---|---|---|
| Method without annotations | `selectByIdFromDb(Long id)` | A method that must always hit the DB |
| ThreadLocal bypass | `CacheKit.withDb(() -> mapper.selectById(id))` | Decided at the call site |
| SpEL condition | `@CachedQuery(condition = "!forceDb")` | Decided by caller arguments |

**Fields that back strong-consistency decisions (stock, balance) must not be cached** — every
cache scheme (double delete, binlog included) is eventually consistent.

## Consistency mechanism

Writes always follow **DB first, then evict** (standard Cache-Aside order). The delete action =
**DEL L2 → local L1 → broadcast → delayed double delete**. The order matters: L2 must be deleted
first — otherwise a concurrent read that misses L1 in the gap reads the stale value from L2 and
back-fills L1, producing reproducible dirty reads.

The core dirty window is the **back-fill race** (a reader finds an old value, the writer deletes,
the reader back-fills the old value into L1/L2). Delayed double delete shrinks that window;
outside it (slow reads / long GC spanning the interval, dropped double-delete backlog,
invalidations lost during Redis outage) the old value can revive in L2 and keep back-filling L1
on every node — the real ceiling for such scenarios is the **L2 TTL (default 10m + jitter)**, not
seconds. This is an inherent Cache-Aside race that this component does not eliminate with
version numbers / CAS; it relies on the TTL ceiling. For consistency-sensitive fields use the
`CacheKit.withDb` bypass or keep them out of the cache entirely.

## Broadcast channel choice (0.3.1+)

| | `pubsub` (default) | `streams` |
|---|---|---|
| Delivery | fire-and-forget: lost when a subscriber is down or the network glitches; bounded by L1 TTL | consumer-group ACK: survives short instance outages, replays after recovery |
| Redis-side cost | none | one Stream (`MAXLEN ~ streams-maxlen` approximate trim, default 10000) |
| Per-instance state | none | one consumer group (destroyed on graceful shutdown; crashed leftovers reaped periodically) |
| Measured delivery latency | 2 instances × 200 keys, 202ms, all delivered | 3 instances × 500 keys, 157ms, zero loss |

Use the default pubsub for single instances or when second-scale TTL backstop is acceptable; use
streams for multi-instance deployments sensitive to invalidations lost during restart windows.

**Operational boundaries of streams mode (read before K8s autoscaling)**:

- **Cleanup period and worst-case residue**: live instances sweep every 5 minutes; groups are
  destroyed only when the epoch embedded in the group name is older than 30 minutes **and** all
  consumers are idle for more than 10 minutes (healthy consumers idle at most 5s, never swept).
  A crashed instance's group lingers **~40 minutes** worst case. Lingering groups are harmless:
  they do not block stream trimming — just group metadata + PEL references.
- **New instances vs old groups**: group names embed `process epoch + UUID`; a new instance always
  creates its own group (starting at `$`, the latest position — a fresh L1 needs no history), so
  there is zero collision with old groups.
- **Real loss surface**: `MAXLEN ~ streams-maxlen` trims the whole stream — when an instance's
  group lags behind by more than that, the lagging part is permanently lost for it (equivalent
  to pubsub message loss; L1 TTL backstop applies). When offline-duration × write rate exceeds
  `streams-maxlen`, streams degrades to pubsub semantics.
- **Frequent scaling**: each replica generation creates one group; the group count ≈ generations
  within the 40-minute window × replica count. Metadata is tiny, but monitor it
  (`XINFO GROUPS <topic>`) or baseline Redis memory. With extremely frequent rollouts (minute
  level) and many instances, prefer pubsub + a shorter L1 TTL.

## Stress comparison: cache-kit vs Spring Cache vs JetCache (0.3.1+)

`CacheStressComparisonIntegrationTest` (Testcontainers redis:7, 16 logical cores).
Each implementation runs in its production-typical configuration: Spring Cache = RedisCacheManager
+ Lettuce pool (8) + JSON serialization; JetCache = RedisLettuceCache (multi-level = Caffeine +
Redis). Numbers vary by environment; **magnitudes and relative relationships** are the point.

Terminology: **op = one cache read call** (cache-kit `load` / Spring Cache `get` / JetCache
`get`); ops/s is the multi-threaded aggregate.

### Scenario 1: concurrent read throughput ceiling (threads swept from cores/2 to 4×cores, 0.3s warmup + 2s timing per level, best level reported)

**Workload (read this before the numbers)**: value is a 2-field entity (JSON ~30B); 1000 keys
rotated uniformly, no skew, all fitting in L1 (10k capacity). The table is the **pure-hit
physical ceiling** — what production reaches is governed by the hit rate (throughput collapses
when the working set overflows L1; see scenario 1b).

| Implementation | Ceiling | Best concurrency | p99 at best |
|---|---|---|---|
| cache-kit three-tier (L1 hit) | **~14.5M ops/s** | 32–64 thread plateau | ~1µs |
| JetCache multi-level (Caffeine+Redis) | ~6.9M ops/s | 32 threads | ~190µs |
| Spring Cache (Redis only, pool 8) | ~28k ops/s | 64 threads | ~4.8ms |
| JetCache remote only | ~28k ops/s | 64 threads | ~5.0ms |
| cache-kit L2 only (single connection) | ~27k ops/s | 64 threads | ~4.9ms |
| Raw Redis GET (pool 8) | ~28k ops/s | 64 threads | ~4.8ms |

Implementations with a local tier reach **~550×** the ceiling of remote-only ones; remote-only
implementations are all bounded by "one Redis round trip + connection pool" and are close to each
other. The value of a three-tier architecture is the **local tier absorbing nearly all round
trips**, not a faster remote read.

### Scenario 1b: working set exceeding L1 (L1 capacity 100 / 1000 keys, 32 threads)

| Access distribution | L1 hit rate | Aggregate throughput |
|---|---|---|
| Uniform | 6.8% | ~27k ops/s (≈ remote ceiling) |
| Zipf hot spots | 30.2% | ~36k ops/s |

Once the working set overflows L1, throughput falls from millions straight back to the remote
tier — **effective throughput = hit rate × hit cost + miss rate × remote cost**; the hit rate
(determined by capacity, TTL, and access skew) is the first variable of production capacity. The
14.5M figure only holds while the hot data fits in L1.

**Attribution notes (do not over-read scenario 1)**:
- **Real cost at low concurrency**: cache-kit deserializes JSON into a **fresh instance** on every
  hit (guarding against shared-mutable-object pollution); ~430ns per hit (mean), vs JetCache's
  local get at ~150ns — **at low concurrency cache-kit is slower**, stated plainly. Both are far
  below 1µs, imperceptible to business code; the cost only matters when hit QPS approaches
  millions, and it buys safe-to-mutate return values.
- Single-thread attribution (`CacheHitPathAttributionTest`, medians): raw Caffeine get ~100ns,
  Jackson readValue ~200ns, cache-kit full hit path ~400ns (mean 494ns), JetCache local get
  ~100ns (mean 166ns).
- cache-kit's multi-threaded aggregate superiority is a scaling difference: JetCache multi-level
  plateaus at 8 threads (p99 degrading from 35µs to 427µs), while cache-kit scales from 8 to 64
  threads. Local-hit tiers are all in the sub-µs-to-µs band — **the difference is not a
  selection criterion**.
- **The mild 64-thread dip is run-to-run noise, not a hard limit**: the 32 vs 64 difference is
  ~3% (14.86M vs 14.43M, either can win), p99 stays ~1µs. cache-kit allocates ~880B per hit
  (JetCache ~306B); 14.5M ops/s ≈ 12.7 GB/s allocation — young GCs are frequent but short (p99
  unaffected), plus diminishing returns from scheduling and memory bandwidth. High-concurrency
  use is fine; 32 threads is not a ceiling.

**Tail-latency degradation mechanism (why JetCache multi-level p99 worsens with concurrency)**:
1. The universal rule: as concurrency pushes utilization toward saturation, wait times amplify
   non-linearly — every implementation has this, only the magnitude differs.
2. JetCache's local cache uses a custom Caffeine `Expiry` (to support `expireAfterAccess`
   semantics) — Caffeine calls `expireAfterRead` **on every read** (`System.currentTimeMillis()`
   plus a continuously-shifting remaining-time computation); the drifting return value makes
   Caffeine reschedule hot entries' timers frequently (shared writes). Isolated experiment
   (`CacheExpiryTailTest`, 16 threads on bare Caffeine): the variable Expiry runs ~25% lower
   throughput than fixed `expireAfterWrite`, with 300ms-class max spikes.
3. Plus per-read allocations of the multi-level wrapper (CacheGetResult / two-tier iteration /
   holder checks), contention amplifies under high concurrency.
4. cache-kit as the counter-example: same Caffeine, same 1000 keys, same threads, a fixed
   `expireAfterWrite` pure-read path — p99 stable at ~1µs, despite allocating *more* (Jackson
   deserialization per hit). Allocation/GC is therefore not the dominant factor; **the per-read
   callback and its entry rescheduling are**.

### Scenario 2: cache stampede (200 concurrent threads on one cold key, fake 50ms DB)

| Implementation | DB loads | Wall time |
|---|---|---|
| cache-kit (single-flight built in) | **1** | ~70ms |
| Spring Cache (default, no mutex) | 200 | ~175ms |
| JetCache (default) | 200 | ~143ms |
| JetCache (penetrationProtect=true) | **1** | ~66ms |

cache-kit's single-flight and null placeholders are on **by default**; JetCache needs
penetrationProtect explicitly enabled, Spring Cache needs your own locking or a locking writer —
otherwise loads on a stampede = concurrency.

### Scenario 2b: cold-start load amplification (64 concurrent × 100 cold keys, each thread requests all keys sequentially, fake 20ms DB)

| Implementation | Total DB loads | Amplification | Wall time |
|---|---|---|---|
| cache-kit | **100** (1 per key) | 1× | ~3.1s |
| JetCache (penetrationProtect=true) | **100** | 1× | ~2.8s |
| Spring Cache (default) | 6,287 | ~63× | ~3.0s |
| JetCache (default) | 6,400 | ~64× | ~3.0s |

The first traffic wave after a restart/cache flush is the load peak: without single-flight
semantics the DB pressure ≈ concurrency × keys; single-flight collapses it to ≈ keys.

### Scenario 3: single-key invalidation propagation (instance A evicts → instance B's L1 cleared, 100 rounds)

| Channel | p50 | p99 | max |
|---|---|---|---|
| cache-kit pubsub | ~8.2ms | ~45ms | ~45ms |
| cache-kit streams | ~8.8ms | ~22ms | ~22ms |

Cross-instance invalidation completes in **tens of milliseconds**; JetCache's cross-instance local
cache invalidation requires its CacheManager broadcast setup (not covered here), and Spring Cache
has no local tier — no such problem by construction, but every read pays a Redis round trip (see
scenario 1).

Reproduce: `mvn test -Dtest=CacheStressComparisonIntegrationTest` (plus the single-threaded
micro-benchmark `CacheBenchmarkIntegrationTest` for an order-of-magnitude reference).

## Production measurements (PaperWise host project)

Single machine, 4–10 instances + Docker Redis/MySQL, Java 17:

| Scenario | Measured |
|---|---|
| Cache-hit reads | microsecond-level; zero growth in MySQL InnoDB row reads across 20,249 HTTP requests |
| In-app write invalidation | ~1–2ms per write (DEL L2 + broadcast + double-delete scheduling); 46k broadcasts with zero loss |
| Broadcast eventual consistency | 10 instances, 716 writes/s concurrent, 46k broadcasts: 100% of instances passed the final version check |
| Dirty-read window (in-app writes) | max 119ms–505ms (including single-JVM scheduling noise), far below the 30s L1 TTL design ceiling |
| Dirty-read window (binlog, out-of-band writes) | p99 14.2ms, zero missed invalidations over 300 out-of-band writes |
| External HTTP end-to-end | ~1700 req/s single instance at saturation; latency baseline dominated by auth Redis round trips, cache hits are microsecond-level |
| Load amplification | ~3 DB loads per write on hot keys (broadcast evict + double-delete evict + in-flight re-read) |
| Cross-instance stampede amplification (0.3.0 quantified) | single-flight works per JVM: N instances missing the same cold key = N DB loads (1 each). For cold starts size the DB at `N × new keys per second`; add an external distributed lock if needed — deliberately not built in (lock cost/deadlock surface > cold-start amplification) |