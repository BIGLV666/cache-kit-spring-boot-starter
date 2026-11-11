# Consistency mechanism

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
- **Group-loss self-healing (0.3.3+)**: when Redis restarts/FLUSHALLs, a failover points at an
  empty instance, or the Stream key is evicted, the group disappears. On `NOGROUP` the consumer
  recreates it in place: a first start still reads from `$`, while a lost runtime group replays
  the retained Stream from `0-0` (bounded by `streams-maxlen`) to recover messages published
  during the gap; replay is idempotent DEL and already-expired messages are bounded by L1 TTL.
  Counted by `cache-kit.broadcast.streams.group.recreated`.
- **Real loss surface**: `MAXLEN ~ streams-maxlen` trims the whole stream — when an instance's
  group lags behind by more than that, the lagging part is permanently lost for it (equivalent
  to pubsub message loss; L1 TTL backstop applies). When offline-duration × write rate exceeds
  `streams-maxlen`, streams degrades to pubsub semantics.
- **Frequent scaling**: each replica generation creates one group; the group count ≈ generations
  within the 40-minute window × replica count. Metadata is tiny, but monitor it
  (`XINFO GROUPS <topic>`) or baseline Redis memory. With extremely frequent rollouts (minute
  level) and many instances, prefer pubsub + a shorter L1 TTL.

## Performance profile (summary)

- Local L1 hits are in the sub-microsecond to microsecond range; implementations with a local tier have a far higher throughput ceiling than remote-only (Redis round-trip + pool) implementations.
- Once the working set exceeds L1, throughput falls back to the remote range; effective capacity is dominated by the hit rate, which is determined by L1 capacity, TTL and access skew.
- Single-flight collapses reloads to one per key; without it, a stampede causes roughly one reload per concurrent caller.
- Cross-instance invalidation propagation completes in tens of milliseconds; pubsub may drop messages and streams have consumer-group lag and MAXLEN boundaries, all bounded by the L1 TTL.
- The full benchmark methodology, raw data, attribution notes and PaperWise production measurements are archived in the maintainer documentation and are not shipped with the component repository.
