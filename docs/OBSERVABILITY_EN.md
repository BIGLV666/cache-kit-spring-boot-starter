# Observability (Micrometer, auto-configured)

When micrometer-core is on the classpath (bundled with spring-boot-starter-actuator), all
`cache-kit.*` counters register automatically — zero configuration. Alternatively, provide your
own `CacheMetricsListener` bean to take over.

## Metric reference

| Metric | Meaning |
|---|---|
| `cache-kit.l1.requests{result=hit\|miss}` | L1 hit rate |
| `cache-kit.l2.requests{result=hit\|miss}` | L2 hit rate |
| `cache-kit.db.loads` | DB loads (batches counted by ID) |
| `cache-kit.null.placeholders` | null placeholder writes (penetration protection triggered) |
| `cache-kit.evict.keys` | invalidated keys (including the second double-delete pass) |
| `cache-kit.broadcast.sent` / `received{applied=true\|false}` | broadcast send/receive — **sent consistently above received indicates broadcast loss** (bounded by L1 TTL) |
| `cache-kit.l2.fallbacks{op}` | L2 degradations (op=get/put/multiGet/evictAll); sustained growth means Redis is unhealthy |
| `cache-kit.evict.retries.exhausted` | L2 delete retries exhausted — **invalidation lost** (stale value remains in L2 until TTL); alert recommended |
| `cache-kit.doubledelete.skipped` | double-delete backlog skips (staleness bounded by TTL); sustained growth means write pressure exceeds scheduler capacity |
| `cache-kit.binlog.position.resets` | binlog position reset — **invalidations lost during the disconnect window** (fires when the server purged the position); alert recommended |
| `cache-kit.l1.hit.rate` / `cache-kit.l2.hit.rate` | hit-rate gauges (hit/(hit+miss), computed live from counters) |
| `cache-kit.invalidation.delay` (Timer, p50/p99) | propagation delay from the MySQL binlog event timestamp to local invalidation application; **includes clock skew between the two hosts — trend only** |
| `cache-kit.broadcast.streams.lag.seconds` (Gauge) | streams-mode consumer-group lag of this instance (latest invalidation event − last consumed event); sustained >0 means the consumer cannot keep up or is disconnected |
| `cache-kit.l2.circuit.opened` | L2 circuit breaker openings (0.3.2+); frequent growth means persistent Redis jitter — check network and server |
| `cache-kit.l2.circuit.state` (Gauge) | current breaker state: 0=CLOSED 1=HALF_OPEN 2=OPEN (0.3.2+) |
| `cache-kit.binlog.derive.skipped` | rows whose binlog key-segment restoration was skipped (0.3.2+, custom segment not derivable from row data); sustained growth means the customizer should implement `segmentFor` |
| `cache-kit.l1.refreshahead.triggered` | L1 refresh-ahead submissions (0.3.3+, with `l1.refresh-ahead` on); rate ≈ hot keys × 1/TTL |
| `cache-kit.l1.refreshahead.dropped` | refresh tasks dropped because the queue was full (0.3.3+); the next read re-triggers, so this is only refresh delay — sustained growth means more refresh threads are needed |
| `cache-kit.l1.refreshahead.failed` | refresh task failures (0.3.3+); the key simply lives out its TTL, no correctness impact |
| `cache-kit.l2.value.oversized` | values above `l2.max-value-kb` skipped at BOTH levels (0.3.3+); sustained growth means large-field entities keep bypassing the cache (DB hit on every read) |
| `cache-kit.l2.value.compressed` | L2 values written compressed (0.3.3+, with `l2.compression-enabled` on) |

## Alerting advice

`evict.retries.exhausted`, `doubledelete.skipped` and `binlog.position.resets` correspond to real
data loss/staleness windows — sustained growth should trigger investigation. `l2.fallbacks{op}` is
the direct Redis health signal; `l2.circuit.state` frequently leaving 0 means persistent Redis
jitter; sustained `binlog.derive.skipped` growth means custom key segments cannot be derived from
row data; sustained `l2.value.oversized` growth means large-field entities frequently bypass the
cache (a DB hit on every read) — shrink the entity or raise the cap.

## Ops endpoint (/actuator/cachekit, 0.3.2+)

With spring-boot-actuator on the classpath the read-only endpoint `/actuator/cachekit` registers
automatically (exposure is governed by the host's
`management.endpoints.web.exposure.include`):

- **l1**: estimated entry count, TTL, capacity bounds;
- **l2**: TTL, null-placeholder TTL, breaker enabled flag and state (CLOSED/HALF_OPEN/OPEN/DISABLED);
- **doubleDelete**: current backlog / cap (10,000);
- **streams** (streams mode): consumer group name, last measured lag in seconds (−1 = not yet measured);
- **entities**: per-prefix registered metadata (TTL, entity type) and cumulative stats —
  L1/L2 hits and hit rates, DB loads, null placeholders, invalidations (hit rate = hit/(hit+miss),
  −1 = no requests yet).

Security boundary: outputs prefixes, counters and states only — **no cached keys or values**; no
binlog credentials or other sensitive configuration.

## Grafana dashboard

Template: [`grafana-dashboard.json`](grafana-dashboard.json) (import and select the Prometheus
data source — hit rates, request/reload rates, invalidation-loss alerts, L2 degradations,
propagation delay, refresh-ahead and L2 value-policy panels work out of the box).
