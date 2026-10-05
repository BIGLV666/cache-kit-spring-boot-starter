# Resilience design

- **L2 failure degradation**: when Redis is down, lookups are treated as misses (rate-limited
  warning) and reads/writes never fail because of L2 — L1/DB carry the traffic.
  Note: degradation is bounded by "a single Redis call returning an error" — with a long command
  timeout (Lettuce default 60s), Redis jitter first blocks the reader thread until the timeout.
  Configure `spring.data.redis.timeout: 2s` (a startup warning fires above 5s)
- **L2 circuit breaker (0.3.2+)**: after `cache-kit.l2.circuit-breaker.failure-threshold`
  consecutive failures (default 20) all L2 calls short-circuit for `open-duration` (default 10s) —
  zero-overhead degradation (no per-call Redis attempts, no per-call block-until-timeout); a single
  probe is allowed on expiry (success closes, failure re-opens). Openings count as
  `cache-kit.l2.circuit.opened`, state gauge `cache-kit.l2.circuit.state`
  (0=CLOSED/1=HALF_OPEN/2=OPEN).
  Note: the breaker cannot remove the "block until command timeout" wait of the first N failures
  (Lettuce default 60s) — the `spring.data.redis.timeout: 2s` advice still stands; the breaker
  saves the subsequent flood of retries, not the first wait
- **Measured bounds of the invalidation-loss window (chaos-test finding, 0.3.2+)**: Lettuce's
  default `DisconnectedBehavior.DEFAULT` buffers commands issued during a disconnect and flushes
  them after reconnect — a "DEL landing exactly in the flapping window" is often self-healed by
  Lettuce, so the real loss window is frequently better than the worst case promised below;
  `REJECT_COMMANDS` (immediate rejection) exhibits the promised bound. Both paths share the same
  eventual-consistency semantics (bounded by L2 TTL)
- **L2 delete-failure retry (0.3.0+)**: a DEL landing in a Redis flapping window leaves the stale
  value in L2 — the invalidation path automatically retries at the double-delete delay
  (up to 3 times), further shrinking the loss window; exhausted retries count as
  `cache-kit.evict.retries.exhausted`
- **Serialization failure never loses data**: when an entity holds structures Jackson cannot
  serialize (self-references etc.), the result is still returned — just not cached; a cached value
  that fails to deserialize (entity drift) is treated as a miss and overwritten by the next
  successful load — no invalidation storm
- **L2 per-value cap (0.3.3+)**: serialized JSON above `cache-kit.l2.max-value-kb` (default 512KB)
  skips BOTH cache levels (a DB hit on every read — same semantics as "serialization failure
  never caches"), keeping a few large fields (long text / big JSON columns) from blowing up Redis
  memory and the network; counted by `cache-kit.l2.value.oversized` with a rate-limited warning,
  0 disables the cap
- **L2 compression (0.3.3+, off by default)**: values at or above `compression-min-kb` are stored
  in L2 with a "gz:" prefix + Base64; decompression failure (corrupted data / marker mismatch) is
  treated as a miss and overwritten by the reload — it never throws; L1 always stores plain text,
  and already-compressed values stay readable after compression is turned off (the reader detects
  the prefix regardless of configuration)
- **L1 refresh-ahead (0.3.3+, off by default)**: inside the `l1.refresh-ahead` window a hit
  schedules a background refresh (evict the local key, then a full read-through) — reads always
  see unexpired values. This deliberately does NOT serve stale values past expiry (classic SWR),
  so the "L1 TTL = dirty-read upper bound" promise is unchanged. Refresh tasks merge with
  concurrent readers through single-flight (anti-stampede: a stress run of 32 threads × 2s on a
  hot key produced a single-digit number of DB loads); a failed task leaves the key to expire
  naturally (`l1.refreshahead.failed`), and a full queue drops the task until the next read
  (`l1.refreshahead.dropped`) — neither affects correctness
- **Double-delete backlog protection**: when the delayed double-delete backlog exceeds 10,000
  tasks, new tasks are skipped with a rate-limited warning (staleness bounded by TTL), counted as
  `cache-kit.doubledelete.skipped`
- **Broadcast source validation**: subscribers only clear keys whose prefix matches a known entity,
  preventing arbitrary clients from flushing the cache (cache-DoS surface)
- **No polymorphic Jackson deserialization**: default typing is not enabled, so there is no
  deserialization gadget surface
- **SPI exception isolation**: an exception from `CacheKeyCustomizer.segment()` degrades to "no
  custom segment" with a rate-limited warning and never interrupts the read/write/invalidation
  main flow
