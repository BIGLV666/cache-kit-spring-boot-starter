# Boundaries, known limitations & FAQ

## MVP boundaries (0.3.0)

- Only **primary-key-located** queries are cached (single + key-batch `selectBatchIds` /
  `@CachedQuery` returning `List<entity>` whose parameters are ID collections); condition queries
  (by phone, status, …) are **never cached** — the matched set is unknown and cannot be
  invalidated, so they always hit the DB
- No write-through / write-behind: the business code performs the DB write; this component only
  invalidates
- It does not take over the persistence layer: the MyBatis/MP relationship is metadata + AOP only
- **Composite keys are supported since 0.3.3 (annotation paths only)**: multiple `@CacheId` fields
  form the key segment in declaration order joined by ':' (`prefix:v1:v2`) — `@CachedQuery` strict
  derivation (every key field needs a same-named scalar parameter; one missing → bypass), entity
  instance parameters, the `EntityCache` manual handle (`get(List.of(v1, v2))`) and binlog
  invalidation all operate on the joined key. Constraints:
  ① a segment value containing ':' makes the key ambiguous — reads reject it outright and
  invalidation skips it (such a value can never have been cached);
  ② MyBatis-Plus itself does not support composite keys, so the BaseMapper auto-aspects bypass
  composite-key entities entirely (one warning per entity);
  ③ on the binlog side, all PRI columns are aligned to field declaration order via a
  "field name ↔ column name" (camel↔snake) mapping; an unmapped field skips that row's
  invalidation (rate-limited warning, TTL/double-delete as the fallback)
- When the write method runs in an active transaction, invalidation is deferred to **afterCommit**
  (`cache-kit.tx.evict-after-commit`, default on); rollback does not invalidate.
  Semantics note: a same-transaction write-then-read of the same key returns the **pre-commit
  stale value** from the cache (invalidation was deferred) — use the `CacheKit.withDb` bypass when
  same-transaction visibility is required
- In binlog mode the primary-key column positions come from `information_schema`, **cached per
  table for the process lifetime** — after a schema change, restart the process to re-parse
- The binlog × `CacheKeyCustomizer` combination (mostly resolved since 0.3.2): the binlog parse
  thread has no application context, but a customizer that overrides
  `segmentFor(meta, rowData)` can restore the key segment from row data (column name → value), so
  binlog invalidation hits the exact segment-qualified key (with a segment-less fallback);
  **custom segments without a `segmentFor` implementation cannot be restored** — exact
  invalidation for those rows is skipped (the `cache-kit.binlog.derive.skipped` metric plus a
  rate-limited warning; only TTL/double-delete remain). When the row image column count differs
  from the cached information_schema column count (schema drift), the same skip-and-count applies

## Known limitations & notes

- **binlog × transaction timing**: MySQL writes binlog events at **COMMIT time** (they only sit in
  the transaction binlog cache before that), so listeners naturally see post-commit data — there is
  no "invalidate before commit" race; residual read-back-fill races are covered by double-delete +
  TTL
- **null placeholders × INSERT**: an in-app `mapper.insert()` automatically clears the matching
  "confirmed absent" placeholder (new data visible immediately); in binlog mode INSERT row events
  do the same; with neither present, the placeholder is bounded by `null-ttl` (default 30s)
- **Batch-miss reload**: per-ID single-flight guarantees each missing ID loads at most once;
  several **different** batch requests are not merged into one bigger IN query
- **Key type / table-name migration**: old keys become orphans and age out via TTL/L1 bounds; to
  switch immediately, change `cache-kit.key-namespace` to discard the old key space at once
- **Pluggable L1 (0.3.1+)**: the default is CaffeineChannel; an `L1Channel` bean replaces it (it
  must be an in-process local cache — broadcast invalidation only deletes keys on this instance,
  a remote-store implementation would break broadcast semantics). The L2 channel can also be
  replaced by defining a custom `RedisChannel` bean
- **Single-flight waiters share data**: with single-flight merged loads, each waiter receives its
  own decoded instance (mutable objects are never shared); only the rare "entity not serializable"
  fallback path shares the instance
- **Same-prefix entities fail fast (0.3.0+)**: when two entities resolve to the same cache prefix
  (e.g. both annotated `@TableName("user")`), the second registration throws — a shared prefix
  means a shared key space where values overwrite each other and field subsets silently miss data
- **`@CacheInvalidate` strict semantics**: entity instance parameters and entity-collection
  parameters (0.3.0+) resolve keys directly; scalar parameters must be named exactly like the key
  field; **scalar collections (`List<Long>` etc.) are rejected** — an element cannot be proven to
  be a key, guarding against condition values mistaken for keys (MP's deleteByIds path is covered
  by the auto-aspects; no annotation needed)
- **`@CachedQuery` list queries with empty results**: under the strict guard, an empty reload
  result (none of the requested values exist) is treated as misuse and bypassed without caching —
  the request values cannot be proven to be keys; existing rows cache normally. Misuse bypasses
  re-run the original query with all parameters, guaranteeing a complete result (concurrent
  overlapping requests never receive another thread's partial subset), at the cost of one extra
  DB query on the misuse path

## FAQ

**Mapper XML throws `Invalid bound statement` after packaging?**
A MyBatis-Plus path-case issue unrelated to this component but a frequent trap: placing XML under
`resources/Mapper/` (capital M) happens to match `classpath*:/mapper/*.xml` in IDE directory mode
(case-insensitive file systems), but always fails once jarred. Configure
`mybatis-plus.mapper-locations: classpath*:/Mapper/*.xml` explicitly.

**binlog connection fails?**
Check in order: is `log_bin` enabled and `binlog_format=ROW`; is `binlog_row_image=FULL`
(MINIMAL row images miss key columns and per-row invalidation silently drops); does the account
have `REPLICATION SLAVE`; is `server-id` unique against MySQL and other replicas; MySQL 8.4 needs
connector 0.30.0+.

**Broadcast invalidation across multiple Redis-backed instances?**
The broadcast topic lives on a single Redis; every instance must subscribe to the same Redis.
When Redis Cluster shards L2 traffic, converge the invalidation topic onto a dedicated node or
use Redis 7 sharded pub/sub.

**No AOP auto-proxying in the host (aspects do nothing)?**
Normal Spring Boot applications enable it by default. If you assemble tests manually with
`ApplicationContextRunner`, remember to add `AopAutoConfiguration`.
