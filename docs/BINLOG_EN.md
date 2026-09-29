# Direct binlog invalidation (0.2.0+, covers "writes that bypass your app")

Broadcast invalidation presumes writes go through your application. When a DBA edits a table or
another service writes to it, cache-kit cannot see the change — the binlog module fills that gap:
it connects to MySQL's binlog via the replica protocol (ROW format) and, for any source of writes
to cached entity tables, extracts primary keys per row and triggers invalidation.

**No Canal server needed**: add `mysql-binlog-connector-java` (declared optional by this starter;
services enabling binlog must add it themselves) and flip the switch. **Fool-proofing**: with
`cache-kit.binlog.enabled=true` but the connector missing, startup fails immediately with a hint —
it never silently skips (0.3.1+). MySQL requires `log_bin`, `binlog_format=ROW`,
`binlog_row_image=FULL` (MINIMAL drops key columns from row images and per-row invalidation is
silently lost), and an account with `REPLICATION SLAVE` privilege.

```yaml
cache-kit:
  binlog:
    enabled: true        # off by default
    # host/port/database default to spring.datasource.url; account defaults to the datasource account
    # server-id defaults to a random value; set it explicitly when running multiple replicas
    server-id: 18365     # must be unique per MySQL server
```

Note: MySQL 8.4 removed `SHOW MASTER STATUS`; connector 0.30.0+ is required (this starter
defaults to 0.31.0).

## Position mode: file/position vs GTID

**Default (file/position)**: the connector advances the binlog position as it streams, so
reconnects resume from the last position natively — events inside a disconnect window are
replayed, and replayed invalidations are idempotent DELs.

If the recorded binlog file was purged by the server during a long outage (expiry /
`PURGE BINARY LOGS`), reconnects fail instantly in a loop — the lifecycle detects
"connected-then-dropped instantly with no events" for 5 consecutive rounds, resets to the latest
position, and warns (invalidations lost inside that window are bounded by the L2 TTL).

**GTID mode (0.3.2+)** — recommended for production HA setups:

```yaml
cache-kit:
  binlog:
    gtid-enabled: true
    # gtid-set: optional; when omitted, the start point is @@global.gtid_executed at startup
```

- A GTID set replaces file/position for locating the stream: **positions stay continuous across
  primary/replica failovers** (file/position becomes invalid after a failover).
- The connector's built-in `gtidSetFallbackToPurged` automatically falls back to the latest GTID
  set when the recorded position was purged (complementing the manual 5-round reset of
  file/position mode).
- When `gtid-set` is not configured explicitly, startup queries `@@global.gtid_executed` as the
  start point — the connector's default is to replay from the earliest available event, which
  would cause a startup invalidation storm. Startup fails fast when the host has no DataSource to
  query and no explicit `gtid-set` is configured, or when MySQL has GTID disabled.

Measured: out-of-band write → invalidation → next read returns the new value with end-to-end
propagation latency **avg 10.3ms / p99 14.2ms / max 53.7ms** (300 out-of-band writes, zero
misses, including 5ms polling granularity).

## Test coverage

- `BinlogInvalidationIntegrationTest`: real MySQL (pre-started container in CI), end-to-end —
  out-of-band writes must invalidate
- `BinlogReconnectIntegrationTest`: Testcontainers mysql:8.4 + redis:7 — reconnect replay and
  automatic position reset after PURGE (auto-skips without Docker)
- `BinlogGtidIntegrationTest`: GTID mode end-to-end (mysql:8.4 with `--gtid-mode=ON`) —
  out-of-band invalidation works and the GTID start point is set; fail-fast path without a
  DataSource
- `BinlogLifecycleTest`: unit paths of the position-purge fallback

## Troubleshooting

Check in order: MySQL has `log_bin` on and `binlog_format=ROW`; `binlog_row_image` is `FULL`
(MINIMAL drops key columns and per-row invalidation is silently lost); the account has
`REPLICATION SLAVE`; `server-id` does not conflict with MySQL or other replicas; MySQL 8.4 needs
connector 0.30.0+; GTID mode additionally needs `gtid_mode=ON`.