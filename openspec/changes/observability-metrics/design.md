## Context

The Web UI (axum, port = pgwire + 1000) already serves `/api/v1/*` from a
`WebState` holding the `NodeRegistry`, the `AdmissionController` and a
`QueryTracker`. The tracker cannot back query counters: pgwire statements
never enter it, and the coordinator adds entries that never reach a terminal
state.

## Decisions

### D1 — Observe at `execute_query_with_limit`

Both protocols reach the engine through this one function (pgwire directly,
Trino via `trino/engine.rs`). Metadata interception (`pg_catalog`, `SET`) runs
before it and is not counted. A `QueryObservation` guard is started there and
finished with the result. Its `Drop` records `cancelled`, which covers the
statement future being dropped by a cancel, `statement_timeout` or a client
disconnect without threading a cancel signal through.

### D2 — Hand-written exposition, no crate

There are three counters, three histograms and a handful of scrape-time
gauges. The text format is simple, and `prometheus` / `prometheus-client`
would add a registry and a dependency (plus a `deny.toml` review) for little
gain. The writer escapes label values and follows the cumulative-bucket rule.
The Tier 2 run validates it against a real Prometheus.

### D3 — Gauges read at scrape time

The admission counters, memory pool and worker liveness already exist as
state, so the handler reads them on each scrape instead of mirroring them
into atomics that could drift.

### D4 — Slow-query threshold is a runtime knob

`[observability] slow_query_threshold_ms`, overridable by
`ARNEB_SLOW_QUERY_THRESHOLD_MS`, is resolved and validated in `config.rs` and
logged on `arneb::config` at startup, like the other sections and following
`docs/guide/configuration.md`. An invalid value fails startup. The default is 10 s, which is above
every SF10 TPC-H query on the bench cluster, so the log stays quiet unless
something is wrong.

## Risks / Trade-offs

- **Process-wide statics** make unit tests share counters, so the tests
  assert deltas or lower bounds.
- **No per-worker memory:** workers have no HTTP listener. Exporting their
  pools needs either a listener or heartbeat-carried stats, which is a
  follow-up.
