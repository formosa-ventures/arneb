## Why

Phase 3 M3 / P5 (`docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`):
an operator running Arneb for a team has to see what it is doing. Today the
only views are the Web UI's JSON API and log lines. There is no metrics
endpoint to scrape and no record of slow queries. P5 lists metrics, query
history, a slow-query log and a Grafana dashboard. This change delivers the
metrics, the slow-query log and the dashboard. Bounded query history and
`system.runtime.queries` follow in their own change: they need the
`QueryTracker` to record every pgwire statement and to evict old entries, which
it does today for neither.

## What Changes

- **`GET /metrics`** on the Web UI port (coordinator / standalone), in the
  Prometheus text format:
  - `arneb_queries_total{outcome=finished|failed|cancelled}`
  - `arneb_query_duration_seconds` histogram by outcome
  - `arneb_queries_running`
  - `arneb_admission_running` / `arneb_admission_queued`
  - `arneb_memory_pool_{reserved,peak,limit}_bytes`
  - `arneb_workers{state}`, `arneb_worker_up{worker_id}` and
    `arneb_worker_heartbeat_age_seconds{worker_id}`
  - `arneb_uptime_seconds` and `arneb_build_info`

  Queries are observed once, in `execute_query_with_limit`, which every
  pgwire and Trino statement goes through. A statement dropped mid-way
  (cancel, `statement_timeout`, client gone) counts as cancelled.
- **Slow-query log:** queries that take at least
  `[observability] slow_query_threshold_ms` (`ARNEB_SLOW_QUERY_THRESHOLD_MS`;
  default 10000; `0` turns it off) are
  logged on the `arneb::slow_query` target with their duration, outcome,
  rows, error and SQL (truncated to 2000 chars).
- **Grafana:** `docker/observability/` holds a Prometheus scrape config, a
  provisioned Grafana with an "Arneb" dashboard, and a compose overlay over
  the bench cluster.
- The metrics are hand-written atomics in `arneb-common`. No new dependency.

## Capabilities

### New Capabilities

- `observability-metrics`: the `/metrics` contract (series, labels,
  semantics of each outcome), the slow-query log, and the bundled dashboard.

## Impact

- `crates/common/src/metrics.rs` (new), `crates/protocol/src/handler.rs`
  (observation around `execute_query_with_limit`), `crates/server/src/web/`
  (route and handler, plus `memory_pool` / `memory_limit_bytes` in
  `WebState`), and `crates/scheduler` (`NodeRegistry::all_workers`).
- `docker/observability/`, docs (`docs/guide/observability.md`).
- Workers have no Web UI, so worker-side memory is not exported yet. The
  coordinator reports worker liveness.
