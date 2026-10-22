# observability-metrics — Tier 2 acceptance (2026-10-21)

Local bench cluster (coordinator + 2 workers, SF10) plus `prom/prometheus:v3.5.0`
scraping `arneb-coord:6432/metrics` every 5 s and `grafana/grafana:12.1.1`
(`docker/observability/`). The coordinator ran with `ARNEB_SLOW_QUERY_THRESHOLD_MS=3000`.

Workload after a fresh start: one `SELECT 1` readiness probe, TPC-H q01–q22
once each, one `SELECT nope FROM nation` (missing column), and q09 under
`SET statement_timeout = '200ms'`. Then `docker kill arneb-worker-2`, wait 45 s,
and restart it.

Read back from the Prometheus API:

| Query | Value | Expected |
|---|---|---|
| `arneb_queries_total{outcome="finished"}` | 23 | 22 TPC-H + 1 probe |
| `arneb_queries_total{outcome="failed"}` | 1 | the missing column |
| `arneb_queries_total{outcome="cancelled"}` | 1 | the statement timeout |
| `arneb_query_duration_seconds_count` (each outcome) | 23 / 1 / 1 | equal to the counters |
| `arneb_queries_running` | 0 (max 1 over the run) | sequential client |
| `max_over_time(arneb_workers{state="dead"}[15m])` | 1 | the kill |
| `min_over_time(arneb_worker_up{worker_id="worker-2"}[15m])` | 0 | the kill |
| `arneb_workers{state="alive"}` now | 2 | worker-2 rejoined |
| `arneb_memory_pool_limit_bytes` | 6442450944 | coord.toml `spill_budget_bytes` |
| p95 finished, `histogram_quantile` over 15 m | 4.54 s | |

- Slow-query log: 4 `arneb::slow_query` events, all ≥ 3000 ms (`duration_ms=4397,
  3249, 4279, 3535`, all `outcome="finished"`). Nothing was logged for faster
  queries or for the timed-out one.
- Grafana: `GET /api/dashboards/uid/arneb` returned 200 with 10 panels.
