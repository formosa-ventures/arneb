# observability-metrics Specification

## Purpose
TBD - created by archiving change observability-metrics. Update Purpose after archive.
## Requirements
### Requirement: Prometheus metrics endpoint
The coordinator and a standalone server SHALL serve `GET /metrics` on the Web
UI port in the Prometheus text format (0.0.4). The response SHALL include:
- `arneb_queries_total` with an `outcome` label (`finished`, `failed`,
  `cancelled`)
- `arneb_query_duration_seconds` as a histogram by `outcome`
- `arneb_queries_running`
- `arneb_admission_running` and `arneb_admission_queued` when admission
  control is on
- `arneb_memory_pool_reserved_bytes` and `arneb_memory_pool_peak_bytes`, and
  `arneb_memory_pool_limit_bytes` when the pool is bounded
- `arneb_workers` with a `state` label (`alive`, `dead`)
- `arneb_worker_up` and `arneb_worker_heartbeat_age_seconds` per `worker_id`
- `arneb_uptime_seconds` and `arneb_build_info`

#### Scenario: Scraped by Prometheus
- **WHEN** a Prometheus server scrapes a bench cluster that ran the 22 TPC-H queries
- **THEN** `arneb_queries_total{outcome="finished"}` increased by 22, the finished histogram count equals it, and `arneb_workers{state="alive"}` is 2

#### Scenario: A lost worker is visible
- **WHEN** a worker stops heartbeating for longer than the heartbeat timeout
- **THEN** `arneb_workers{state="dead"}` is 1 and `arneb_worker_up` for that worker is 0

### Requirement: Query outcomes
The server SHALL count exactly once every statement that reaches query
execution through pgwire or the Trino protocol. A statement that returns a result
SHALL count as `finished`, one that returns an error as `failed`, and one
dropped before it ends (cancel, `statement_timeout`, client disconnect) as
`cancelled`.

#### Scenario: Failure and timeout
- **WHEN** one statement references a missing column and another exceeds its `statement_timeout`
- **THEN** `failed` and `cancelled` each increase by one

### Requirement: Slow-query log
The server SHALL log a statement whose duration is at least
`[observability] slow_query_threshold_ms` milliseconds
(`ARNEB_SLOW_QUERY_THRESHOLD_MS`; default 10000; `0` disables the log) at
`warn` on the `arneb::slow_query` tracing target, with its
duration, outcome, row count or error, and SQL text truncated to 2000 characters. The server SHALL reject an
invalid threshold at startup and SHALL log the effective one.

#### Scenario: Threshold override
- **WHEN** the coordinator runs with `ARNEB_SLOW_QUERY_THRESHOLD_MS=3000` and executes queries slower than 3 s
- **THEN** each of them produces one `arneb::slow_query` event, and faster queries produce none

### Requirement: Bundled dashboard
`docker/observability/` SHALL provide a Prometheus scrape configuration and a
provisioned Grafana dashboard. The dashboard SHALL show running and queued
queries, queries per second by outcome, latency quantiles, the memory pool,
and worker liveness over these metrics.

#### Scenario: Dashboard loads
- **WHEN** the observability overlay is started next to the bench cluster
- **THEN** Grafana serves the dashboard with uid `arneb`, and its latency quantile query returns data

