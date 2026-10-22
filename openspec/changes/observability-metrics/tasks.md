## 1. Metrics

- [x] 1.1 `arneb_common::metrics`: outcome counters, duration histogram, running gauge, `QueryObservation` (drop = cancelled), text renderer with label escaping; tests
- [x] 1.2 Observe in `execute_query_with_limit`; protocol test that executed statements are counted
- [x] 1.3 `GET /metrics` with scrape-time admission / memory pool / worker gauges; handler test

## 2. Slow-query log

- [x] 2.1 `[observability] slow_query_threshold_ms` / `ARNEB_SLOW_QUERY_THRESHOLD_MS` (default 10000, 0 = off), validated and logged at startup; `arneb::slow_query` event with duration, outcome, rows / error, SQL

## 3. Dashboard

- [x] 3.1 `docker/observability/`: Prometheus config, Grafana provisioning, "Arneb" dashboard, compose overlay

## 4. Tier 2

- [x] 4.1 Bench cluster + real Prometheus: counters, histogram and workers match the run; failure / timeout outcomes; slow-query log; lost worker; Grafana dashboard loads

## 5. Docs

- [x] 5.1 `docs/guide/observability.md`; link from the configuration guide
