# Observability

## Metrics

The coordinator, or a standalone server, serves Prometheus metrics at
`GET /metrics` on the Web UI port (the pgwire port + 1000, e.g. `6432`):

```bash
curl -s http://127.0.0.1:6432/metrics
```

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `arneb_queries_total` | counter | `outcome` | Statements that ended: `finished`, `failed`, or `cancelled` (cancel, `statement_timeout`, or the client left) |
| `arneb_query_duration_seconds` | histogram | `outcome` | Statement wall time, from parse to the last batch; time spent waiting in the admission queue is not included |
| `arneb_queries_running` | gauge | | Statements executing now, whether or not admission control is on |
| `arneb_admission_running` / `arneb_admission_queued` | gauge | | Admission slots in use and the queue length, when admission control is on |
| `arneb_memory_pool_reserved_bytes` / `_peak_bytes` / `_limit_bytes` | gauge | | This node's memory pool (the limit only appears when the pool is bounded) |
| `arneb_workers` | gauge | `state` | Registered workers, `alive` or `dead` |
| `arneb_worker_up` | gauge | `worker_id` | `1` while the worker heartbeats within the timeout |
| `arneb_worker_heartbeat_age_seconds` | gauge | `worker_id` | Seconds since the worker's last heartbeat |
| `arneb_uptime_seconds`, `arneb_build_info` | gauge | `version` | Process uptime and version |
| `arneb_exchange_stream_breaks_total` | counter | `outcome` | Exchange streams this node read that broke mid-stream on a transport error: `resumed` (broadcast output, read again from the first missing row) or `failed` |
| `arneb_exchange_streams_dropped_total` | counter | | Exchange streams this node served that were dropped before their end |
| `arneb_query_retries_total` | counter | | Distributed queries re-run after one of their workers was lost ([query retry](/guide/distributed#query-retry)) |
| `arneb_pg_catalog_fallbacks_total` | counter | | pgwire `pg_catalog` statements the engine could not run, answered with a fixed result that may not match the query; each one is also logged at `warn` on `arneb::protocol` with its SQL ([catalog introspection](/guide/clients#not-supported-yet)) |

The query metrics count what the [query history](/guide/configuration#query-history)
records: every pgwire statement that reaches the engine and every statement
submitted over the Trino protocol, each once. pgwire metadata queries answered
by the protocol layer (`pg_catalog`, `SET`, `SHOW`) do not count. A statement
refused by admission or timed out in the queue counts as `failed`. Workers have no HTTP listener, so their memory
pools are not exported. The coordinator reports their liveness.

`arneb_queries_running` and `arneb_admission_running` measure different
things. The first counts every statement the engine is executing and exists
even with `max_running_queries = 0`. The second counts admission slots in use
(with `max_running_queries = 0` every query holds one). For "how busy is the
coordinator", use `arneb_queries_running`; for "how close are we to the
admission cap", use the admission pair.

### Exchange events

Workers exchange data over Arrow Flight. Both ends of an exchange stream log
on the `arneb::exchange` target, so worker-to-worker exchanges, which no
`/metrics` endpoint covers, still show up in the logs:

- `exchange stream broke, reopening` (warn, reading side): the stream broke
  on a transport error. Broadcast output is reopened; partitioned output
  can't be, and the query fails.
- `exchange response dropped before its end` (info, sending side): with the
  ticket, buffer kind, peer address, messages and bytes sent, and the
  stream's age. A consumer that stops reading on purpose (a cancelled query)
  also causes it.

To see why a connection failed, add
`tonic::transport::server=debug,tonic::transport::channel::service::connection=debug`
to `RUST_LOG`. Each logs one line per failed connection, so they are cheap
to leave on, unlike `h2=debug`.

## Slow-query log

Statements that take at least `[observability] slow_query_threshold_ms`
milliseconds (`ARNEB_SLOW_QUERY_THRESHOLD_MS`; default `10000`; `0` turns the
log off) are logged at `warn` on the
`arneb::slow_query` target. Each entry has the duration, outcome, row count or
error, and the SQL (up to 2000 characters). As with the histogram, the
duration starts once the statement is admitted:

```text
WARN arneb::slow_query: slow query duration_ms=12731 outcome="finished" rows=175 sql=SELECT ...
```

To keep only these events, set `RUST_LOG=warn,arneb::slow_query=warn` or use a
log pipeline filter on the target.

## Prometheus and Grafana

`docker/observability/` scrapes the local bench cluster and ships a
provisioned dashboard:

```bash
docker compose -f docker-compose.yml -f docker/arneb-bench/docker-compose.bench.yml \
  -f docker/observability/docker-compose.observability.yml up -d prometheus grafana
```

- Prometheus: `http://localhost:9091`
- Grafana: `http://localhost:3000`, dashboard **Arneb**. It shows running and
  queued queries, queries per second by outcome, p50/p95/p99 latency, the
  memory pool, and worker liveness.

For your own Prometheus, scrape `<coordinator>:<web port>/metrics`. The
dashboard JSON is `docker/observability/grafana/dashboards/arneb.json`.
