## Why

P4 stage 1 (#171) makes a lost worker fail its queries cleanly within 30 s.
A team running BI dashboards on Arneb still sees those queries fail on every
rolling restart that misses the drain, every OOM-killed pod and every
network blip. Trino offers `retry_policy=QUERY` for this. The roadmap
(v0.1.8, "P4 stage 2") calls for the same: a failed read-only query re-runs
on the remaining workers, off by default.

## What Changes

- `[cluster] retry_policy = "none" | "query"` (`ARNEB_RETRY_POLICY`, default
  `none`) and `query_retry_attempts` (`ARNEB_QUERY_RETRY_ATTEMPTS`, default
  4), read on the coordinator and logged at startup. `task` is refused.
- With `query`, the coordinator re-runs a read-only distributed query whose
  run failed because one of that run's workers was lost, on the workers still
  active, after 1 s, doubling to at most 10 s, up to the attempt limit.
- Each re-run logs `retrying query after worker loss` on `arneb::cluster`
  and counts in a new `arneb_query_retries_total` counter.
- `fault_injection.sh` gains `RETRY=1`: q09 completes with the same rows as
  a run without a fault, under a kill and a pause.

## Capabilities

### New Capabilities

### Modified Capabilities
- `worker-failure-handling`: queries re-run after a worker loss when retry is on.
- `server-config`: the retry settings.

## Impact

- `crates/server/src/coordinator.rs` (retry loop around one run),
  `crates/server/src/config.rs`, `crates/server/src/main.rs`,
  `crates/common/src/metrics.rs`.
- `benchmarks/tpch/scripts/fault_injection.sh`,
  `docker/arneb-bench/docker-compose.bench.yml` (passes `ARNEB_RETRY_POLICY`).
- Docs: configuration, distributed, observability.
- No protocol or wire change. With the default (`none`) behavior is
  unchanged.
