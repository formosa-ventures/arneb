## 1. Release finished queries

- [x] 1.1 `release_query` Flight action + `arneb_rpc::release_query` client; worker callback shares `cancel_query`'s teardown, logs at debug
- [x] 1.2 `CancelGuard`: a disarmed guard sends `release_query` on drop; an armed one `cancel_query`
- [x] 1.3 Tests: release drops a query's tasks and buffers; unknown query is acknowledged; guard sends the right action

## 2. Worker state

- [x] 2.1 `WorkerState` (`active`/`draining`/`stopped`) in `HeartbeatMessage`, `#[serde(default)]`
- [x] 2.2 `NodeRegistry`: store state; `active_workers()`; `stopped` removes and logs `worker deregistered`
- [x] 2.3 `QueryCoordinator` schedules on `active_workers()`
- [x] 2.4 Tests: draining excluded from `active_workers` but alive; `stopped` removes; `active` restores

## 3. Drain

- [x] 3.1 `[cluster] drain_timeout_secs` + `ARNEB_DRAIN_TIMEOUT_SECS` (default 120, ≥ 1), logged at startup on workers
- [x] 3.2 `shutdown_signal()`: SIGTERM or Ctrl-C, used for every role
- [x] 3.3 Worker drain: draining heartbeat at once, wait one interval, poll `TaskManager` until idle or timeout, `stopped` heartbeat, exit; second signal exits at once
- [x] 3.4 Tests: config parsing; drain returns when idle and when the timeout passes

## 4. Web UI and docs

- [x] 4.1 `state` in `GET /api/v1/cluster/workers`; "Draining" badge on the card; rebuild the bundle
- [x] 4.2 `docs/guide/distributed.md` (drain, release) and `configuration.md` (`drain_timeout_secs`)

## 5. Validation

- [x] 5.1 Tier 1: rpc, scheduler, server tests; clippy and fmt clean
- [x] 5.2 Tier 2 (native SF10, 2026-10-24): SIGTERM to worker-2 0.5 s into q09 → q09 succeeds with the warm-run result (to 2 dp; doubles differ in the last digits from summation order), worker-2 logs `worker draining` (`active_queries=1`) then `worker drained` once q09 ends, the coordinator logs `worker deregistered`, and the workers API drops it. q06 started during the drain gets no task on worker-2 and succeeds. An idle worker exits 2.1 s after SIGTERM. With `ARNEB_DRAIN_TIMEOUT_SECS=1`, `drain timed out` is logged, q09 fails naming worker-2, and the next query succeeds.
- [x] 5.3 Fixes from Tier 2: the `stopped` heartbeat is sent by the heartbeat loop, so a periodic `draining` heartbeat in flight can no longer re-register a deregistered worker; a query lost to a drain timeout reports `left the cluster when its drain ended` with the worker's address instead of `() no longer registered`.
