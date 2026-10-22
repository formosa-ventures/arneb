## Why

Phase 3 M3 / P4 stage 1 ("fail cleanly",
`docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`): when a worker
dies mid-query, the coordinator must fail the query with a clear error within
30 s, release its tasks on the other workers, and keep serving.

Measured on the local bench cluster (SF10, coordinator + 2 workers, main
`fd1bc61`) before this change:

| Fault during q09 | Today |
|---|---|
| `docker kill` worker-2 | Query fails in 4 s, but the error is a bare `h2 protocol error … BrokenPipe` that does not name the worker. The next queries fail too (`task submission failed: failed to connect to http://arneb-worker-2:9090`) for about 30 s, until a heartbeat check happens to mark the worker dead. |
| `docker pause` worker-2 (hung, sockets open) | **The query never returns.** It was still running when the client gave up at 150 s. Nothing watches a running query's workers. |
| Tasks on the surviving worker | Already cancelled (`CancelGuard`, #138): no zombie tasks. |

Two things cause this. The registry marks a worker dead only when somebody
calls `alive_workers()`. And with a 10 s heartbeat interval and a 30 s
timeout, a hung worker cannot be detected within 30 s at all.

## What Changes

- **Running queries watch their workers.** While a distributed query runs,
  the coordinator checks every 500 ms whether any worker it dispatched to
  has missed its heartbeat deadline. When one has, the query fails with
  `worker '<id>' (<address>) lost: no heartbeat for <n> s`, and the existing
  `CancelGuard` cancels its tasks on every worker.
- **A transport failure marks the worker dead at once.** A failed task
  submission or a broken Flight stream to a worker marks that worker dead in
  the registry. The query's error names the worker, and the next query is
  scheduled on the remaining workers instead of failing until the timeout.
  A later heartbeat brings the worker back (logged as `worker re-registered`).
- **Faster detection:** `[cluster] heartbeat_interval_secs` (worker, default
  **2**) and `heartbeat_timeout_secs` (coordinator, default **10**), with
  `ARNEB_HEARTBEAT_INTERVAL_SECS` / `ARNEB_HEARTBEAT_TIMEOUT_SECS` overrides,
  validated (timeout > interval) and logged at startup.
- **Automated Tier 2 test:** `benchmarks/tpch/scripts/fault_injection.sh`
  kills and pauses a worker during SF10 q09 on the bench cluster. It asserts
  the query fails within 30 s with an error naming the worker, that the next
  query succeeds, and that the restarted worker is used again.

Query retry (stage 2) and task-level retry are out of scope.

## Capabilities

### New Capabilities

- `worker-failure-handling`: how the coordinator detects a lost worker
  (heartbeat deadline, transport failure), what happens to running and
  subsequent queries, the error the client sees, and re-registration.

### Modified Capabilities

- `server-config`: `[cluster]` accepts `heartbeat_interval_secs` and
  `heartbeat_timeout_secs`.

## Impact

- `crates/scheduler/src/node_registry.rs`: deadline-based liveness queries
  plus `mark_dead`.
- `crates/server/src/coordinator.rs`: the liveness watch around distributed
  execution, and marking a worker dead when a submission fails.
- `crates/rpc/src/exchange_client.rs`: Flight stream errors name the remote
  address.
- `crates/server/src/{config,main}.rs`: the two knobs, the heartbeat loop
  interval, and registry construction.
- Docs: `docs/guide/configuration.md` and the distributed deployment guide.
