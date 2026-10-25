## Why

A worker exits the moment it gets SIGTERM: only Ctrl-C is handled, and
even that stops the process at once. Every query with a task on the worker
then fails, so each Helm rolling upgrade (P6) or node drain fails the
queries in flight. This is gap G4 in the "Path to production" plan
(`docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`), and it has
to land before the chart ships.

## What Changes

- **SIGTERM drains a worker.** On SIGTERM or Ctrl-C a worker enters the
  draining state, tells the coordinator at once, keeps running the tasks it
  has, and exits when it holds no query the coordinator still runs, or when
  `drain_timeout_secs` (default 120, `ARNEB_DRAIN_TIMEOUT_SECS`) runs out.
  It then deregisters. A second signal exits at once.
- **The coordinator schedules new queries only on active workers.** A
  draining worker stays in the registry and stays alive for the queries it
  already runs (the lost-worker watch does not fire), but no new query
  picks it. It is listed with `state: "draining"`.
- **The coordinator releases a finished query on its workers.** Today a
  successful query leaves its tasks' buffers on each worker until a 5 s
  cleanup poll sees them consumed, and broadcast buffers for up to 600 s.
  The coordinator now sends `release_query` to each worker once the result
  is collected, and the worker drops that query's tasks and buffers. This
  is how a draining worker knows it is idle, and it frees broadcast memory
  as soon as the query ends.
- **Heartbeats carry the worker state** (`active`, `draining`, `stopped`),
  a field added the way G5 (`cluster-version-handshake`) allows.
- **Coordinator and standalone** also stop on SIGTERM as they do on Ctrl-C
  today (no drain; that is outside this change).

## Capabilities

### New Capabilities

- `worker-graceful-drain`: the drain sequence, its timeout, scheduling on
  active workers only, and deregistration.

### Modified Capabilities

- `worker-failure-handling`: "Lost workers are not scheduled" also excludes
  draining workers from new queries.
- `web-ui-cluster`: the workers API and cards show the state.

## Impact

- `crates/rpc/src/{heartbeat,flight_service,task_submission}.rs`
  (state field, `release_query` action and client)
- `crates/scheduler/src/node_registry.rs` (worker state, `active_workers`)
- `crates/server/src/{main,coordinator,task_manager,config}.rs`
  (signals, drain loop, release, `drain_timeout_secs`)
- `crates/server/src/web/api.rs`, `web/src/` (state on the worker card)
- `docs/guide/{distributed,configuration}.md`
- Builds on `cluster-version-handshake` (G5); P6's chart sets
  `terminationGracePeriodSeconds` above `drain_timeout_secs`.
