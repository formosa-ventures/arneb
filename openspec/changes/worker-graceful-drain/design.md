## Context

`main.rs` ends in a `tokio::select!` over the listeners, the worker
heartbeat loop and `ctrl_c()`. SIGTERM is not handled, so the process dies
with the default action. A worker's queries are tracked in `TaskManager`'s
`QueryTasks.running` (query → task → abort handles). An entry leaves that
map when the task's cleanup poll sees its buffer consumed (every 5 s; up to
600 s for a broadcast buffer, which never reports consumption) or when the
coordinator cancels the query. The coordinator's `CancelGuard` sends
`cancel_query` to every worker on any exit except success; on success it is
disarmed and sends nothing.

Kubernetes sends SIGTERM, waits `terminationGracePeriodSeconds` (default
30 s), then SIGKILL.

## Goals / Non-Goals

**Goals:**
- A worker that gets SIGTERM fails no query that was running on it, as long
  as those queries finish within the drain timeout.
- No new query is scheduled on a draining worker.
- The drain ends as soon as the worker is idle, not after a fixed wait.

**Non-Goals:**
- Draining the coordinator. Its queries are its client sessions; it stops
  as today, now on SIGTERM too.
- Moving a draining worker's tasks elsewhere (that is P4 stage 2, retry).
- A drain triggered over the API. SIGTERM is what Kubernetes and systemd
  send; an API can come later.

## Decisions

### Idle means "no query the coordinator has not released"

A worker is idle when `QueryTasks.running` is empty. Watching task
statuses or open `do_get` streams is not enough: a finished producer task
can still have a consumer that has not connected yet (a hash join opens its
probe side after the build), and exiting then fails the query. Only the
coordinator knows a query is over, so it says so:

- `CancelGuard::disarm` now switches the guard to *release* instead of
  doing nothing. On drop, a disarmed guard sends `release_query` to the
  query's workers; an armed one sends `cancel_query` as before. Disarm
  happens after the root stage has collected every row, so no task of the
  query is still needed.
- On `release_query` the worker runs the same teardown as `cancel_query`
  (abort, drop buffers, statuses, collectors, tombstone) but logs at debug
  and does not count as a cancellation.

A coordinator that dies leaves entries behind; the drain timeout bounds
that case, and the existing 600 s cleanup ceiling still applies.

*Alternative: wait for the cleanup polls.* Broadcast buffers hold a query
for 600 s, which defeats the drain. *Alternative: a fixed grace period.*
Either too short for long queries or slower than needed for short ones.

### Draining is a worker state in the heartbeat

`HeartbeatMessage` gains `state: WorkerState` (`active` | `draining` |
`stopped`, `#[serde(default)]` = `active`). `NodeRegistry` stores it:

- `active_workers()` (alive and active) is what `QueryCoordinator` schedules
  on. `alive_workers()` keeps its meaning (alive, any state), so the
  lost-worker watch and `alive_count` treat a draining worker as alive.
- `stopped` removes the worker and logs `worker deregistered`. The
  registry remembers its address, so a query it still ran (drain timed out)
  fails with `left the cluster when its drain ended` rather than
  `no longer registered`.
- A `draining` heartbeat from an id that already deregistered is a late one
  (sent before `stopped`, delivered after) and is ignored, so the registry
  itself keeps a drained worker out. A worker whose heartbeat says `active`
  (a restart under the same id) rejoins and becomes schedulable again.
- Liveness and drain state are updated under one write lock, so no reader
  sees a draining worker as active in between.

### Drain sequence on the worker

1. SIGTERM or Ctrl-C: set the shared state to `draining`, log
   `worker draining`, and send a heartbeat at once instead of waiting for
   the interval.
2. Wait one heartbeat interval. A query planned just before the coordinator
   saw the heartbeat may still submit tasks, and those must be tracked
   before the idle check.
3. Poll every 500 ms until `QueryTasks.running` is empty or
   `drain_timeout_secs` passes. Tasks are still accepted while draining.
4. Log `worker drained` (or `drain timed out` with the number of queries
   left), send `stopped`, and exit once it is sent (at most 5 s). A
   `draining` heartbeat still in flight may land after it; the coordinator
   ignores it (see above).

A second SIGTERM or Ctrl-C during the drain exits at once.

`drain_timeout_secs` lives in `[cluster]` with `ARNEB_DRAIN_TIMEOUT_SECS`,
default 120 s, read on workers. It is runtime-tunable, so an env override
is allowed under the config rules. The chart must set
`terminationGracePeriodSeconds` above it.

### Signals for every role

`shutdown_signal()` resolves on SIGTERM or Ctrl-C (Unix; Ctrl-C only
elsewhere). Coordinator and standalone stop on it as they do on Ctrl-C now.

## Risks / Trade-offs

- [One extra RPC per worker per successful query] → It goes only to the
  workers that ran a task of the query, once each, in parallel after the
  result is collected, off the query's latency path (spawned from `Drop`).
  A release adds no cancel tombstone on the worker, since every task of a
  successful query was already submitted.
- [A release that fails to arrive keeps the query on the worker until the
  cleanup polls drop it] → Same as today; it only lengthens a drain.
- [Release aborts a task still running after the result is complete, such as
  a producer under a satisfied `LIMIT`] → Intended: the work is no longer
  needed, and today it runs on until it finishes.
- [A query longer than the drain timeout fails] → Logged with the count;
  operators raise `drain_timeout_secs` and the grace period together.

## Migration Plan

Lands with or after G5, so mixed versions are already refused. The
`release_query` action is new; within one release both sides have it.
