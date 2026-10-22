## Context

See proposal.md for the measured behavior. The relevant pieces:

- `NodeRegistry` holds `last_heartbeat` per worker. `alive_workers()` flips
  `alive = false` for workers past `heartbeat_timeout_secs`, but only when
  called. The registry is built with `NodeRegistry::default()` (30 s).
- Workers send a heartbeat every 10 s (`worker_heartbeat_loop`).
- `DistributedExecutor::execute` picks `alive_workers()` once, arms a
  `CancelGuard` over them, dispatches every child fragment
  (`execute_child_on_worker`), then collects the root stage locally from the
  workers' output buffers over Flight.

## Goals / Non-Goals

**Goals:** a hung or dead worker fails its running queries within 30 s with
an error that names it, other workers release that query's tasks, later
queries avoid the worker, and a restarted worker rejoins.

**Non-Goals:** re-running a failed query (stage 2, `retry_policy=QUERY`),
task-level retry or exchange spooling, and coordinator failure.

## Decisions

### D1 — A watch future raced against execution, not per-call timeouts

`execute` runs its dispatch-and-collect body inside `tokio::select!` against
`watch_workers(registry, ids)`. That is a loop that sleeps 500 ms and asks the
registry whether any of `ids` is past its deadline (or was marked dead). When
the watch wins, `execute` returns the "worker lost" error, and dropping the
body plus the armed `CancelGuard` cancels the query everywhere. That path
already exists and is tested.

*Alternatives:* gRPC/HTTP2 keepalive timeouts on every Flight channel would
catch a hung peer on that one connection. But they need per-channel tuning,
fire on long GC-free pauses we do not want to treat as death, and do not cover
a worker that is idle in this query (e.g. a finished stage whose buffer has
not been read yet). Per-call timeouts break long-running scans. The watch
uses the one liveness signal the cluster already has.

### D2 — Liveness is a deadline, evaluated on read

`NodeRegistry` gains `lost_worker(&[ids]) -> Option<LostWorker>`, which
reports a worker that is marked dead or whose `last_heartbeat` is older than
the timeout. It also gains `mark_dead(id, reason)`. A heartbeat from a dead
worker sets `alive = true` again and logs `worker re-registered`.

### D3 — Defaults: 2 s interval, 10 s timeout

A hung worker is detected within `timeout + 0.5 s` ≈ 10.5 s after its last
heartbeat, so at most about 12.5 s after it hangs. That is well inside the
30 s target. Five missed heartbeats in a row are needed, so a busy worker
(heartbeats go through their own Flight call on the worker's runtime) is not
declared dead by one slow beat. Validation rejects `timeout <= interval`,
since the knobs live on different nodes. The coordinator logs its timeout and
the worker logs its interval.

### D4 — Transport failures name the worker and mark it dead

`execute_child_on_worker` already knows the worker of each submission. A
failed submission whose error is a connect/transport failure marks that
worker dead and returns `worker '<id>' (<address>) unreachable: …`. Exchange
reads add the remote address to their error text. The executor maps the
address back to a worker id and marks it dead when the error is a transport
error. Server-side errors (a task that failed) do not mark the worker dead.

## Risks / Trade-offs

- **A worker under heavy load can miss heartbeats → false "lost".** The
  timeout covers five intervals and is configurable. The worker's heartbeat
  runs on its own Tokio task.
- **Marking dead on a transient network blip drops a healthy worker until
  its next heartbeat (≤ 2 s).** That is acceptable: the next heartbeat
  restores it.
