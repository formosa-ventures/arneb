## Why

Phase 3 roadmap, milestone M1 / P2. Arneb is meant to be shared by a team, but nothing stops one user from taking the cluster down:

1. **No real per-query memory limit.** `[memory] query_max_memory_per_node` exists, but `main.rs` wraps the global pool in a single `QueryMemoryPool` that every task on the node shares. Despite the name, it is a second node-wide cap: one heavy query can use all of it, and the next query that asks for memory is the one that fails.
2. **No admission control.** Every query starts planning and dispatching immediately. `crates/scheduler` has a synchronous `ResourceGroup` (running cap + queue), but nothing uses it. Twenty concurrent SF10 queries all run at once and compete for the same worker memory.
3. **No statement timeout.** `SET statement_timeout` is accepted and ignored, and the Trino session property `query_max_execution_time` is echoed but not interpreted.
4. **Cancellation stops at the coordinator.** A Trino `DELETE`, an abandoned query, or a failed fragment drops the coordinator future, but worker tasks keep running until they finish. They hold memory and output buffers, which a timeout would also leave behind.
5. **Resource errors lose their class across the wire.** A worker's `ResourceExhausted` comes back to the coordinator as tonic `Internal`, becomes `InvalidOperation`, and reaches the client as `XX000` / `GENERIC_INTERNAL_ERROR`. Clients cannot tell "your query was too big" from a bug.

## What Changes

- **A per-query memory limit on every node.** Each worker (and the coordinator, for its own fragments) gives each query its own `QueryMemoryPool` child of the node pool, shared by all of that query's tasks on that node and dropped when the last one ends.
  - The limit is `query_max_memory_per_node`, with `ARNEB_QUERY_MAX_MEMORY_BYTES` kept as the override.
  - A query over its limit fails with SQLSTATE `53200` (`out_of_memory`) on pgwire and `EXCEEDED_LOCAL_MEMORY_LIMIT` on the Trino protocol. Co-resident queries are unaffected.
- **Typed error propagation.** A worker task that fails with `ResourceExhausted` reports tonic `ResourceExhausted`, which the coordinator maps back to `ExecutionError::ResourceExhausted`. Every fragment failure also cancels the rest of the query.
- **Cluster-wide cancellation.** A new Flight action `cancel_query { query_id }` aborts every task of a query on a worker, removes its output buffers, and releases its per-query pool.
  - The coordinator sends it from an RAII guard whenever a distributed query ends early: client cancel, timeout, abandonment, a fragment error, or the coordinator future being dropped.
- **Admission control.** A process-wide `AdmissionController` on coordinator and standalone nodes replaces the unused `ResourceGroup`, enforcing a running cap and a FIFO queue with its own cap.
  - Both pgwire and the Trino listener acquire a slot before planning; session and metadata statements bypass it.
  - Queued queries show `QUEUED` in the Web UI and the Trino protocol, and are counted in a periodic `arneb::admission` log line.
  - A full queue rejects the query with `53000` / `QUERY_QUEUE_FULL`.
- **Statement timeout.**
  - **pgwire**: `SET`/`SHOW`/`RESET statement_timeout`. It uses PostgreSQL semantics: milliseconds or a unit string, `0` = off, measured from statement start including queue time.
  - **Trino**: the `query_max_execution_time` session property, measured from the start of execution (Trino semantics, queue time excluded).
  - **Server default**: `statement_timeout`.
  - On expiry the query is cancelled cluster-wide and fails with `57014` (`query_canceled`) / `EXCEEDED_TIME_LIMIT`.
- **Runtime knobs.** New `[admission]` section (`max_running_queries`, `max_queued_queries`, `statement_timeout`) with `ARNEB_*` overrides. The effective values, including the per-node query memory limit, are logged on `arneb::config` at startup.

## Capabilities

### New Capabilities

- `query-memory-limit`: per-query, per-node memory pools, the error class for exceeding them, and typed propagation of resource errors from workers.
- `query-cancellation`: cancelling a distributed query on every worker, and the guarantee that its tasks, buffers and memory are released.
- `query-admission`: the running cap, the FIFO queue, queue-full rejection and queue observability on both client protocols.
- `statement-timeout`: `statement_timeout` on pgwire, `query_max_execution_time` on the Trino protocol, the server default, and their error codes.

### Modified Capabilities

- `server-config`: the new `[admission]` section and its `ARNEB_*` overrides; the corrected semantics and logging of `[memory] query_max_memory_per_node`.

## Impact

- **Crates**:
  - `common`: an error variant or classification for timeouts and cancellation.
  - `scheduler`: an async `AdmissionController` that replaces `ResourceGroup`.
  - `rpc`: the `cancel_query` action, typed `ResourceExhausted` status mapping in `flight_service.rs` / `exchange_client.rs`, and a client helper.
  - `server`: per-query pools and per-query task tracking in `task_manager.rs`, the cancel guard in `coordinator.rs`, config, and wiring in `main.rs`.
  - `protocol`:
    - Admission and timeout around `execute_query` in `handler.rs` and `trino/engine.rs`.
    - `statement_timeout` session state in `session.rs`.
    - SQLSTATE mapping in `error.rs`.
    - Trino error names in `trino/error.rs`.
- **Dependencies**: none new.
- **Compatibility**:
  - Behavior changes only when a limit triggers. With `max_running_queries` above the concurrent load, no per-node query limit, and no timeout, every query runs as today.
  - Three intentional changes:
    - `query_max_memory_per_node` now applies per query, as its name says. Today it is effectively a node-wide cap.
    - Admission is on by default (see design D6 for the default cap).
    - `ResourceExhausted` reaches pgwire clients as `53200` instead of `XX000`.
- **Out of scope**:
  - A cluster-wide (summed across nodes) query memory limit (design D2).
  - Resource groups per user or source, priorities, and query queue time limits.
  - pgwire `CancelRequest` (the out-of-band cancel key). It is a follow-up that will reuse the cancellation path from this change.
  - Killing queries for CPU time.
  - Task-level retry, which is P4.
