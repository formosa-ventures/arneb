## Context

This is item P2 of milestone M1 in the Phase 3 roadmap (`docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`). It depends on P1 (`exec-memory-accounting`, #137): scan decode buffers, exchange channels and the large operator states are now reserved against the memory pool, so a per-query limit measures something close to what a query really uses.

What exists today, read from the code:

| Piece | Where | State |
|---|---|---|
| Node memory pool | `main.rs` around line 490 | `TrackConsumersPool(GreedyMemoryPool(spill_budget))`, or `UnboundedMemoryPool`. Shared by every task on the node. |
| `QueryMemoryPool` | `crates/common/src/memory_pool.rs` | A correct per-query decorator: it checks its own cap, then cascades to the global pool, and drop releases both. `main.rs` wraps the global pool in **one** instance for the whole process (`resolve_query_cap()`, `ARNEB_QUERY_MAX_MEMORY_BYTES`), so every query and task shares a single counter. |
| `ResourceGroup` | `crates/scheduler/src/resource_group.rs` | A synchronous counter plus a `VecDeque` (`try_acquire` / `release`). It has no wake-up mechanism and no callers. |
| `QueryState::Queued` | `crates/scheduler/src/state.rs` | Exists, and the Web UI filters on it. Nothing ever sets it today. |
| Worker task lifecycle | `task_manager.rs` `execute_task` | A spawned task, an RSS admission probe, and a 5 s poll that removes buffers once they are consumed. No per-query index and no cancel. |
| Flight actions | `rpc/src/flight_service.rs` | `heartbeat`, `submit_task`, `report_dynamic_filters`, `notify_dynamic_filter`. No cancel action. |
| Fragment errors | `exchange_client.rs` | A worker task failure arrives as `Status::internal(msg)` and becomes `ExecutionError::InvalidOperation`, so the class is lost. |
| Trino cancel | `trino/mod.rs` `cancel_query` | Aborts the coordinator `JoinHandle` only. Worker tasks keep running, which `trino-clients.md` documents as a limitation. |
| pgwire `SET` | `metadata.rs` | Every `SET` except `search_path` is silently accepted. |
| SQLSTATE mapping | `protocol/src/error.rs` | Every `ArnebError::Execution` maps to `XX000`. |

## Goals / Non-Goals

**Goals**
- One query cannot take a node's memory away from other queries: it fails with a `53200`-class error when it exceeds its own limit.
- At most N queries run at once; the rest wait in a bounded FIFO queue, visible as `QUEUED`.
- A timed-out or cancelled query releases its memory, tasks and output buffers on every node.
- 20 concurrent SF10 queries on the local 2-worker cluster: the process survives and queuing is observable.
- With the limits off or not reached, plans, results and latency are unchanged.

**Non-Goals**
- A cluster-wide (summed) memory limit (D2).
- Per-user or per-source resource groups, priorities, and `query_max_queued_time`.
- pgwire `CancelRequest`. It is a follow-up; this change provides the cancel path it needs.
- CPU-time limits, and task retry (P4).

## Decisions

### D1: Per-query pools on each node, keyed by query id

Every node that runs a query's code gets one `QueryMemoryPool` for that query, a child of the node pool:

- **Worker**: `TaskManager` keeps a `HashMap<QueryId, QueryEntry>`. A `QueryEntry` holds:
  - `Arc<QueryMemoryPool>`;
  - the `AbortHandle` of each task;
  - the task ids, used for buffer cleanup.

  `execute_task` gets or creates the entry and builds the task's `ExecutionContext` with the query's pool instead of the node pool. The entry is removed after the query's last task has finished and its buffers are drained, or on `cancel_query` (D4). Dropping the pool releases anything still reserved.
- **Coordinator / standalone**: the local fragments of a distributed query, and a standalone query's whole plan, run under a pool created in `execute_query` / `run_statement` and dropped when the query ends.
- **Limit**: `query_max_memory_per_node` (bytes; `ARNEB_QUERY_MAX_MEMORY_BYTES` overrides). Unset means a query is limited only by the node pool, as today.
- The process-wide wrapper in `main.rs` goes away. The node pool keeps its spill budget and `TrackConsumersPool` diagnostics.

**Why per node, not cluster-wide.** Memory is reserved on workers. Each `try_grow` there is synchronous and must decide now. A cluster-wide limit would need the coordinator to sum per-worker usage from task status or heartbeats, and that view lags by at least one heartbeat interval (10 s). In that window a query can push a worker into the kernel OOM killer, which is exactly what this change exists to prevent. A per-node limit is exact and local, and it is Trino's own first line of defense (`query.max-memory-per-node`). A cluster-wide limit can be layered on later as a coordinator-side check over the per-node numbers, without changing this enforcement point.

### D2: The roadmap's `query_max_memory` becomes `query_max_memory_per_node`

The roadmap names the knob `query_max_memory`. Given D1, the honest name is `query_max_memory_per_node`, and that name already exists in `[memory]` with the right meaning in its doc comment, just not in its wiring. The change keeps it, fixes the wiring, and leaves `query_max_memory` (cluster-wide) unimplemented and documented as a follow-up. This does not change existing config files.

### D3: Resource errors keep their class across the wire

- **Worker**: when a task fails with `ExecutionError::ResourceExhausted`, its status is recorded as a resource failure. `do_get` on that task's buffer then returns `Status::resource_exhausted(msg)` instead of `Status::internal`.
- **Coordinator**: `ExchangeClient` maps tonic `ResourceExhausted` to `ExecutionError::ResourceExhausted`, and the task-status path does the same.
- **Message**: it names the query, the node, the limit and the top consumers, using the `TrackConsumersPool` text that already exists.
- **pgwire**: `ExecutionError::ResourceExhausted` maps to SQLSTATE `53200` (`out_of_memory`). The other execution errors keep `XX000`.
- **Trino**: it already maps to `EXCEEDED_LOCAL_MEMORY_LIMIT` (131079). That stays; it now actually fires for distributed queries.
- **Peers**: the first fragment failure of any class cancels the query's other tasks (D4), so the remaining fragments don't run to completion for nothing.

### D4: `cancel_query` Flight action and a coordinator cancel guard

- **Action**: a new Flight `do_action` type, `cancel_query`, with body `query_id`. It is idempotent, and an unknown query id is a no-op ack. The worker:
  - aborts every task of the query through the stored `AbortHandle`s (the drop chain releases operator reservations);
  - removes the query's output buffers from `FlightState`, so blocked consumers get `NotFound` instead of waiting;
  - drops the `QueryEntry`, which releases the per-query pool;
  - logs `query cancelled on worker` on `arneb::cluster` with the number of tasks aborted.
- **Coordinator guard**: `coordinator.execute` holds a `CancelGuard { query_id, workers_dispatched_to, armed }`. A successful completion disarms it. Any other exit fires `cancel_query` to every worker that received a task, from a detached task, so `Drop` never blocks. Other exits are an error return, a panic unwind, or the future being dropped. The future is dropped by a Trino `DELETE`, the reaper's abandonment, a timeout (D7), or a pgwire client disconnect.
- **Security**: the action goes through the `rpc::connect` path, so the P3 cluster secret and TLS apply to it.
- **Delivery**: cancellation is best-effort. A worker that is unreachable when the guard fires still drops its tasks through the existing consumed-buffer cleanup, and the RSS probe protects it meanwhile. The guard logs failed deliveries.

### D5: `AdmissionController` replaces `ResourceGroup`

`crates/scheduler/src/admission.rs`:

```rust
pub struct AdmissionController { /* max_running, max_queued, Mutex<state>, waiters: VecDeque<oneshot::Sender<Permit>> */ }
impl AdmissionController {
    pub async fn admit(&self, query: QueryId) -> Result<AdmissionPermit, AdmissionError>; // waits FIFO; QueueFull if over max_queued
    pub fn snapshot(&self) -> AdmissionSnapshot { running, queued, max_running, max_queued }
}
pub struct AdmissionPermit { /* Drop: releases the slot and hands it to the next waiter */ }
```

- **FIFO**: a slot freed by `Drop` goes straight to the head waiter, so a later arrival cannot overtake a queued query. A waiter whose future is dropped, because the client went away or timed out while queued, is skipped. The `ResourceGroup` struct and its tests are removed, since it has no callers.
- **Where**: one controller per coordinator / standalone process, shared by `ProtocolServer` and `TrinoServer`. The permit is acquired after parsing and session/metadata interception, and before planning, so a queued query holds no plan, no memory and no workers. It is held until the query's results are fully produced. For Trino, that means the statement task has completed; paging out materialized results does not hold a slot.
- **Bypass**: `SET`/`SHOW`/`RESET`, `pg_catalog` / `information_schema` / `system.*` metadata, `EXPLAIN` without `ANALYZE`, transaction no-ops and DDL that only touches the catalog never queue. Only statements that execute a plan do.
- **Observability**:
  - The Trino listener reports `QUEUED` until the permit is granted. The tracker entry moves `Queued → Planning`, so the Web UI's existing `QUEUED` filter shows waiting queries.
  - `arneb::admission` logs an INFO line when a query is queued (with its queue position) and when it is admitted (with how long it waited).
  - The same target logs a periodic DEBUG snapshot.
  - `/api/v1/cluster` gains `running_queries`, `queued_queries`, `max_running_queries` and `max_queued_queries`.
- **Queue full**: the query fails immediately with SQLSTATE `53000` (`insufficient_resources`) and the message `query queue is full (N running, M queued)`. On the Trino protocol it is `QUERY_QUEUE_FULL` (131074, `INSUFFICIENT_RESOURCES`).
- **Workers**: workers have no controller. Their admission stays the existing RSS probe, and the coordinator cap bounds how many queries reach them.

### D6: Defaults

| Knob | Default | Why |
|---|---|---|
| `max_running_queries` | `8` | A small multiple of the 2-to-4 workers a team cluster starts with. Enough parallelism to keep workers busy on SF10, low enough that 20 concurrent clients queue instead of splitting memory 20 ways. `0` means unlimited, which is today's behavior. |
| `max_queued_queries` | `256` | Absorbs a burst of BI dashboard queries; beyond that, failing fast beats unbounded waiting. |
| `query_max_memory_per_node` | unset | Stays opt-in (today's default) until the Tier 2 numbers show a safe value. The docs recommend about 0.5–0.8 × the node spill budget. |
| `statement_timeout` | `0` (off) | Matches PostgreSQL. Clients set it per session. |

These are recommendations; the user's call is recorded in the decisions log.

### D7: Statement timeout

- **pgwire**: `SET statement_timeout = 5000`, `= '5s'`, `= '2min'` or `TO DEFAULT` / `RESET`. `SHOW statement_timeout` reports the value in PostgreSQL's format (`5s`, `0`).
  - The value is stored in the connection's session extensions, next to `search_path`.
  - The timer starts when the statement arrives and covers the queue wait, matching PostgreSQL, where a lock wait counts.
  - On expiry the statement future is dropped (D4 cancels the workers), and the client gets `ERROR 57014 canceling statement due to statement timeout`. The connection stays usable.
- **Trino**: the `query_max_execution_time` session property (`X-Trino-Session`, `SET SESSION`) takes a Trino duration (`30s`, `5m`, `1h`).
  - The timer starts when the query is admitted, which is Trino's execution-time semantics: queue time is excluded.
  - On expiry the query fails with `EXCEEDED_TIME_LIMIT` (131075, `INSUFFICIENT_RESOURCES`), and D4 cancels the workers.
  - `query_max_run_time`, which includes queue time, is accepted and enforced the same way from submission.
- **Server default**: `[admission] statement_timeout` (`ARNEB_STATEMENT_TIMEOUT`, same syntax). It applies to every session that does not set its own, on both protocols. On pgwire, `SHOW` reports the effective value.
- **What a timeout stops**: planning, the queue wait (pgwire only) and execution. Already-materialized results being paged out over Trino are not cut off.

### D8: Config and logging

```toml
[admission]
max_running_queries = 8      # ARNEB_MAX_RUNNING_QUERIES; 0 = unlimited
max_queued_queries  = 256    # ARNEB_MAX_QUEUED_QUERIES
statement_timeout   = "0"    # ARNEB_STATEMENT_TIMEOUT; e.g. "30s", "5min", 30000 (ms)

[memory]
query_max_memory_per_node = 4294967296   # existing field; ARNEB_QUERY_MAX_MEMORY_BYTES (existing)
```

- Env-over-file parsing goes through an `env` closure so tests do not touch the process environment, the pattern `security-tls-auth` introduced.
- An invalid duration or a negative number is a startup error.
- **Coordinator / standalone**: one `arneb::config` line at startup with `max_running_queries`, `max_queued_queries`, `statement_timeout` and `query_max_memory_per_node` (or `off`).
- **Workers**: they log `query_max_memory_per_node` only; the admission knobs do not apply to them.
- **Deprecation**: the old startup line that says "per-task query cap" is reworded to "per-query cap".

## Risks / Trade-offs

- **[Per-node limit and skew]** A query whose input is skewed toward one worker hits the limit there first (compare #126). → That is the safe failure. The error names the node, and the docs explain that the limit is per node.
- **[Admission on by default]** A deployment that relies on more than 8 concurrent queries now queues them. → The startup log shows the cap, and `0` restores unlimited. Called out in the docs and the CHANGELOG.
- **[Cancel is best-effort]** A lost `cancel_query` leaves tasks running until the existing cleanup. → The guard logs failed deliveries, the per-query pool still bounds them, and the Tier 2 test checks the normal path releases everything.
- **[Holding the slot through result materialization]** Big result sets keep a slot while the coordinator collects batches. → That is intended: those bytes are the coordinator's memory. Paging out already-materialized results over Trino does not hold one.
- **[Error-class change]** Clients that matched on `XX000` for memory errors now see `53200`. → The new code is the correct one, and it is documented.

## Migration Plan

1. PR A: D1, D2 and D3 (per-query pools, typed errors, `53200`).
2. PR B: D4 (cancel action and guard). This also removes the "worker tasks keep running after a Trino cancel" limitation.
3. PR C: D5 and D6 (admission).
4. PR D: D7 (timeouts), which depends on PR B, plus D8's remaining knobs.

Docs land with each PR. Rollback: unset the memory limit, set `max_running_queries = 0` and leave `statement_timeout` at `0`, and behavior matches today apart from the typed error codes.

## Decisions log

Open questions for review are listed in `tasks.md` section 0 and are resolved before implementation starts.
