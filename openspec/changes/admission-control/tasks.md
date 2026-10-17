Sections 1–4 land as four PRs, in order: A (memory), B (cancellation), C (admission), D (timeouts). Section 5 is the Tier 1 gate for every PR, section 6 is the Tier 2 check (run per PR for its slice, and in full before D merges), and section 7 is the docs, landed with each PR.

## 0. Decisions (resolved 2026-10-17, see design.md "Decisions log")

- [x] 0.1 Default `max_running_queries`. Recommended: `8`. Alternatives: `0` (unlimited, opt-in admission), or derived from the number of workers (for example 4 × alive workers, recomputed as workers join).
- [x] 0.2 Default `max_queued_queries` and queue-full behavior. Recommended: `256`, then reject with `53000` / `QUERY_QUEUE_FULL`. Alternative: an unbounded queue with a `max_queued_time`.
- [x] 0.3 Memory limit scope. Recommended: per node only now (D1/D2), with a cluster-wide limit as a follow-up. Alternative: also ship a coordinator-side cluster-wide sum check, enforced by polling task status (laggy, see D1).
- [x] 0.4 Default `query_max_memory_per_node`. Recommended: unset (opt-in) until Tier 2 numbers exist. Alternative: default to a fraction (for example 0.8) of the node spill budget when one is set.
- [x] 0.5 Default `statement_timeout`. Recommended: `0` (off, PostgreSQL's default). Alternative: a generous server default such as `1h`.

## 1. PR A — per-query memory pools and typed resource errors (D1–D3)

- [ ] 1.1 `TaskManager`: add `HashMap<QueryId, QueryEntry { pool: Arc<QueryMemoryPool>, tasks: HashMap<TaskId, AbortHandle> }>`. `execute_task` gets or creates the entry and builds `ExecutionContext` with the query's pool. Remove the entry when the last task's buffers are cleaned up.
- [ ] 1.2 `main.rs`: remove the process-wide `QueryMemoryPool` wrap. Pass the node pool plus the resolved `query_max_memory_per_node` into `TaskManager`, `ProtocolServer` and `TrinoServer`. Reword the startup log to say "per-query limit".
- [ ] 1.3 Coordinator / standalone: `execute_query` (pgwire) and `run_statement` (Trino) create a per-query pool for local execution and the coordinator's own fragments, and drop it at the end of the query.
- [ ] 1.4 Typed errors: the worker records `ResourceExhausted` task failures and `do_get` returns `Status::resource_exhausted`. `ExchangeClient` and the task-status path map that status back to `ExecutionError::ResourceExhausted`. The message names the node and the limit.
- [ ] 1.5 pgwire SQLSTATE: map `ExecutionError::ResourceExhausted` to `53200` in `protocol/src/error.rs`. Keep Trino's `EXCEEDED_LOCAL_MEMORY_LIMIT` and add a test for it on the distributed path.
- [ ] 1.6 Tests:
  - `QueryMemoryPool` per query: two queries on one pool, one over its limit, the other unaffected.
  - The entry is dropped after the last task, and node reserved bytes return to the baseline.
  - The Flight round trip keeps the `ResourceExhausted` class (rpc integration test).
  - The pgwire `53200` mapping.
  - An end-to-end standalone test: a tiny limit fails `SELECT ... GROUP BY` with `53200`.

## 2. PR B — cluster-wide cancellation (D4)

- [ ] 2.1 `rpc`: add the `cancel_query` action (body: query id), a `CancelQueryCallback` on `FlightState`, and the client helper `cancel_query(addr, query_id)` through `rpc::connect`.
- [ ] 2.2 Worker: the callback aborts the query's tasks through the stored `AbortHandle`s, removes their buffers from `FlightState`, drops the `QueryEntry`, and logs `query cancelled on worker` (`arneb::cluster`, with the task count). It is idempotent.
- [ ] 2.3 Coordinator: a `CancelGuard` in `coordinator.execute` records the workers that received tasks, is disarmed on success, and on any other exit (including `Drop`) sends `cancel_query` from a detached task. Failed deliveries are logged.
- [ ] 2.4 The first fragment error fails the query and fires the guard, so the remaining fragments are cancelled.
- [ ] 2.5 Tests:
  - An rpc integration test: `cancel_query` aborts a long task, removes its buffer and is idempotent.
  - A server test: dropping the coordinator future sends `cancel_query` to every worker that received a task.
  - A fragment failure cancels its sibling tasks.
- [ ] 2.6 Docs: remove the "worker tasks that are already running may continue" limitation from `docs/guide/trino-clients.md`.

## 3. PR C — admission control (D5, D6)

- [ ] 3.1 `scheduler/src/admission.rs`: `AdmissionController::admit(query_id) -> Result<AdmissionPermit, AdmissionError>` with FIFO hand-off on `Drop`, dropped-waiter skipping, `QueueFull`, `snapshot()`, and `0` = unlimited. Remove the unused `ResourceGroup` and its tests.
- [ ] 3.2 Config: an `[admission]` section with `max_running_queries`, `max_queued_queries` and `statement_timeout` (the latter used in PR D), `ARNEB_*` overrides through an `env` closure, validation, and the startup log on `arneb::config`.
- [ ] 3.3 pgwire: acquire the permit after interception and before planning, in both the Simple and Extended paths. Session and metadata statements and `EXPLAIN` (without `ANALYZE`) bypass it. Hold the permit until execution completes.
- [ ] 3.4 Trino: acquire the permit inside the statement task, report `QUEUED` until it is granted, and release it when the statement task completes (not after paging).
- [ ] 3.5 Tracker and Web UI: create the tracker entry in `Queued` and move it to `Planning` on admission. Add `running_queries`, `queued_queries`, `max_running_queries` and `max_queued_queries` to `/api/v1/cluster`, and show them on the dashboard.
- [ ] 3.6 Errors: queue full → pgwire `53000` / Trino `QUERY_QUEUE_FULL` (131074, `INSUFFICIENT_RESOURCES`).
- [ ] 3.7 Logging: `arneb::admission` INFO lines on queue (with position) and admit (with wait time), and a DEBUG snapshot every 10 s while anything is queued.
- [ ] 3.8 Tests:
  - Controller unit tests: cap, FIFO order, dropped-waiter skipping, queue full, unlimited.
  - pgwire and Trino integration tests with `max_running_queries = 1`: the second query waits and shows `QUEUED`, then completes; a third with `max_queued = 1` gets the queue-full error; `SET` and metadata bypass.

## 4. PR D — statement timeouts (D7, D8)

- [ ] 4.1 pgwire session: handle `SET` / `SHOW` / `RESET statement_timeout` in `session.rs` next to `search_path`, with PostgreSQL value parsing and an invalid value → `22023`.
- [ ] 4.2 pgwire enforcement: wrap admission plus execution in `tokio::time::timeout` from statement receipt. On expiry, drop the future (PR B cancels the workers) and return `57014 canceling statement due to statement timeout`.
- [ ] 4.3 Trino: parse `query_max_execution_time` (timer from admission) and `query_max_run_time` (timer from submission) from the session as Trino durations. On expiry, fail with `EXCEEDED_TIME_LIMIT` (131075) through `cancel_query`. A bad duration → `INVALID_SESSION_PROPERTY`.
- [ ] 4.4 Server default `[admission] statement_timeout` / `ARNEB_STATEMENT_TIMEOUT` applies to both protocols when the session sets none.
- [ ] 4.5 Tests:
  - Duration parsing (PostgreSQL and Trino forms).
  - pgwire: `SET`/`SHOW`/`RESET`, a timeout fires `57014`, and the connection stays usable.
  - Trino: `EXCEEDED_TIME_LIMIT`, and queue time is excluded from `query_max_execution_time`.
  - The server default applies.

## 5. Tier 1 — local correctness gate (every PR)

- [ ] 5.1 `cargo fmt -- --check` and `cargo clippy --workspace --all-targets -- -D warnings` are clean.
- [ ] 5.2 `cargo test --workspace` is green. With limits off or not reached, all existing tests pass unchanged.

## 6. Tier 2 — local distributed check (coordinator + 2 workers, SF10)

- [ ] 6.1 Plumbing (CLAUDE.md rule):
  - The new `arneb::config`, `arneb::admission` and `arneb::cluster` lines appear under the default `RUST_LOG`.
  - `/api/v1/cluster` shows the admission counters.
- [ ] 6.2 Concurrency acceptance: 20 concurrent SF10 TPC-H queries (a mix including q09, q18 and q21) with the default `max_running_queries`.
  - Every coordinator and worker process survives (no OOM kill).
  - The Web UI and `/api/v1/cluster` show queued queries draining.
  - Every query returns cell-identical results to its single-query run.
  - Record peak RSS per node and the total wall time, compared with `max_running_queries = 0`.
- [ ] 6.3 Memory limit:
  - With `ARNEB_QUERY_MAX_MEMORY_BYTES` set below q18's per-worker peak (about 1.4 GB at SF10, see #126), q18 fails with `53200` over psql and `EXCEEDED_LOCAL_MEMORY_LIMIT` over the trino CLI.
  - A concurrent small query completes.
  - The workers' tracked bytes return to the baseline afterwards (`arneb::mem`).
- [ ] 6.4 Timeout:
  - `SET statement_timeout = '500ms'` on q09 gives `57014`.
  - Every worker logs `query cancelled on worker`, no task of that query is left in `FlightState`, and the per-query pool is gone.
  - The same holds for `query_max_execution_time=1s` via the trino CLI and for a Trino `DELETE` mid-query.
- [ ] 6.5 No regression: run the SF10 22-query bench (`run_benchmark.sh --scenario=sf10`) with defaults against the pre-change baseline. Latency and memory geomeans should be within run-to-run noise, using Trino as the drift control. Record both numbers here.
- [ ] 6.6 Tier 3 (remote SF30) is not required: this change carries no performance claim. An optional SF30 run of 6.3 (memory limit under real pressure) can ride along with the deferred P1 SF30 tasks.

## 7. Docs

- [ ] 7.1 `docs/guide/configuration.md`:
  - An Admission section covering the cap, the queue, queue-full behavior, the knobs and the logs.
  - The corrected `query_max_memory_per_node` description (per query, per node), with sizing guidance.
  - The new `ARNEB_*` variables.
- [ ] 7.2 `docs/guide/trino-clients.md`:
  - `query_max_execution_time` and `query_max_run_time` are now enforced.
  - Document `QUEUED`, `QUERY_QUEUE_FULL` and `EXCEEDED_TIME_LIMIT`.
  - Remove the "session properties are not interpreted" claim for these two properties.
- [ ] 7.3 `docs/sql/overview.md` (or the pgwire section): `SET statement_timeout`, and the error codes `53200`, `53000` and `57014`.
- [ ] 7.4 CHANGELOG notes for the three intentional behavior changes: `query_max_memory_per_node` now applies per query, admission is on by default, and memory errors are reported as `53200`.
