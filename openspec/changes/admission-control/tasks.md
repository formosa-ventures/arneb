Sections 1–4 land as four PRs, in order: A (memory), B (cancellation), C (admission), D (timeouts). Section 5 is the Tier 1 gate for every PR, section 6 is the Tier 2 check (run per PR for its slice, and in full before D merges), and section 7 is the docs, landed with each PR.

## 0. Decisions (resolved 2026-10-17, see design.md "Decisions log")

- [x] 0.1 Default `max_running_queries`. Recommended: `8`. Alternatives: `0` (unlimited, opt-in admission), or derived from the number of workers (for example 4 × alive workers, recomputed as workers join).
- [x] 0.2 Default `max_queued_queries` and queue-full behavior. Recommended: `256`, then reject with `53000` / `QUERY_QUEUE_FULL`. Alternative: an unbounded queue with a `max_queued_time`.
- [x] 0.3 Memory limit scope. Recommended: per node only now (D1/D2), with a cluster-wide limit as a follow-up. Alternative: also ship a coordinator-side cluster-wide sum check, enforced by polling task status (laggy, see D1).
- [x] 0.4 Default `query_max_memory_per_node`. Recommended: unset (opt-in) until Tier 2 numbers exist. Alternative: default to a fraction (for example 0.8) of the node spill budget when one is set.
- [x] 0.5 Default `statement_timeout`. Recommended: `0` (off, PostgreSQL's default). Alternative: a generous server default such as `1h`.

## 1. PR A — per-query memory pools and typed resource errors (D1–D3)

- [x] 1.1 `TaskManager`: add `HashMap<QueryId, QueryEntry { pool: Arc<QueryMemoryPool>, tasks: HashMap<TaskId, AbortHandle> }>`. `execute_task` gets or creates the entry and builds `ExecutionContext` with the query's pool. Remove the entry when the last task's buffers are cleaned up.
- [x] 1.2 `main.rs`: remove the process-wide `QueryMemoryPool` wrap. Pass the node pool plus the resolved `query_max_memory_per_node` into `TaskManager`, `ProtocolServer` and `TrinoServer`. Reword the startup log to say "per-query limit".
- [x] 1.3 Coordinator / standalone: `execute_query` (pgwire) and `run_statement` (Trino) create a per-query pool for local execution and the coordinator's own fragments, and drop it at the end of the query.
- [x] 1.4 Typed errors: the worker records `ResourceExhausted` task failures and `do_get` returns `Status::resource_exhausted`. `ExchangeClient` and the task-status path map that status back to `ExecutionError::ResourceExhausted`. The message names the node and the limit.
- [x] 1.5 pgwire SQLSTATE: map `ExecutionError::ResourceExhausted` to `53200` in `protocol/src/error.rs`. Keep Trino's `EXCEEDED_LOCAL_MEMORY_LIMIT` and add a test for it on the distributed path.
- [x] 1.6 Tests:
  - `QueryMemoryPool` per query: two queries on one pool, one over its limit, the other unaffected.
  - The entry is dropped after the last task, and node reserved bytes return to the baseline.
  - The Flight round trip keeps the `ResourceExhausted` class (rpc integration test).
  - The pgwire `53200` mapping.
  - An end-to-end standalone test: a tiny limit fails `SELECT ... GROUP BY` with `53200`.

## 2. PR B — cluster-wide cancellation (D4)

- [x] 2.1 `rpc`: add the `cancel_query` action (body: query id), a `CancelQueryCallback` on `FlightState`, and the client helper `cancel_query(addr, query_id)` through `rpc::connect`.
- [x] 2.2 Worker: the callback aborts the query's tasks through the stored `AbortHandle`s, removes their buffers from `FlightState`, drops the `QueryEntry`, and logs `query cancelled on worker` (`arneb::cluster`, with the task count). It is idempotent.
- [x] 2.3 Coordinator: a `CancelGuard` in `coordinator.execute` records the workers that received tasks, is disarmed on success, and on any other exit (including `Drop`) sends `cancel_query` from a detached task. Failed deliveries are logged.
- [x] 2.4 The first fragment error fails the query and fires the guard, so the remaining fragments are cancelled.
- [x] 2.5 Tests:
  - An rpc integration test: `cancel_query` aborts a long task, removes its buffer and is idempotent.
  - A server test: dropping the coordinator future sends `cancel_query` to every worker that received a task.
  - A fragment failure cancels its sibling tasks. (Covered by the guard: every early `return Err` / `?` in `execute` drops the still-armed guard, which is the drop path `cancel_guard_cancels_on_drop_unless_disarmed` and `dropping_the_query_future_cancels_on_workers` test; the end-to-end case is in Tier 2 6.4.)
- [x] 2.6 Docs: remove the "worker tasks that are already running may continue" limitation from `docs/guide/trino-clients.md`.

## 3. PR C — admission control (D5, D6)

- [x] 3.1 `scheduler/src/admission.rs`: `AdmissionController::admit(query_id) -> Result<AdmissionPermit, AdmissionError>` with FIFO hand-off on `Drop`, dropped-waiter skipping, `QueueFull`, `snapshot()`, and `0` = unlimited. Remove the unused `ResourceGroup` and its tests.
- [x] 3.2 Config: an `[admission]` section with `max_running_queries`, `max_queued_queries` and `statement_timeout` (the latter used in PR D), `ARNEB_*` overrides through an `env` closure, validation, and the startup log on `arneb::config`.
- [x] 3.3 pgwire: acquire the permit after interception and before planning, in both the Simple and Extended paths. Session and metadata statements and `EXPLAIN` (without `ANALYZE`) bypass it. Hold the permit until execution completes.
- [x] 3.4 Trino: acquire the permit inside the statement task, report `QUEUED` until it is granted, and release it when the statement task completes (not after paging).
- [x] 3.5 Tracker and Web UI: create the tracker entry in `Queued` and move it to `Planning` on admission. Add `running_queries`, `queued_queries`, `max_running_queries` and `max_queued_queries` to `/api/v1/cluster`, and show them on the dashboard. (As implemented: the Trino listener's tracker entry stays `Queued` until admission. A queued pgwire statement has no tracker entry yet — the coordinator creates one when it plans — so pgwire queuing shows in the `/api/v1/cluster` counters and the dashboard's "N queued" line, not in the query list.)
- [x] 3.6 Errors: queue full → pgwire `53000` / Trino `QUERY_QUEUE_FULL` (131074, `INSUFFICIENT_RESOURCES`).
- [x] 3.7 Logging: `arneb::admission` INFO lines on queue (with position) and admit (with wait time), and a DEBUG snapshot every 10 s while anything is queued.
- [x] 3.8 Tests:
  - Controller unit tests: cap, FIFO order, dropped-waiter skipping, queue full, unlimited.
  - pgwire and Trino integration tests with `max_running_queries = 1`: the second query waits and shows `QUEUED`, then completes; a third with `max_queued = 1` gets the queue-full error; `SET` and metadata bypass.

## 4. PR D — statement timeouts (D7, D8)

- [x] 4.1 pgwire session: handle `SET` / `SHOW` / `RESET statement_timeout` in `session.rs` next to `search_path`, with PostgreSQL value parsing and an invalid value → `22023`.
- [x] 4.2 pgwire enforcement: wrap admission plus execution in `tokio::time::timeout` from statement receipt. On expiry, drop the future (PR B cancels the workers) and return `57014 canceling statement due to statement timeout`.
- [x] 4.3 Trino: parse `query_max_execution_time` (timer from admission) and `query_max_run_time` (timer from submission) from the session as Trino durations. On expiry, fail with `EXCEEDED_TIME_LIMIT` (131075) through `cancel_query`. A bad duration → `INVALID_SESSION_PROPERTY`.
- [x] 4.4 Server default `[admission] statement_timeout` / `ARNEB_STATEMENT_TIMEOUT` applies to both protocols when the session sets none.
- [x] 4.5 Tests:
  - Duration parsing (PostgreSQL and Trino forms).
  - pgwire: `SET`/`SHOW`/`RESET`, a timeout fires `57014`, and the connection stays usable.
  - Trino: `EXCEEDED_TIME_LIMIT`, and queue time is excluded from `query_max_execution_time`.
  - The server default applies.

## 5. Tier 1 — local correctness gate (every PR)

- [x] 5.1 `cargo fmt -- --check` and `cargo clippy --workspace --all-targets -- -D warnings` are clean.
- [x] 5.2 `cargo test --workspace` is green. With limits off or not reached, all existing tests pass unchanged.

## 6. Tier 2 — local distributed check (coordinator + 2 workers, SF10)

- [x] 6.1 Plumbing (CLAUDE.md rule):
  - The new `arneb::config`, `arneb::admission` and `arneb::cluster` lines appear under the default `RUST_LOG`.
  - `/api/v1/cluster` shows the admission counters.
- [x] 6.2 Concurrency acceptance: 20 concurrent SF10 TPC-H queries (a mix including q09, q18 and q21) with the default `max_running_queries`.
  - Every coordinator and worker process survives (no OOM kill).
  - The Web UI and `/api/v1/cluster` show queued queries draining.
  - Every query returns cell-identical results to its single-query run.
  - Record peak RSS per node and the total wall time, compared with `max_running_queries = 0`.
- [x] 6.3 Memory limit:
  - With `ARNEB_QUERY_MAX_MEMORY_BYTES` set below q18's per-worker peak (about 1.4 GB at SF10, see #126), q18 fails with `53200` over psql and `EXCEEDED_LOCAL_MEMORY_LIMIT` over the trino CLI.
  - A concurrent small query completes.
  - The workers' tracked bytes return to the baseline afterwards (`arneb::mem`).
- [x] 6.4 Timeout:
  - `SET statement_timeout = '500ms'` on q09 gives `57014`.
  - Every worker logs `query cancelled on worker`, no task of that query is left in `FlightState`, and the per-query pool is gone.
  - The same holds for `query_max_execution_time=1s` via the trino CLI and for a Trino `DELETE` mid-query.
- [x] 6.5 No regression: run the SF10 22-query bench (`run_benchmark.sh --scenario=sf10`) with defaults against the pre-change baseline. Latency and memory geomeans should be within run-to-run noise, using Trino as the drift control. Record both numbers here.
- [x] 6.6 Tier 3 (remote SF30) is not required: this change carries no performance claim. An optional SF30 run of 6.3 (memory limit under real pressure) can ride along with the deferred P1 SF30 tasks.

### Tier 2 results (2026-10-17)

Native release build, coordinator + 2 workers on one host, against the docker-compose Hive SF10 data (re-seeded for #135). Admission at its defaults unless stated.

- **6.1 plumbing.** Under the default `RUST_LOG` the coordinator logs `admission effective settings ... max_running_queries=8 max_queued_queries=256 statement_timeout=0 query_max_memory_per_node=off` on `arneb::config`, workers log `worker per-query memory limit`, `arneb::admission` logs `query queued` / `query admitted ... waited_ms=7989`, and `arneb::cluster` logs the cancel lines in 6.4. `/api/v1/cluster` returns `running_queries`, `queued_queries`, `max_running_queries` and `max_queued_queries`.
- **6.2 concurrency (20 queries: q09, q18, q21, q01, q03, q05, q06, q10, q12, q14, each twice).** All three processes survived every run. Results compared with single-query runs of the same queries (relative 1e-12):

  | Build / cap | Running max | Queued max | Worker RSS peak | Wall | Identical | Failed |
  |---|---|---|---|---|---|---|
  | this change, cap 8 | 8 | 12 | 3.20 / 3.17 GB | 31.7 s | 19/19 | 1 (q03) |
  | this change, cap 0 | 20 | 0 | 3.77 / 3.74 GB | 36.0 s | 18/18 | 2 (q03, q18) |
  | `main` (no admission) | 20 | — | 3.82 / 3.86 GB | 36.6 s | 19/19 | 1 (q03) |

  The queue drained (8 running / 11 queued at 4 s, 0 queued at 20 s). Every failure, **including on `main`**, is the same pre-existing exchange fault: a worker's Flight `do_get` on another worker's broadcast buffer ends with `h2 protocol error: error reading a body from connection` / `broken pipe` under concurrent load. It is not caused by this change (it reproduces on `main`), and the cancel guard then cancelled the failed query on both workers as designed. Tracked as a follow-up.
- **6.3 memory limit (`ARNEB_QUERY_MAX_MEMORY_BYTES=300000000`).** q18 fails over psql with `ERROR 53200: stream error during execution: resource exhausted: query memory limit exceeded: consumer 'HashAggregateExec.merge' requested 15108272 bytes; query used 285459736/300000000 bytes (per-query cap)`, and over the trino CLI / REST with `EXCEEDED_LOCAL_MEMORY_LIMIT` (`INSUFFICIENT_RESOURCES`). q06 run alongside completes with the right result. Afterwards both workers report `tracked_now_bytes` 0 / 104. **Tier 2 found a bug here** (first run: `XX000`): an operator re-wrapped the remote ResourceExhausted as InvalidOperation, so the class was lost at the next exchange hop. Fixed by keeping the tag in the text and classifying at the client-facing edge; regression test `class_survives_operator_rewrapping_over_two_hops`.
- **6.4 timeout and cancel (q09).** `SET statement_timeout = '500ms'` gives `ERROR 57014: canceling statement due to statement timeout` and the session still answers `SHOW statement_timeout` (`500ms`). The trino CLI with `--session query_max_execution_time=1s` fails with `Query exceeded the maximum execution time limit of 1s`. A Trino `DELETE` on a RUNNING query returns 204. For each of the three, the coordinator logs `cancelling query on workers workers=2`, worker-1 logs `query cancelled on worker tasks=9` and worker-2 `tasks=8`; no task of those queries completes after the cancel, and the next query sees `tracked_now_bytes=104` on both workers.
- **6.5 no regression.** `run_benchmark.sh --scenario=sf10 --mode=both` with the image of this branch (`memory_total_20261017_125722`, defaults: cap 8, no memory limit, no timeout) against `memory_total_20261017_004235` (same seed; main + P1 scan accounting, which measured ~1.0× on its own). 22/22 OK. Geomean ratios: arneb latency 0.985× cold / 0.975× warm, memory 1.019× / 1.005×; Trino drift 0.982× / 0.987×, memory 1.023× / 0.966×. Worst single query: arneb q15 warm 1.08×, inside the documented per-query noise. No regression attributable to this change.
- **6.6.** Tier 3 not run (no performance claim).

## 7. Docs

- [x] 7.1 `docs/guide/configuration.md`:
  - An Admission section covering the cap, the queue, queue-full behavior, the knobs and the logs.
  - The corrected `query_max_memory_per_node` description (per query, per node), with sizing guidance.
  - The new `ARNEB_*` variables.
- [x] 7.2 `docs/guide/trino-clients.md`:
  - `query_max_execution_time` and `query_max_run_time` are now enforced.
  - Document `QUEUED`, `QUERY_QUEUE_FULL` and `EXCEEDED_TIME_LIMIT`.
  - Remove the "session properties are not interpreted" claim for these two properties.
- [x] 7.3 `docs/sql/overview.md` (or the pgwire section): `SET statement_timeout`, and the error codes `53200`, `53000` and `57014`.
- [x] 7.4 CHANGELOG notes for the three intentional behavior changes: `query_max_memory_per_node` now applies per query, admission is on by default, and memory errors are reported as `53200`. (As implemented: `CHANGELOG.md` is generated by release-please from commit messages, so the three changes are stated in the commit bodies and the PR description rather than edited into the file.)
