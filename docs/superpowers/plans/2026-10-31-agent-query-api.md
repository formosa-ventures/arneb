# H2 `agent-query-api` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** H2 adds an agent query layer to `/api/v2`. It covers six things:
- bounded discovery of catalogs, schemas, tables and columns
- a bounded table sample
- `EXPLAIN` with scan estimates
- async statements: submit, poll, fetch pages, cancel
- row, byte and cell limits, with truncation reported
- structured errors that carry a SQL position and nearest-name hints

**Architecture:**
- **One service in the protocol crate.** A new public module, `arneb_protocol::agent`, owns `AgentQueryService`. It runs SQL through the Trino module's private `Engine`, so an agent gets the same semantics as a Trino client. That includes access control, `information_schema`, `system.runtime.queries`, admission, the statement timeout and query-tracker registration.
- **Copied state machine.** The service keeps its own statement registry and reaper, copied from the Trino `Query`/`Inner`/`Phase` design rather than shared with it.
- **The server crate stays thin.** It only translates HTTP into service calls (`crates/server/src/web/agent.rs`). It also maps `AgentError` onto the H1 error envelope, which gains `location` and `candidates`.

**Tech Stack:** Rust (axum 0.8, serde/serde_json, tokio, thiserror, tracing). No new dependencies.

**Spec:** `docs/superpowers/specs/2026-10-30-headless-agent-interface-design.md` (stage H2: Goals 3 and 4, the query layer). This builds on H1, on branch `feat/headless-api-auth` (PR #215).

## Global Constraints

- **Logging and errors.** Use `tracing`, not `log`. Library errors use `thiserror`. Public APIs get doc comments.
- **Runtime knobs.** Every limit and timeout is a runtime knob:
  - It has a `[agent_api]` TOML field and an `ARNEB_AGENT_*` env override.
  - Precedence is CLI > env > file > defaults.
  - The effective values are logged once on `arneb::config` at startup.
- **No blocking calls.** No request waits longer than `max_wait_ms`. Long statements are submitted, polled, fetched and cancelled.
- **Bounded results.** Every response that returns data is bounded by rows and by bytes. When something is cut, the response says so and gives the full count when it is known.
- **Equal rights.**
  - An API statement runs with exactly the visibility and privileges the same user has over Trino or pgwire. No new privilege checks; write privileges are H4.
  - An object the user may not see answers exactly like one that does not exist.
- **Statements are per user.** Only the submitting user can poll, fetch or cancel a statement. Any other user gets `not_found`. Cancelling *any* query is H4.
- **Nothing else changes.** `/api/v1`, the existing `/api/v2` routes, pgwire behavior, and the Trino JSON fields that already exist are unchanged. Trino errors only *gain* `errorLocation`.
- **Coordinator and standalone only.** Workers do not build the service.
- **Validation tiers.** Tier 1 is `cargo test --workspace`, `cargo fmt -- --check`, and `cargo clippy --workspace --all-targets -- -D warnings`. Tier 2 is the local distributed stack (Task 8). The PR names the tier behind each claim.
- **Housekeeping.** At the start, run `./scripts/target-housekeeping.sh`. Use `--sweep` at 🟡.

## Decisions (my calls; recorded in the OpenSpec design)

- **Routes**, all under `/api/v2` and all authenticated by the H1 middleware:

  | Method and path | Purpose |
  |---|---|
  | `GET /catalogs` | list catalogs |
  | `GET /catalogs/{c}/schemas` | list schemas |
  | `GET /catalogs/{c}/schemas/{s}/tables` | list tables |
  | `GET /catalogs/{c}/schemas/{s}/tables/{t}` | describe a table: columns plus statistics |
  | `GET /catalogs/{c}/schemas/{s}/tables/{t}/sample?rows=` | sample a table |
  | `POST /explain` | explain a statement |
  | `POST /statements` | submit |
  | `GET /statements/{id}?wait_ms=` | poll |
  | `GET /statements/{id}/results?cursor=&max_bytes=` | fetch a page |
  | `DELETE /statements/{id}` | cancel |

  The listings accept `?prefix=&limit=`.
- **Statement ids** are the query tracker's `QueryId` string, so `/api/v2/queries/{id}` shows the same query.
- **Retention.** A statement keeps at most `max_rows` rows after it finishes, and its total row count. The rest are dropped at completion. Memory during execution is bounded by the existing query memory limit, as on the Trino path.
- **Inline first page.** `submit` and `status` accept `wait_ms`, which defaults to 0 and is capped at `max_wait_ms`. A statement that finishes inside the wait carries its first page inline.
- **Sample** submits `SELECT * FROM "c"."s"."t" LIMIT n` with `wait_ms = max_wait_ms`, so it behaves like a short statement. `rows` defaults to 10 and is capped at 100.
- **Explain** is synchronous planning only. It is not admitted, not tracked, and does not support `ANALYZE`; an agent submits `EXPLAIN ANALYZE` as a statement.
- **Session statements are refused.** The API is stateless per statement. Statements that `trino::statements::parse_control` recognizes, such as `USE`, `SET SESSION` and `PREPARE`, are refused with `invalid_argument`, hint `pass "catalog" and "schema" in the request body`. `SHOW` is not refused.
- **Limits** (`[agent_api]` field, then env var, then default):

  | `[agent_api]` field | Env var | Default |
  |---|---|---|
  | `default_max_rows` | `ARNEB_AGENT_DEFAULT_MAX_ROWS` | 1000 |
  | `max_rows` | `ARNEB_AGENT_MAX_ROWS` | 10000 |
  | `default_page_bytes` | `ARNEB_AGENT_DEFAULT_PAGE_BYTES` | 262144 |
  | `max_page_bytes` | `ARNEB_AGENT_MAX_PAGE_BYTES` | 4194304 |
  | `max_cell_bytes` | `ARNEB_AGENT_MAX_CELL_BYTES` | 8192 |
  | `max_wait_ms` | `ARNEB_AGENT_MAX_WAIT_MS` | 10000 |
  | `idle_timeout_secs` | `ARNEB_AGENT_IDLE_TIMEOUT_SECS` | 600 |

  Two caps are constants: 32 live statements per user and 1024 in total (`overloaded`). A request value above its ceiling is refused with `invalid_argument`, not clamped.
- **Idle timeout.** After `idle_timeout_secs` with no request for a statement, the reaper does one of two things:
  - it drops a terminal statement
  - it cancels a running one, recording the reason `abandoned`
- **Error codes** are added to the H1 `ErrorCode`, all snake_case:

  | Code | HTTP status |
  |---|---|
  | `syntax_error` | 400 |
  | `catalog_not_found` | 404 |
  | `schema_not_found` | 404 |
  | `table_not_found` | 404 |
  | `column_not_found` | 400 |
  | `function_not_found` | 400 |
  | `type_mismatch` | 400 |
  | `not_supported` | 400 |
  | `query_failed` | 400 |
  | `exceeded_memory` | 400 |
  | `exceeded_time` | 400 |
  | `cancelled` | 409 |

  - The existing `forbidden`, `overloaded`, `invalid_argument`, `not_found` and `internal` are reused.
  - A statement that fails after submission is a 200 status body whose `state` is `failed` and which carries an `error`. HTTP error statuses are only for the request itself.

## Review Focus

1. **A huge `SELECT` must not reach the agent unbounded, in rows or in bytes.**
   - Six million rows yield `max_rows` rows, `truncated: true` and `total_rows` = the true count.
   - A single 10 MB string cell is cut to `max_cell_bytes`, and the cut is counted.
   - Tests: `large_result_is_truncated_with_total` and `oversized_cell_is_cut_and_counted` (Task 4).
2. **Polling must never block beyond `max_wait_ms`, even when admission is full.** A queued statement's poll returns `queued` with `queue_position`. Test: `poll_returns_within_wait_while_queued` (Task 3).
3. **Another user must not learn that a statement or table exists.** Their poll, fetch or cancel returns the same `not_found` as a random id. A table hidden by the rules answers exactly like a missing one. Tests: `other_users_statement_is_not_found` (Task 3) and `hidden_table_is_indistinguishable_from_missing` (Task 5).
4. **A wrong column name must come back with a position and real suggestions.** It must not list columns from tables the user cannot see. Test: `unknown_column_has_location_and_candidates` (Task 1), plus the HTTP check in Task 7.
5. **An abandoned statement must free its admission slot.** The reaper cancels it after the idle timeout. Test: `abandoned_running_statement_is_cancelled_and_releases_permit` (Task 3).

---

### Task 1: Structured error details (position and candidates)

**Files:**
- Create: `crates/common/src/suggest.rs`. Export it from `crates/common/src/lib.rs` as `pub mod suggest`.
- Modify: `crates/common/src/error.rs`
  - `PlanError::ColumnNotFound` gains `candidates: Vec<String>`.
  - `column_not_found()` sets it to an empty vec.
  - Add `ParseError::location(&self) -> Option<Location>`, which parses sqlparser's `Line: N, Column: M` text. It replaces the method that always returns `None`.
- Modify: `crates/planner/src/planner.rs:113`. Fill `candidates` from the resolver's in-scope column names.
- Modify: `crates/protocol/src/trino/error.rs`
  - `TrinoError` gains `pub(crate) location: Option<Location>` and `pub(crate) candidates: Vec<String>`. `from_arneb` fills both.
  - `to_json()` adds `"errorLocation": {"lineNumber": L, "columnNumber": C}` when a location is present.
- Update every other `ColumnNotFound { .. }` construction site that the compiler flags.

**Interfaces:**
- Produces:
  - `arneb_common::suggest::nearest(target: &str, candidates: impl IntoIterator<Item = impl AsRef<str>>, max: usize) -> Vec<String>`
    - Matching is case-insensitive Levenshtein distance.
    - It keeps candidates within `max(2, target.len() / 3)` of the target.
    - Results are ordered by distance, then by name, and deduplicated. At most `max` are returned.
    - The planner uses `max = 3`.
  - `TrinoError.location` and `TrinoError.candidates` (used by Task 3).

- [ ] **Step 1: Write the failing tests**

```rust
// common/src/suggest.rs
#[test] fn nearest_finds_typo_and_orders_by_distance()
// nearest("l_quantiy", ["l_quantity","l_orderkey","l_tax"], 3) == ["l_quantity"]
#[test] fn nearest_is_case_insensitive_and_bounded()
// nearest("NAME", ["n_name","name","nation"], 2) == ["name","n_name"]
#[test] fn nearest_returns_empty_when_nothing_is_close()
// nearest("zzzz", ["l_tax"], 3).is_empty()

// common/src/error.rs
#[test] fn parse_error_location_is_read_from_sqlparser_text()
// ParseError::InvalidSyntax("Expected: ..., found: FORM at Line: 2, Column: 8".into()).location()
//   == Some(Location { line: 2, column: 8 })

// planner/src/planner.rs
#[tokio::test] async fn unknown_column_has_location_and_candidates()
// plan "SELECT l_quantiy FROM lineitem" (single line) → Err(PlanError::ColumnNotFound { name: "l_quantiy",
//   location: Some(Location { line: 1, column: 8 }), candidates }) with candidates[0] == "l_quantity"

// protocol/src/trino/error.rs
#[test] fn trino_json_carries_error_location()
// from_arneb(ColumnNotFound at 1:8) → to_json()["errorLocation"] == {"lineNumber":1,"columnNumber":8}
```

- [ ] **Step 2:** Run `cargo test -p arneb-common -p arneb-planner -p arneb-protocol`. Expected: the new tests fail to compile or fail.
- [ ] **Step 3: Implement.** The `ColumnNotFound` Display text stays `column not found: {name}`. The candidates do not go into the message; the API puts them in the hint (Task 3).
- [ ] **Step 4:** Run `cargo test --workspace`. Expected: PASS. Existing Trino error tests are unchanged apart from the added field.
- [ ] **Step 5: Commit** `feat(errors): SQL position and nearest-column candidates on plan and parse errors`

### Task 2: `[agent_api]` limits

**Files:**
- Create: `crates/protocol/src/agent/mod.rs`. Add `pub mod agent;` in `crates/protocol/src/lib.rs`.
- Create: `crates/protocol/src/agent/limits.rs`
- Modify: `crates/server/src/config.rs`. Add `AgentApiConfig` with the 7 fields in the Decisions table, `#[serde(default)]`, and `apply_env(&mut self, env: impl Fn(&str) -> Option<String>) -> Result<()>` following `AdmissionConfig::apply_env` (config.rs:359). Hook it into `AppConfig` like `[admission]`.
- Modify: `crates/server/src/main.rs`. Log the effective values once, on `arneb::config`, in the admission log's style (main.rs:918).
- Modify: `docs/guide/configuration.md`. Add an `[agent_api]` section with the table of fields, env vars and defaults.

**Interfaces:**
- Produces:
  - `pub struct AgentLimits` with these fields, all `pub`:
    - `default_max_rows: usize`, `max_rows: usize`
    - `default_page_bytes: usize`, `max_page_bytes: usize`
    - `max_cell_bytes: usize`
    - `max_wait: Duration`, `idle_timeout: Duration`
  - `impl Default for AgentLimits` (the values from the Decisions table).
  - `AgentLimits::validate(&self) -> Result<(), String>`. Every value must be greater than 0, `default_max_rows <= max_rows`, and `default_page_bytes <= max_page_bytes`.
  - `AgentApiConfig::limits(&self) -> AgentLimits`

- [ ] **Step 1: Write the failing tests** (in `config.rs` tests)

```rust
#[test] fn agent_api_defaults_match_documented_values()
// AppConfig::default().agent_api.limits() == AgentLimits::default(); max_rows == 10000; max_wait == 10s
#[test] fn agent_api_env_overrides_file()
// file max_rows = 500, env ARNEB_AGENT_MAX_ROWS=700 → 700
#[test] fn agent_api_rejects_default_above_max()
// default_max_rows = 2000, max_rows = 1000 → config load error mentioning "default_max_rows"
#[test] fn agent_api_rejects_unparsable_env()
// ARNEB_AGENT_MAX_WAIT_MS=abc → error naming the variable
```

- [ ] **Step 2:** Run `cargo test -p arneb-server config`. Expected: FAIL.
- [ ] **Step 3: Implement** the code.
- [ ] **Step 4:** Run `cargo test -p arneb-server config`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(config): [agent_api] limits with ARNEB_AGENT_* overrides`

### Task 3: Statement lifecycle (submit, poll, cancel, reaper)

**Files:**
- Create: `crates/protocol/src/agent/service.rs`, `crates/protocol/src/agent/statement.rs`, `crates/protocol/src/agent/error.rs`
- Modify: `crates/protocol/src/trino/session.rs`. Add `pub(crate) fn ClientSession::for_agent(user: &str, catalog: Option<String>, schema: Option<String>) -> ClientSession`, with no properties and no prepared statements.
- Modify: `crates/protocol/src/trino/mod.rs` only to expose to the crate what the service needs: `Engine` construction, `admit`/`needs_admission`, and the deadline helper.

**Interfaces:**
- Consumes:
  - `Engine::run_statement(&ClientSession, &str) -> Result<QueryOutput, TrinoError>`
  - `parse_control`
  - `QueryTracker::{create_query_from, advance_to, finish_query, fail_query, cancel_query}` (`create_query_from` uses source `"api"`)
  - `AdmissionController::admit_with` → `Pending::{queue_position, wait}`
  - `AgentLimits` (Task 2)
  - `TrinoError.location` and `TrinoError.candidates` (Task 1)
- Produces (all in `arneb_protocol::agent`, re-exported from `mod.rs`):
  - **Service type:** `#[derive(Clone)] pub struct AgentQueryService`
  - **Construction:** `new(catalog_manager: Arc<CatalogManager>, connector_registry: Arc<ConnectorRegistry>, query_tracker: Arc<QueryTracker>, limits: AgentLimits)`, plus these builders, which mirror `TrinoServer`:
    - `with_distributed_executor`, `with_memory_pool`, `with_query_memory_limit`
    - `with_admission`, `with_statement_timeout`, `with_access_rules`
  - **Reaper:** `pub fn spawn_reaper(&self) -> tokio::task::JoinHandle<()>`
  - **Lifecycle methods:**
    - `pub async fn submit(&self, user: &str, req: SubmitRequest, wait: Duration) -> Result<StatementStatus, AgentError>`
    - `pub async fn status(&self, user: &str, id: &str, wait: Duration) -> Result<StatementStatus, AgentError>`
    - `pub fn cancel(&self, user: &str, id: &str) -> Result<StatementStatus, AgentError>`
    - `cancel` is idempotent: on a terminal statement it returns the current status.
  - **`SubmitRequest`** (`Deserialize`): `{ sql: String, catalog: Option<String>, schema: Option<String>, max_rows: Option<usize> }`
  - **`StatementStatus`** (`Serialize`):
    - Identity and progress: `id`, `state` (`"queued" | "running" | "finished" | "failed" | "cancelled"`), `queue_position: Option<usize>`, `elapsed_ms: u64`
    - Results: `update_type: Option<String>`, `columns: Option<Vec<Column{name, type}>>`, `total_rows: Option<u64>`, `truncated: bool`, `first_page: Option<ResultPage>` (filled by Task 4)
    - Failure: `error: Option<AgentError>`
  - **`AgentError`** (`Serialize`, `thiserror`):
    - `{ code: AgentErrorCode, message: String, hint: Option<String>, location: Option<SqlLocation{line, column}>, candidates: Vec<String> }`
    - `candidates` is skipped when empty; `location` and `hint` are skipped when `None`.
  - **`AgentErrorCode`:** one variant per code in the Decisions table, plus `Forbidden`, `Overloaded`, `InvalidArgument`, `NotFound` and `Internal`.
  - **`AgentError::from_trino(&TrinoError) -> AgentError`** maps by Trino error name:

    | Trino error | Agent code |
    |---|---|
    | `SYNTAX_ERROR` | `syntax_error` |
    | `CATALOG_NOT_FOUND`, `SCHEMA_NOT_FOUND`, `TABLE_NOT_FOUND`, `COLUMN_NOT_FOUND`, `FUNCTION_NOT_FOUND`, `TYPE_MISMATCH` | the snake_case name |
    | `NOT_SUPPORTED` | `not_supported` |
    | `PERMISSION_DENIED` | `forbidden` |
    | `EXCEEDED_*_MEMORY_LIMIT` | `exceeded_memory` |
    | `EXCEEDED_TIME_LIMIT` | `exceeded_time` |
    | `USER_CANCELED` | `cancelled` |
    | `QUERY_QUEUE_FULL` | `overloaded` |
    | other user errors | `query_failed` |
    | internal errors | `internal` |

  - **Hints:**
    - column not found with candidates: `did you mean: a, b?`
    - column not found without candidates: `list columns with GET /api/v2/catalogs/{catalog}/schemas/{schema}/tables/{table}`
    - table not found: `list tables with GET /api/v2/catalogs/{catalog}/schemas/{schema}/tables`
    - syntax error with a location: `check the SQL near line L, column C`
    - overloaded: `retry after a running statement finishes`
  - **Cap rejection:** the per-user cap of 32 and the total cap of 1024 return `overloaded`, before the statement is registered with the tracker.

- [ ] **Step 1: Write the failing tests** (`service.rs` tests, over an in-memory catalog, following the Trino module's test setup)

```rust
#[tokio::test] async fn submit_then_poll_reaches_finished()
// submit "SELECT 1" wait 0 → state in {queued, running, finished}; status(wait 2s) → finished, total_rows == Some(1)
#[tokio::test] async fn submit_with_wait_returns_finished_inline()
#[tokio::test] async fn poll_returns_within_wait_while_queued()
// admission max_running=1 held by a sleeping statement; second submit → status(wait 200ms) returns within ~300ms,
// state == "queued", queue_position == Some(1)
#[tokio::test] async fn cancel_running_statement_marks_cancelled_and_tracker_cancelled()
#[tokio::test] async fn cancel_is_idempotent_on_terminal_statement()
#[tokio::test] async fn other_users_statement_is_not_found()
// bob's status/cancel on alice's id → AgentErrorCode::NotFound, same message as for a random id
#[tokio::test] async fn session_statements_are_refused_with_hint()
// "USE tpch.tiny" → InvalidArgument, hint contains "catalog"
#[tokio::test] async fn failed_statement_reports_structured_error()
// "SELECT l_quantiy FROM lineitem" → state "failed", error.code ColumnNotFound, location Some(1:8),
// candidates[0] == "l_quantity", hint starts with "did you mean"
#[tokio::test] async fn per_user_cap_returns_overloaded()
#[tokio::test(start_paused = true)] async fn abandoned_running_statement_is_cancelled_and_releases_permit()
// idle_timeout 1s; advance 2s; reaper tick → state "cancelled", admission snapshot running == 0
#[tokio::test(start_paused = true)] async fn idle_finished_statement_is_dropped()
// after idle_timeout → status → NotFound
#[tokio::test] async fn statement_id_is_the_tracker_query_id()
```

- [ ] **Step 2:** Run `cargo test -p arneb-protocol agent::`. Expected: FAIL.
- [ ] **Step 3: Implement.**
  - The spawned task follows the order in `trino/mod.rs` `submit` (mod.rs:511-760):
    1. register with the tracker
    2. admit, when `needs_admission`
    3. `advance_to(Running)`
    4. `run_statement` under the statement timeout
    5. `finish_query` / `fail_query`
  - Every `status` and `cancel` call sets `last_access`.
  - The reaper ticks every `min(idle_timeout / 4, 30 s)`.
  - Retaining rows is Task 4. This task stores the `QueryOutput` as is.
- [ ] **Step 4:** Run `cargo test -p arneb-protocol`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(agent): async statement lifecycle over the Trino engine`

### Task 4: Bounded results (rows, pages, cells)

**Files:**
- Create: `crates/protocol/src/agent/results.rs`
- Modify: `crates/protocol/src/agent/statement.rs` and `service.rs`

**Interfaces:**
- Consumes:
  - `trino::types::{encode_cell, materialize, trino_type_of}`, made `pub(crate)` if they are not already
  - `AgentLimits`
- Produces:
  - **Retention at completion.** `fn retain(batches: Vec<RecordBatch>, max_rows: usize) -> Retained { batches, total_rows: u64, truncated: bool }`
    - It slices the batches so that at most `max_rows` rows are kept.
    - The `max_rows` used is `req.max_rows.unwrap_or(default_max_rows)`.
    - A request above `max_rows` is refused at submit with `invalid_argument`.
  - **`ResultPage`** (`Serialize`): `{ rows: Vec<Vec<serde_json::Value>>, next_cursor: Option<u64>, truncated_cells: u64 }`
    - Cursors are row offsets into the retained rows, starting at 0.
    - A page holds whole rows until adding the next row would exceed `max_bytes`. Size is measured as the serialized row length plus 1, and a page always has at least 1 row.
    - A cell whose serialized JSON exceeds `max_cell_bytes` is replaced with a JSON string. The string is the first `max_cell_bytes` bytes of its text form, cut on a char boundary, followed by `…`. Each replacement is counted in `truncated_cells`.
  - **Fetching a page:**
    - `pub fn results(&self, user: &str, id: &str, cursor: u64, max_bytes: Option<usize>) -> Result<ResultPage, AgentError>`
    - It returns `invalid_argument` in two cases: the statement has not finished, or the cursor is past the end of the retained rows.
  - **First page on the status.** `StatementStatus` gains `max_rows: usize` and `first_page`. `first_page` is filled whenever the status is `finished` and `default_page_bytes` applies.
  - `columns` uses Trino type display names, such as `bigint` and `varchar(25)`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn retain_slices_across_batches_and_counts_total()
// batches of 3+3+3 rows, max_rows 4 → 4 rows kept, total_rows 9, truncated true
#[tokio::test] async fn large_result_is_truncated_with_total()
// table of 50_000 rows, max_rows 1000 → total_rows 50_000, truncated, sum of all pages' rows == 1000
#[tokio::test] async fn pages_respect_max_bytes_and_cursor_walks_all_rows()
// max_bytes 1024 → every page's serialized rows ≤ 1024 + one-row slack; following next_cursor yields each row once, in order
#[tokio::test] async fn oversized_cell_is_cut_and_counted()
// one 100_000-char varchar, max_cell_bytes 8192 → cell is a string of ≤ 8192 bytes + "…", truncated_cells == 1
#[tokio::test] async fn results_before_finish_is_invalid_argument()
#[tokio::test] async fn cursor_past_end_is_invalid_argument()
#[tokio::test] async fn max_rows_above_ceiling_is_refused()
#[tokio::test] async fn ddl_reports_update_type_without_rows()
// "CREATE TABLE memory.default.t AS SELECT 1 AS x" → update_type Some(..), first_page rows match the Trino path
```

- [ ] **Step 2:** Run `cargo test -p arneb-protocol agent::`. Expected: FAIL.
- [ ] **Step 3: Implement** the code.
- [ ] **Step 4:** Run `cargo test -p arneb-protocol`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(agent): bounded result pages with row, byte and cell limits`

### Task 5: Discovery, describe and sample

**Files:**
- Create: `crates/protocol/src/agent/discovery.rs`

**Interfaces:**
- Consumes:
  - `trino::metadata::{catalog_names, schema_names, table_names, table_columns}`, called on the access-scoped root that `Engine::session_root` returns. That keeps visibility identical to `SHOW`.
  - `TableProvider::statistics()`
  - `submit`
- Produces:
  - **`ListRequest`** (`Deserialize`): `{ prefix: Option<String>, limit: Option<usize> }`
    - `prefix` is matched case-insensitively.
    - `limit` defaults to `default_max_rows`. Above `max_rows` it is `invalid_argument`.
  - **`Listing`** (`Serialize`): `{ items: Vec<Named{name}>, total: usize, truncated: bool }`
    - Items are sorted by name.
    - `total` counts the matches before the limit.
  - **Listing methods:**
    - `pub async fn list_catalogs(&self, user: &str, req: &ListRequest) -> Result<Listing, AgentError>`
    - `list_schemas(&self, user, catalog: &str, req)`
    - `list_tables(&self, user, catalog: &str, schema: &str, req)`
  - **Describe:**
    - `pub async fn describe_table(&self, user: &str, catalog: &str, schema: &str, table: &str) -> Result<TableDescription, AgentError>`
    - `TableDescription { catalog, schema, table, columns: Vec<ColumnDescription{name, type, nullable}>, statistics: TableStats{row_count: Option<u64>, size_bytes: Option<u64>} }`
  - **Sample:**
    - `pub async fn sample(&self, user: &str, catalog: &str, schema: &str, table: &str, rows: Option<usize>) -> Result<StatementStatus, AgentError>`
    - `rows` defaults to 10 and is capped at 100. Above the cap it is `invalid_argument`.
    - Identifiers are double-quoted with `"` doubled.
    - It waits up to `max_wait`.
  - **Not-found errors:**
    - A missing or hidden catalog, schema or table returns `catalog_not_found`, `schema_not_found` or `table_not_found`.
    - `candidates` come from `suggest::nearest` over the *visible* names at that level, with `max = 3`.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn list_catalogs_matches_show_catalogs()
#[tokio::test] async fn listing_honors_prefix_limit_and_reports_total()
// 25 tables, prefix "t", limit 10 → 10 items, total 25, truncated true
#[tokio::test] async fn hidden_table_is_indistinguishable_from_missing()
// rules hide orders from bob: describe(orders) and describe(nosuch) → same code, same message shape; neither lists "orders" in candidates
#[tokio::test] async fn describe_returns_columns_types_and_stats()
// file/memory table → columns in order with Trino types; statistics.row_count Some for a parquet file table, None where unknown
#[tokio::test] async fn table_not_found_suggests_visible_neighbours()
// describe("lineitm") → candidates[0] == "lineitem"
#[tokio::test] async fn sample_returns_bounded_rows_inline()
// sample(lineitem, rows 5) → finished, first_page.rows.len() == 5
#[tokio::test] async fn sample_quotes_identifiers()
// a table named with a double quote character samples without a syntax error
```

- [ ] **Step 2:** Run `cargo test -p arneb-protocol agent::discovery`. Expected: FAIL.
- [ ] **Step 3: Implement** the code.
- [ ] **Step 4:** Run `cargo test -p arneb-protocol`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(agent): bounded discovery, describe and sample`

### Task 6: Explain with scan estimates

**Files:**
- Create: `crates/protocol/src/agent/explain.rs`
- Modify: `crates/protocol/src/trino/engine.rs`. Add a crate-private `Engine::plan_for_explain(&self, session: &ClientSession, sql: &str) -> Result<(String, Vec<ScanEstimate>), TrinoError>`. It parses and plans with `QueryPlanner::plan_statement_with_context` and the optimizer over the same scoped root `run` would use, including the virtual catalogs. It returns the plan's Display text (identical to `EXPLAIN`) and one estimate per `LogicalPlan::TableScan`.

**Interfaces:**
- Produces:
  - `ExplainRequest { sql, catalog: Option<String>, schema: Option<String> }`
  - `Explanation { plan: String, scans: Vec<ScanEstimate{table: String /* c.s.t */, row_count: Option<u64>, size_bytes: Option<u64>}>, estimated_scan_bytes: Option<u64> }`
    - `estimated_scan_bytes` is the sum of `size_bytes` only when every scan knows it. Otherwise it is `None`, which means unknown.
  - `pub async fn explain(&self, user: &str, req: &ExplainRequest) -> Result<Explanation, AgentError>`
  - Refusals:
    - A statement with no query plan (DDL, `USE`, …) → `invalid_argument`, hint `explain applies to queries`.
    - An `EXPLAIN …` input → `invalid_argument`, hint `send the query without EXPLAIN`.
  - Errors carry a location and candidates exactly as the statements do.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn explain_text_matches_explain_statement()
// explain("SELECT ...").plan == the single "plan" cell of submitting "EXPLAIN SELECT ..."
#[tokio::test] async fn explain_sums_known_scan_sizes()
// two file tables with size_bytes → estimated_scan_bytes == sum
#[tokio::test] async fn explain_unknown_size_is_null()
// memory table (no stats) in the scan set → estimated_scan_bytes None, that scan's size_bytes None
#[tokio::test] async fn explain_refuses_ddl_and_nested_explain()
#[tokio::test] async fn explain_error_has_location()
#[tokio::test] async fn explain_does_not_register_with_tracker()
```

- [ ] **Step 2:** Run `cargo test -p arneb-protocol agent::explain`. Expected: FAIL.
- [ ] **Step 3: Implement** the code.
- [ ] **Step 4:** Run `cargo test -p arneb-protocol`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(agent): EXPLAIN with per-scan estimates`

### Task 7: HTTP routes, envelope and wiring

**Files:**
- Create: `crates/server/src/web/agent.rs`. It holds the handlers for the routes in the Decisions table.
- Modify: `crates/server/src/web/v2.rs`. Merge `agent::routes()`.
- Modify: `crates/server/src/web/error.rs`
  - Add the codes from the Decisions table, with their statuses.
  - `ApiError` gains `location: Option<SqlLocation>` and `candidates: Vec<String>`, serialized inside `error`, and skipped when empty or `None`.
  - Add `impl From<AgentError> for ApiError` with an exhaustive match.
- Modify: `crates/server/src/web/mod.rs`. `WebState` gains `agent: Option<AgentQueryService>`. When it is `None` (a worker), every agent route returns `not_found`.
- Modify: `crates/server/src/main.rs`
  - Build `AgentQueryService` next to the `TrinoServer` (main.rs:~990), from the same handles: tracker, admission, statement timeout, access rules, memory pool and limit, distributed executor.
  - Build it even when `--no-trino` is set.
  - Call `spawn_reaper()`.
- Modify: `docs/guide/http-api.md`. Add a "Querying" section: the routes, request and response examples, limits and truncation, the error codes table, and a worked curl session.

**Interfaces:**
- Consumes: every `AgentQueryService` method (Tasks 3–6), and `Extension<Identity>` (H1 `web/auth.rs:49`) for the user.
- Produces: the HTTP surface. Query parameters:
  - `wait_ms`, a u64. Above `max_wait_ms` it is `invalid_argument`.
  - `cursor`, which defaults to 0.
  - `max_bytes`
  - `rows`
  - `prefix`
  - `limit`
- Response status:
  - `submit` returns 201 with the `StatementStatus` and a `Location: /api/v2/statements/{id}` header.
  - Every other success returns 200.

- [ ] **Step 1: Write the failing tests** (in `web/agent.rs` tests, using the router helpers of H1's `v2.rs` tests)

```rust
#[tokio::test] async fn statements_require_authentication()
// password auth, no credentials → 401 envelope
#[tokio::test] async fn submit_poll_fetch_cancel_over_http()
// POST /api/v2/statements {"sql":"SELECT 1"} → 201 + Location; GET ?wait_ms=2000 → finished; GET /results → rows [[1]]
#[tokio::test] async fn unknown_column_envelope_has_location_candidates_hint()
// failed status body: error.code == "column_not_found", error.location == {"line":1,"column":8}, error.candidates[0] == "l_quantity"
#[tokio::test] async fn explain_error_is_http_400_with_envelope()
#[tokio::test] async fn wait_above_ceiling_is_invalid_argument()
#[tokio::test] async fn discovery_routes_under_auth_none_use_claimed_user()
#[tokio::test] async fn worker_role_has_no_agent_routes()
#[tokio::test] async fn statement_shows_in_queries_with_source_api()
// GET /api/v2/queries/{id} → source == "api", user == submitter
```

- [ ] **Step 2:** Run `cargo test -p arneb-server web::agent`. Expected: FAIL.
- [ ] **Step 3: Implement** the code.
- [ ] **Step 4:** Run the full Tier 1 set. Expected: all green.
  - `cargo test --workspace`
  - `cargo fmt -- --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] **Step 5: Commit** `feat(web): /api/v2 agent query routes`

### Task 8: Tier 2 scenario script, OpenSpec change, evidence

**Files:**
- Create: `scripts/agent-api-smoke.sh`
  - It takes `ARNEB_URL` (the Web port) and `ARNEB_TOKEN`, or `ARNEB_USER` and `ARNEB_PASSWORD`.
  - It uses only `curl` and `jq`.
  - It exits non-zero on the first failed check and prints `PASS <step>` / `FAIL <step>` lines. A later CI kind job and H3 will reuse it.
- Create: `openspec/changes/agent-query-api/` (proposal, design, specs, tasks), following the structure of `openspec/changes/headless-api-auth/`. The design records the Decisions above.

**Checks the script runs (acceptance steps 1–4 of the spec):**
1. Catalog discovery. `GET /catalogs` contains the TPC-H catalog. Listing the `tpch` tables contains `lineitem`. Describing `lineitem` shows `l_quantity`.
2. Sample. Sampling `nation` with `rows=5` returns 5 rows.
3. Explain. Explaining TPC-H Q6 returns a non-empty `plan` and lists the `lineitem` scan.
4. Submit Q6.
   - It reaches `finished`.
   - Its single revenue value equals the value psql returns for Q6 over pgwire (the script runs both).
5. Truncation. `SELECT * FROM lineitem` with `max_rows=100` gives:
   - `truncated == true`
   - `total_rows` equal to `count(*)`
   - exactly 100 rows across all pages
6. Bad column. `SELECT l_quantiy FROM lineitem` fails with `column_not_found`, a location, and `candidates[0] == "l_quantity"`.
7. Fix from the hint. The script resubmits with the first candidate substituted, and the statement finishes.
8. Long statement. It submits a long query (a 3-way self-join of `lineitem` at SF1), polls it until it is `running`, cancels it, and checks the state is `cancelled` and `/api/v2/queries/{id}` shows it cancelled.
9. Ownership. A second user polls the first user's statement and gets 404 `not_found`.

- [ ] **Step 1:** Write the script and the OpenSpec change.
- [ ] **Step 2: Tier 2.**
  1. Bring up the local distributed stack, coordinator plus 2 workers, with password auth, `[tls]` and the hive TPC-H SF1 catalog (`docker compose up -d`, `docker compose run --rm tpch-seed`).
  2. Mint a token with the H1 API.
  3. Run the script.

  Expected: 9 lines of `PASS`. Save the commands and raw output verbatim into the task report.
- [ ] **Step 3:** Run `./scripts/target-housekeeping.sh` and report the verdict.
- [ ] **Step 4: Commit** `test(agent): Tier 2 smoke script and OpenSpec change for agent-query-api`
