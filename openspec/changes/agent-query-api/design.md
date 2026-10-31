## Context

Stage H2 of the headless agent interface, built on H1 (`headless-api-auth`): an authenticated Web port with an identity per request, API tokens, and the `/api/v2` error envelope. Before H2, SQL ran only over pgwire and the Trino client protocol. The Trino module already owns an `Engine` that applies access control, `information_schema`, `system.runtime.queries`, admission, the statement timeout and query-tracker registration.

## Goals / Non-Goals

**Goals**
- An agent can discover, sample, explain and run SQL over HTTP with no driver.
- No response returns data unbounded in rows or bytes; truncation is reported with the full count when known.
- No request blocks longer than `max_wait_ms`; long statements are submitted, polled, fetched and cancelled.
- Errors say where in the SQL they are and what the nearest valid name is.
- An API statement has exactly the visibility and privileges the user has over Trino or pgwire.

**Non-Goals**
- DDL and DML over the API, and write privileges (H4).
- Cancelling another user's query (H4).
- Session state across statements (`USE`, `SET SESSION`, `PREPARE`).
- The MCP endpoint (H3).

## Decisions

**D1. One service in the protocol crate, over the Trino engine.** `arneb_protocol::agent::AgentQueryService` runs SQL through the Trino module's `Engine`, so semantics, access rules, admission and tracking are those of a Trino client. The statement registry and reaper copy the Trino `Query`/`Inner`/`Phase` design rather than share it, so the Trino paging protocol is untouched. The server crate only translates HTTP (`crates/server/src/web/agent.rs`). Alternative: a second engine path; rejected as duplicated access logic.

**D2. Routes.** All under `/api/v2`, all authenticated by the H1 middleware:

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

**D3. Statement ids are query-tracker ids.** The `QueryId` string is the statement id, so `/api/v2/queries/{id}` shows the same query.

**D4. Retention and the execution row cap.** A query submitted through the API is planned with `LIMIT max_rows + 1` at its root (merged with a root `LIMIT` by taking the smaller, and placed under root projections), so the physical planner turns it into a Top-K over a sort or a limit hint over a bare scan, and `LimitExec` stops pulling its input after the cap. Only `SELECT` statements are capped; other statements run as before. The one extra row tells whether the result was truncated. The cost is the exact count: `total_rows` is reported only when the result was not truncated and is `null` otherwise (agents run `count(*)` when they need it). A statement keeps at most `max_rows` rows, copied out of the operator's output buffers, so a finished statement holds memory for at most `max_rows` rows. Retention is also bounded by bytes: each kept batch is measured with `get_array_memory_size` and charged against a per-statement cap (`max_retained_bytes`, 64 MiB) and a process-wide budget (`max_total_retained_bytes`, 1 GiB), held by an RAII reservation that is given back when the statement is evicted or reaped. Past either, the remaining rows are dropped and the statement still finishes, reporting `truncated_by` (`max_rows`, `max_retained_bytes` or `server_budget`) so an agent can narrow the query or retry. Without this a wide result (say 100 KB rows x 10,000) would pin about 1 GB per statement outside the memory pool for the idle timeout. Aggregations, joins and sorts still read their whole input, under the existing query memory limit as on the Trino path. The Trino and pgwire paths are unchanged.

**D5. Inline first page.** `submit` and `status` accept `wait_ms`, default 0, capped at `max_wait_ms`. A statement that finishes inside the wait carries its first page inline.

**D6. Sample is a short statement.** It submits `SELECT * FROM "c"."s"."t" LIMIT n` with `wait_ms = max_wait_ms`. `rows` defaults to 10 and is capped at 100 (refused above), then lowered to the server's `max_rows` when that is smaller, so a small configured ceiling shortens the sample rather than failing it with an error about `max_rows`, which the caller never sent.

**D7. Explain is synchronous planning only.** It is not admitted, not tracked, and does not support `ANALYZE`; an agent submits `EXPLAIN ANALYZE` as a statement.

**D8. Session statements are refused.** The API is stateless per statement. Statements that `trino::statements::parse_control` recognizes (`USE`, `SET SESSION`, `PREPARE`, ...) are refused with `invalid_argument`, hint `pass "catalog" and "schema" in the request body`. `SHOW` is not refused.

**D9. Limits.** `[agent_api]` field, then env var, then default; the effective values are logged once on `arneb::config`:

| `[agent_api]` field | Env var | Default |
|---|---|---|
| `default_max_rows` | `ARNEB_AGENT_DEFAULT_MAX_ROWS` | 1000 |
| `max_rows` | `ARNEB_AGENT_MAX_ROWS` | 10000 |
| `default_page_bytes` | `ARNEB_AGENT_DEFAULT_PAGE_BYTES` | 262144 |
| `max_page_bytes` | `ARNEB_AGENT_MAX_PAGE_BYTES` | 4194304 |
| `max_cell_bytes` | `ARNEB_AGENT_MAX_CELL_BYTES` | 8192 |
| `max_retained_bytes` | `ARNEB_AGENT_MAX_RETAINED_BYTES` | 67108864 |
| `max_total_retained_bytes` | `ARNEB_AGENT_MAX_TOTAL_RETAINED_BYTES` | 1073741824 |
| `max_wait_ms` | `ARNEB_AGENT_MAX_WAIT_MS` | 10000 |
| `idle_timeout_secs` | `ARNEB_AGENT_IDLE_TIMEOUT_SECS` | 600 |

Two caps are constants: 32 statements per user and 1024 in total. At a cap the least-recently-accessed terminal statement is evicted; when all are queued or running, the submit is `overloaded`. A request value above its ceiling is refused with `invalid_argument`, not clamped.

**D10. Idle timeout.** After `idle_timeout_secs` with no request for a statement, the reaper drops a terminal statement, or cancels a running one with the reason `abandoned`, which frees its admission slot. A statement with a request long-polling it is never idle, and `idle_timeout_secs` must be at least `2 × max_wait_ms` (validated at startup), which leaves a full `max_wait` of slack between a long-poll ending and the reaper deciding.

**D11. Error codes.** Added to the H1 `ErrorCode`, all snake_case:

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

The existing `forbidden`, `overloaded`, `invalid_argument`, `not_found` and `internal` are reused. A statement that fails after submission is a `200` status body whose `state` is `failed` and which carries an `error`; HTTP error statuses are only for the request itself. The envelope gains optional `location` and `candidates`; Trino error JSON gains `errorLocation`.

**D12. Per-user statements.** Only the submitter can poll, fetch or cancel; anyone else gets the same `not_found` as for a random id. An object the user may not see answers exactly like one that does not exist, and name candidates come only from tables the user can see.

## Risks / Trade-offs

- [A truncated result has no exact `total_rows`] -> reported as `null`, documented, and agents run `count(*)` when they need the count. In exchange a truncated `SELECT *` reads only `max_rows + 1` rows.
- [Explain and run could disagree on catalog scoping] -> both call one `Engine::statement_scope` helper.
- [Name matching for describe and sample is exact-case, like `SHOW COLUMNS`] -> documented; listings' `prefix` is case-insensitive.
- [Statements live in coordinator memory] -> lost on restart; the agent resubmits.

## Migration Plan

Additive: no config is required, and no existing route, protocol or JSON field changes except that Trino errors gain `errorLocation`. Roll back by deploying the previous release.

## Open Questions

None for H2. Writes and cross-user cancel are H4; the MCP tool surface is H3.
