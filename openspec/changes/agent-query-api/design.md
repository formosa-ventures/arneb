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

**D4. Retention.** A statement keeps at most `max_rows` rows after it finishes, and its total row count; the rest are dropped at completion. The kept rows are copied out of the operator's output buffers, so a finished statement holds memory for at most `max_rows` rows. `max_rows` does not bound execution: the query reads and materializes its whole result on the coordinator before truncation, and that final collect is not reserved against the memory pool (the distributed executor returns the whole result as one `Vec`). Operators stay bounded by the existing query memory limit, as on the Trino path. Agents should add `LIMIT` or use `sample` to explore; an execution-side row limit is deferred.

**D5. Inline first page.** `submit` and `status` accept `wait_ms`, default 0, capped at `max_wait_ms`. A statement that finishes inside the wait carries its first page inline.

**D6. Sample is a short statement.** It submits `SELECT * FROM "c"."s"."t" LIMIT n` with `wait_ms = max_wait_ms`. `rows` defaults to 10 and is capped at 100.

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
| `max_wait_ms` | `ARNEB_AGENT_MAX_WAIT_MS` | 10000 |
| `idle_timeout_secs` | `ARNEB_AGENT_IDLE_TIMEOUT_SECS` | 600 |

Two caps are constants: 32 statements per user and 1024 in total. At a cap the least-recently-accessed terminal statement is evicted; when all are queued or running, the submit is `overloaded`. A request value above its ceiling is refused with `invalid_argument`, not clamped.

**D10. Idle timeout.** After `idle_timeout_secs` with no request for a statement, the reaper drops a terminal statement, or cancels a running one with the reason `abandoned`, which frees its admission slot.

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

- [A `SELECT *` over a large table still runs to completion to count `total_rows`, and its full result is materialized on the coordinator before truncation] -> `max_rows` bounds only what is retained afterwards; operators stay under the query memory limit, but the final collect is not reserved against the pool. Docs steer agents to `LIMIT` or `sample`; cancel or the idle reaper stops a runaway statement. An execution-side row limit is deferred.
- [Explain duplicates the run path's catalog-scoping logic] -> kept in step by tests; a shared helper is a follow-up.
- [Name matching for describe and sample is exact-case, like `SHOW COLUMNS`] -> documented; listings' `prefix` is case-insensitive.
- [Statements live in coordinator memory] -> lost on restart; the agent resubmits.

## Migration Plan

Additive: no config is required, and no existing route, protocol or JSON field changes except that Trino errors gain `errorLocation`. Roll back by deploying the previous release.

## Open Questions

None for H2. Writes and cross-user cancel are H4; the MCP tool surface is H3.
