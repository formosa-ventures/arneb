## Why

Stage H2 of the headless agent interface (`docs/superpowers/specs/2026-10-30-headless-agent-interface-design.md`, Goals 3 and 4). H1 (`headless-api-auth`) gave a remote agent a revocable credential and an authenticated `/api/v2`, but the only way to run SQL was a driver (pgwire or Trino). An agent needs to explore the catalog and run SQL over plain HTTP without ever receiving an unbounded result, blocking past a deadline, or being left to guess what a failing name should have been. H2 is the query layer the MCP server (H3) exposes as tools.

## What Changes

- **Discovery.** `GET /api/v2/catalogs`, `/catalogs/{c}/schemas`, `/catalogs/{c}/schemas/{s}/tables` (each with `?prefix=&limit=` and a `total`/`truncated` listing), and `/catalogs/{c}/schemas/{s}/tables/{t}` (columns with Trino types plus row-count and size statistics).
- **Sample.** `GET .../tables/{t}/sample?rows=` (default 10, at most 100), returned as a statement status.
- **Explain.** `POST /api/v2/explain` plans a statement and returns the plan text, each scan's row and byte estimates, and their total. Not admitted, not tracked, no `ANALYZE`.
- **Async statements.** `POST /api/v2/statements` (with `catalog`, `schema`, `max_rows`, `?wait_ms=`), `GET /statements/{id}?wait_ms=`, `GET /statements/{id}/results?cursor=&max_bytes=`, `DELETE /statements/{id}`. Statement ids are query-tracker ids, so `/api/v2/queries/{id}` shows them with `source: "api"`. Only the submitter can see a statement.
- **Bounded results.** Row (`max_rows`), page byte and cell byte limits, with `truncated`, `total_rows` and `truncated_cells` reported. Limits are `[agent_api]` fields with `ARNEB_AGENT_*` overrides; values above a ceiling are refused, never clamped.
- **Structured errors.** The H1 envelope gains `location` (`{line, column}`) and `candidates` (nearest names), and twelve new codes (`syntax_error`, `column_not_found`, `table_not_found`, ...). Trino error JSON gains `errorLocation`.
- **Idle reaper.** A statement nobody asks about for `idle_timeout_secs` is dropped (terminal) or cancelled as `abandoned` (running), freeing its admission slot.

Nothing else changes: `/api/v1`, the H1 `/api/v2` routes, pgwire, and the existing Trino JSON fields are as before. DDL and DML are not supported over the API in this release.

## Capabilities

### New Capabilities

- `agent-discovery`: catalog, schema and table listings, table description, sample and explain.
- `agent-statements`: the asynchronous statement lifecycle, bounded results, limits and the idle reaper.
- `agent-errors`: the structured error codes, SQL location and name candidates.

### Modified Capabilities

## Impact

- New module `arneb_protocol::agent` (`service`, `statement`, `results`, `discovery`, `explain`, `limits`, `error`). It runs SQL through the Trino module's `Engine`, so access control, admission, timeouts and tracking match the Trino path.
- `crates/planner` and `crates/sql-parser`: SQL positions and nearest-column candidates on plan and parse errors.
- `crates/server`: `[agent_api]` config, `web/agent.rs` routes, envelope fields. Coordinator and standalone only; workers do not build the service.
- Docs: "Querying" section of `docs/guide/http-api.md`; `[agent_api]` in the configuration guide.
- `scripts/agent-api-smoke.sh`: the acceptance scenario over curl and jq, reused by CI and H3.
- Tier 1 (unit and integration tests) covers the logic; Tier 2 (coordinator plus two workers, password auth, TLS, hive TPC-H) runs the smoke script.
