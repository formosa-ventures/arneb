## Why

Arneb positions itself as a Trino alternative, but it only speaks the PostgreSQL wire protocol. Teams replacing Trino have their tooling wired to Trino's client REST protocol: the `trino` CLI, the Trino JDBC driver (DBeaver, DataGrip, Metabase), `trino-python-client` and its SQLAlchemy dialect (Superset), and dbt-trino. Asking them to switch every tool to a PostgreSQL driver — with different type names, metadata tables and session semantics — is the largest adoption barrier left before go-to-market. Speaking Trino's protocol lets those tools point at an Arneb coordinator unchanged.

## What Changes

- Add a Trino client REST protocol (v1) HTTP server to the `protocol` crate: `POST /v1/statement`, `GET`/`DELETE` on `nextUri`, `GET /v1/query/{id}`, `GET /v1/info`, `GET /v1/info/state`.
- Execute statements through the same parse → plan → optimize → execute pipeline pgwire uses (including distributed execution), with per-query state held in a map with abandonment/retention expiry, bounded result pages and cancellation.
- Encode results with Trino's JSON conventions and type signatures (bigint/integer numbers, decimal strings, `YYYY-MM-DD` dates, `YYYY-MM-DD HH:MM:SS.fff` timestamps, base64 varbinary, …) and failures as Trino `QueryError`s.
- Honor `X-Trino-*` (and legacy `X-Presto-*`) session headers: user, source, client info, catalog/schema, session properties, prepared statements.
- Answer Trino-only statements in the protocol layer: `SHOW CATALOGS/SCHEMAS/TABLES/COLUMNS/SESSION`, `DESCRIBE`, `USE`, `SET/RESET SESSION`, `PREPARE`/`EXECUTE`/`EXECUTE IMMEDIATE`/`DEALLOCATE PREPARE`, transaction no-ops — with Trino's `X-Trino-Set-*` response headers.
- Serve virtual `information_schema`, `system.jdbc` and `system.metadata` tables (Trino column layouts) that run through the regular engine.
- Add `CatalogManager::with_session_defaults` so a session's catalog/schema resolve unqualified tables without touching server defaults.
- Planner: allow `ORDER BY` on a column that is not in the SELECT list by sorting beneath the final projection (needed by the SQLAlchemy dialect's column query, and useful generally).
- Server: `[trino] enabled/port` config (default on, port 8080), `ARNEB_TRINO_ENABLED`/`ARNEB_TRINO_PORT`, `--trino-port`/`--no-trino`; coordinator and standalone only; queries registered with the `QueryTracker` so they appear in the Web UI.

## Capabilities

### New Capabilities

- `trino-client-protocol`: Trino client REST protocol v1 server — statement submission, paging, cancellation, session headers, control statements, metadata tables, type encoding and error mapping.

### Modified Capabilities

- `server-config`: new `[trino]` section with env/CLI overrides.
- `catalog-manager`: session views with their own default catalog/schema.
- `query-planner`: `ORDER BY` columns not present in the SELECT list.

## Impact

- **Crates**: `protocol` (new `trino` module, `execute_query` shared crate-internally), `catalog` (session views), `planner` (ORDER BY below projection, TableScan references qualified on session views), `server` (config, CLI, startup wiring).
- **Dependencies**: `protocol` gains `axum`, `serde`, `serde_json`, `uuid` (already workspace dependencies), `base64` and `percent-encoding` (already in the lockfile), and dev-dependency `reqwest` (already in the lockfile via `object_store`).
- **Ports**: a new HTTP listener on 8080 by default for coordinator/standalone; a bind failure is logged and non-fatal.
- **Unlocks**: trino CLI, Trino JDBC, trino-python-client/SQLAlchemy (Superset), Metabase's Trino driver and dbt-trino without client changes.
