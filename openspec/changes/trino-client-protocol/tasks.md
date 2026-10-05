## 1. Engine plumbing

- [x] 1.1 Share `execute_query` crate-internally (`pub(crate)`) so the Trino front end reuses the pgwire pipeline
- [x] 1.2 `CatalogManager::with_session_defaults` (shared catalog map) + `qualify_table_reference`; unit test that a view resolves against its own defaults, shares late registrations, and the root never rewrites references
- [x] 1.3 Planner records `qualify_table_reference(name)` in `TableScan`; unit test (root plan unqualified, session-view plan qualified)
- [x] 1.4 Planner: ORDER BY on a column not in the SELECT list sorts beneath the projection; unit test (plain column, alias, unknown column still errors)

## 2. Trino protocol module (`crates/protocol/src/trino/`)

- [x] 2.1 `types.rs`: Arrow → Trino type mapping, `type` strings, `typeSignature`, JDBC type codes, per-cell JSON encoding; unit tests for scalars, decimals, dates, timestamps, NaN/Infinity, varbinary
- [x] 2.2 `session.rs`: `X-Trino-*` / `X-Presto-*` header parsing (user, source, client info, catalog, schema, session, prepared statements), URL encode/decode; unit tests
- [x] 2.3 `error.rs`: `ArnebError` → Trino `QueryError` (code/name/type, failureInfo); unit test
- [x] 2.4 `statements.rs`: recognizer for SHOW/DESCRIBE/USE/SET SESSION/RESET SESSION/PREPARE/EXECUTE/EXECUTE IMMEDIATE/DEALLOCATE/transaction no-ops, `?` binding, LIKE matching; unit tests
- [x] 2.5 `metadata.rs`: SHOW helpers + virtual `information_schema`, `system.jdbc`, `system.metadata` tables built into a per-query memory catalog; unit tests
- [x] 2.6 `engine.rs`: statement dispatch (control statements, metadata path, session-view path, root path) with response-header side effects
- [x] 2.7 `mod.rs`: axum router, `POST /v1/statement`, `GET`/`DELETE` nextUri (queued + executing), `/v1/query[/{id}]`, `/v1/info[/state]`; query map, long-poll, bounded paging, idempotent re-fetch, 410 for stale tokens, reaper (abandon + retention), QueryTracker integration, panic capture

## 3. Server wiring

- [x] 3.1 `[trino] enabled/port` config section with `ARNEB_TRINO_ENABLED` / `ARNEB_TRINO_PORT`; config unit test
- [x] 3.2 CLI `--trino-port` / `--no-trino`; effective values logged on `arneb::config`
- [x] 3.3 Start the listener for coordinator/standalone only, sharing catalogs, connectors, memory pool, distributed executor and query tracker; bind failure non-fatal

## 4. Tests

- [x] 4.1 Integration tests (`crates/protocol/tests/trino.rs`, ephemeral ports, reqwest): info endpoints, type encoding, multi-page results, idempotent re-fetch + 410, error responses, empty statement 400, session catalog/schema, SHOW family, information_schema via engine, system.jdbc driver queries, USE/SET/RESET SESSION headers, X-Presto headers, PREPARE/EXECUTE/EXECUTE IMMEDIATE/DEALLOCATE, DELETE cancel of a running query (execution future dropped, tracker CANCELLED), abandoned-query cancel, DELETE between pages
- [x] 4.2 Manual smoke test with trino-python-client 0.340.0 (DB-API + SQLAlchemy inspector) against a standalone server

## 5. Docs

- [x] 5.1 `docs/guide/trino-clients.md` (+ sidebar), configuration guide ports/knobs
- [x] 5.2 README feature + quick start mention, CLAUDE.md ports and crate layout
