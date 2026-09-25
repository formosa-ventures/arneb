## Context

Trino's client protocol is a stateless HTTP/JSON protocol. A client `POST`s SQL to `/v1/statement` and receives a `QueryResults` document; it then repeatedly `GET`s the `nextUri` in each document, accumulating `columns` and `data`, until a document has no `nextUri`. `DELETE` on the `nextUri` cancels. The whole client session (user, catalog, schema, session properties, prepared statements) travels in `X-Trino-*` request headers, and the server asks the client to change it with `X-Trino-Set-*` / `X-Trino-Clear-*` / `X-Trino-Added-Prepare` response headers. Row values use Trino's JSON encodings, and each column carries a `type` string (used by the Python client) and a `typeSignature` (used by JDBC).

Arneb already has a complete engine entry point in `crates/protocol/src/handler.rs::execute_query` (parse → plan → optimize → register data sources → resolve scalar subqueries → distributed or local execution → collected batches) used by both pgwire query paths, and an axum Web UI in `crates/server`.

## Goals / Non-Goals

**Goals:**

- Unmodified Trino clients (CLI, JDBC, python client + SQLAlchemy, Metabase/Superset drivers, dbt-trino) can connect, run queries, page results, cancel, and browse metadata.
- One engine path for both protocols — no duplicated planning/execution logic.
- Session catalog/schema semantics that also hold under distributed execution.
- Bounded memory for abandoned queries; bounded page sizes.

**Non-Goals:**

- Authentication, TLS, or authorization (the listener trusts `X-Trino-User`).
- Streaming the first page while the query is still running (the engine collects results; paging happens afterwards, as with pgwire).
- Interpreting Trino session properties.
- `DESCRIBE INPUT/OUTPUT`, transactions, spooled/segment protocol, `X-Trino-Client-Capabilities` negotiation.

## Decisions

### D1: Module in the `protocol` crate, axum server

**Choice**: `crates/protocol/src/trino/` (`mod.rs` server + query state, `engine.rs` statement execution, `statements.rs` control-statement recognizer, `metadata.rs` catalog browsing, `types.rs` type mapping/encoding, `session.rs` headers, `error.rs` error mapping), exposed as `arneb_protocol::{TrinoServer, TrinoConfig}`.

**Rationale**: `execute_query` is crate-private to `protocol`; putting the HTTP front end next to it keeps it private (`pub(crate)`) and makes the two protocols siblings. axum is already the workspace HTTP framework.

**Alternative**: a new crate or the `server` crate. Rejected — would force `execute_query` and its helpers public, and the server crate is a binary.

### D2: Query lifecycle — spawn, long-poll, page from collected batches

**Choice**: `POST` registers a `Query` in a `Mutex<HashMap<id, Arc<Query>>>`, spawns the statement on a tokio task (panics caught and reported as `GENERIC_INTERNAL_ERROR`), and immediately returns state `QUEUED` with `nextUri` token 1. `GET` on token *n* waits up to `max_wait` (1 s) on a `watch` channel while the query runs and then returns `RUNNING` with the same token; once finished it encodes the next page (≈1 MiB estimated from Arrow buffer sizes, ≤10 000 rows) and advances the token. The last served document is cached so a retried request for the same token is idempotent; older tokens get `410 Gone`. When the last page is handed out the batches are dropped.

A background reaper cancels queries whose client has not polled for `client_timeout` (5 min, `ABANDONED_QUERY`) and removes finished queries `retention` (2 min) after the last request. `DELETE` aborts the task (dropping the execution future) and removes the query.

**Rationale**: matches Trino's observable behavior and client retry semantics while reusing the collected-batch engine API. Unguessable `slug` path segments prevent other clients from reading a query by id.

### D3: Session catalog/schema via catalog-manager views + qualified scans

**Choice**: `CatalogManager` stores its catalog map behind an `Arc`, and `with_session_defaults(catalog, schema)` returns a view sharing all registrations but with different defaults. On a view, the planner records fully qualified `TableReference`s in `TableScan` (`qualify_table_reference`). The root manager is untouched, so pgwire and existing plans are byte-identical.

**Rationale**: `register_data_sources` and workers resolve connectors from `table.catalog` falling back to the *server* default; a qualified reference makes the session's intent explicit across the coordinator/worker boundary. Catalog statistics are keyed by the same qualified reference, so cost-based decisions are unaffected.

### D4: Control statements recognized before the SQL parser

**Choice**: a small tokenizer recognizes `SHOW …`, `DESCRIBE`, `USE`, `SET/RESET SESSION`, `PREPARE`, `EXECUTE [IMMEDIATE] … USING`, `DEALLOCATE PREPARE`, and transaction/`SET TIME ZONE|ROLE|PATH` no-ops. Everything else goes to the engine. Prepared statements live on the client (Trino semantics): `PREPARE` returns `X-Trino-Added-Prepare`, `EXECUTE` looks the text up in `X-Trino-Prepared-Statement` and binds `?` placeholders (outside literals/identifiers/comments) textually with the `USING` expressions.

**Rationale**: these statements are protocol/session concerns, not engine features, and `sqlparser`'s generic dialect does not model Trino's forms. Mirrors how pgwire intercepts SET/SHOW in `metadata.rs`.

### D5: Metadata tables are materialized, then queried by the engine

**Choice**: when a statement references `information_schema`, `system.jdbc` or `system.metadata`, build a per-query `CatalogManager` + `ConnectorRegistry` backed by the memory connector, containing the Trino-shaped tables (`schemata`, `tables`, `columns`, `views`; JDBC `catalogs`, `schemas`, `tables`, `columns`, `table_types`, `types`; `table_comments`) for only the catalogs the SQL mentions (all catalogs for `system.*`), loading table schemas only if a `columns` table is referenced. The client SQL then runs over it with `execute_query` (no distributed executor).

**Rationale**: BI tools send arbitrary filters, projections, functions and ordering over these tables; running them through the real engine is both cheaper to build and more correct than pattern-matching queries (the pgwire approach).

### D6: Types from the produced Arrow data

**Choice**: column types are derived from the Arrow schema of the produced batches when present (falling back to the planned `DataType` for empty results) and mapped to Trino types (`decimal(p,s)`, `timestamp(p)` from the time unit, `varchar` with the unbounded length argument, nested `array/map/row`). Values are encoded per Arrow array type.

**Rationale**: the encoded data must agree with the declared type; the physical Arrow type is the source of truth (e.g. `Decimal64` storage, dictionary arrays).

### D7: Listener configuration and failure mode

**Choice**: `[trino] enabled = true, port = 8080`, `ARNEB_TRINO_ENABLED` / `ARNEB_TRINO_PORT`, `--trino-port` / `--no-trino`; effective values logged on target `arneb::config`. Coordinator and standalone only. A bind failure is logged at error level and the server keeps serving pgwire.

**Rationale**: 8080 is what every Trino client defaults to. The repository's docker-compose publishes a real Trino on 8080 for benchmarking, so a port clash must not take down the pgwire server.

### D8: Planner — ORDER BY below the projection

**Choice**: if an ORDER BY key cannot be resolved against the SELECT output and the plan is a `Projection`, resolve all keys against the projection's input (keys naming an output column use that column's projection expression) and place the `Sort` beneath the projection. Otherwise the original error is returned.

**Rationale**: SQLAlchemy's Trino dialect lists columns with `… ORDER BY ordinal_position` without selecting it; this is standard SQL. Only queries that previously failed planning take the new path.

## Risks / Trade-offs

- **No auth over HTTP** → documented; bind to a trusted interface or front with an authenticating TLS proxy.
- **Whole result held in memory until paged out** → same as pgwire today; batches are released as soon as the final page is served and abandoned queries are canceled.
- **Duplicate Web UI entries on a coordinator** (SQL from the Trino listener + plan from the coordinator) → acceptable; documented.
- **Metadata snapshot cost** on huge Hive metastores when `columns` is queried → documented; limited to referenced catalogs.
