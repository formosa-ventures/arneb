## Context

arneb has a complete single-node query pipeline: SQL parsing → logical planning → physical execution, with connectors for in-memory tables and CSV/Parquet files. All execution is synchronous — `ExecutionPlan::execute()` returns `Result<Vec<RecordBatch>>`. The catalog system (`CatalogManager`) resolves table references, and the connector system (`ConnectorRegistry`) maps catalogs to data source factories.

There is currently no way for external clients to connect and query the engine. The protocol crate introduces a TCP server that speaks the PostgreSQL wire protocol (v3), enabling any PostgreSQL-compatible client to issue SQL queries against arneb.

Project conventions: `Arc<dyn Trait>` for polymorphism, `thiserror` for errors, `tracing` for instrumentation, Arrow columnar format for all intermediate data.

## Goals / Non-Goals

**Goals:**

- Accept TCP connections from PostgreSQL clients (psql, JDBC, psycopg2, DBeaver, etc.)
- Implement the Simple Query flow: receive SQL text → execute full pipeline → return results
- Map Arrow `DataType` to PostgreSQL type OIDs for RowDescription messages
- Encode Arrow `RecordBatch` column values as text-format DataRow messages
- Map `ArnebError` variants to PostgreSQL `ErrorResponse` with SQLSTATE codes
- Handle connection lifecycle: startup handshake, authentication (trust mode), query loop, termination
- Provide a `ProtocolServer` struct that the server crate (Change 8) can instantiate with configured state

**Non-Goals:**

- No Extended Query protocol (prepared statements, parameter binding) — Simple Query is sufficient for MVP
- No SSL/TLS support — plaintext connections only for MVP
- No password authentication — trust mode only (accept all connections)
- No COPY protocol or replication protocol
- No binary encoding format — text format only for all values
- No connection pooling or connection limits
- No cancellation of in-flight queries
- No transaction support (each query is auto-committed)

## Decisions

### D1: Use the `pgwire` crate instead of hand-rolling the protocol

**Choice**: Depend on the `pgwire` crate for protocol message handling, framing, and the server-side API.

**Rationale**: `pgwire` provides a well-tested implementation of PostgreSQL wire protocol v3 with a trait-based API (`SimpleQueryHandler`, `StartupHandler`). It handles message framing, type OID definitions, DataRow encoding, and the startup handshake. Hand-rolling the binary protocol is error-prone and adds significant implementation scope for no differentiated value. The crate is actively maintained and used by several Rust database projects.

**Alternative**: Implement protocol from scratch using raw TCP + `bytes`. Rejected — significant effort for message parsing, encoding edge cases, and client compatibility testing. The protocol crate's value is in query pipeline integration, not wire format implementation.

### D2: Simple Query protocol only for MVP

**Choice**: Implement only `SimpleQueryHandler`. No `ExtendedQueryHandler`.

**Rationale**: Simple Query handles text-based SQL strings and returns results — this covers psql, most JDBC drivers in simple mode, and basic tool connectivity. Extended Query (Parse/Bind/Describe/Execute) adds prepared statement caching, parameter binding, and binary encoding — complexity that is not needed for MVP. Simple Query is sufficient to demonstrate the full pipeline.

**Alternative**: Implement Extended Query from the start. Deferred — it can be added later by implementing `ExtendedQueryHandler` without changing the Simple Query path.

### D3: Bridge synchronous execution with async protocol via `spawn_blocking`

**Choice**: The `pgwire` handler is async (tokio). The arneb execution engine is synchronous (`execute() → Result<Vec<RecordBatch>>`). Bridge the gap using `tokio::task::spawn_blocking` to run the synchronous query pipeline off the async runtime's thread pool.

**Rationale**: The execution engine reads files and performs CPU-intensive operations (sorting, aggregation, joins). Running these synchronously on an async task would block the tokio runtime. `spawn_blocking` moves the work to a dedicated thread pool, keeping the async runtime responsive for other connections.

**Alternative**: Make the execution engine async. Rejected — this is a massive cross-cutting change (every operator would need to be async). The MVP execution model is synchronous by design (Change 5, Decision D1). `spawn_blocking` is the standard bridge pattern.

### D4: Shared state via `Arc` — CatalogManager + ConnectorRegistry

**Choice**: `ProtocolServer` holds `Arc<CatalogManager>` and `Arc<ConnectorRegistry>`. Each connection handler receives clones of these Arcs. Per-connection state (current database, schema) lives in the handler instance.

**Rationale**: Catalogs and connectors are read-heavy, shared across all connections. `Arc` provides zero-cost sharing. Per-connection mutation (session variables) stays local to each handler. This matches the pattern used throughout the codebase.

### D5: Text-format encoding for all types

**Choice**: Encode all values in PostgreSQL text format (format code 0). No binary encoding.

**Rationale**: Text format is simpler to implement (just string conversion), universally supported by all clients, and sufficient for MVP correctness. Binary encoding is an optimization for driver performance — it can be added in the Extended Query path later.

**Type mapping**:
| Arrow/trino DataType | PostgreSQL Type | OID | Text encoding |
|---|---|---|---|
| Boolean | BOOL | 16 | `t` / `f` |
| Int8, Int16 | INT2 | 21 | decimal string |
| Int32 | INT4 | 23 | decimal string |
| Int64 | INT8 | 20 | decimal string |
| Float32 | FLOAT4 | 700 | decimal string |
| Float64 | FLOAT8 | 701 | decimal string |
| Decimal128 | NUMERIC | 1700 | decimal string |
| Utf8, LargeUtf8 | VARCHAR | 1043 | raw string |
| Binary | BYTEA | 17 | hex-encoded |
| Date32 | DATE | 1082 | `YYYY-MM-DD` |
| Timestamp | TIMESTAMP | 1114 | `YYYY-MM-DD HH:MM:SS` |
| Null | TEXT | 25 | NULL (no value) |

### D6: Error mapping — ArnebError to PostgreSQL ErrorResponse

**Choice**: Map each `ArnebError` variant to a PostgreSQL SQLSTATE code and severity level.

**Mapping**:
| ArnebError variant | SQLSTATE | Severity | Category |
|---|---|---|---|
| `Parse(ParseError)` | `42601` (syntax_error) | ERROR | SQL syntax issues |
| `Plan(PlanError)` | `42P01` (undefined_table) or `42703` (undefined_column) | ERROR | Planning failures |
| `Execution(ExecutionError)` | `XX000` (internal_error) | ERROR | Runtime failures |
| `Connector(ConnectorError)` | `58030` (io_error) | ERROR | Data access failures |
| `Catalog(CatalogError)` | `3D000` (invalid_catalog_name) | ERROR | Catalog resolution |
| `Config(ConfigError)` | `F0000` (config_file_error) | ERROR | Configuration issues |

### D7: Module structure within the protocol crate

**Choice**: Organize by protocol concern:
```
crates/protocol/src/
├── lib.rs          — public exports (ProtocolServer, ProtocolConfig)
├── server.rs       — TCP listener, ProtocolServer struct, connection spawning
├── handler.rs      — SimpleQueryHandler + StartupHandler implementations
├── encoding.rs     — Arrow DataType → PG Type OID mapping, RecordBatch → DataRow encoding
├── error.rs        — ProtocolError type, ArnebError → ErrorResponse mapping
└── session.rs      — Per-connection session state (database, schema)
```

**Rationale**: Each module has a single responsibility. The handler coordinates the query pipeline; encoding handles type conversion; error handles mapping; session holds connection state.

## Risks / Trade-offs

**[Synchronous execution blocks a thread per query]** → `spawn_blocking` uses tokio's blocking thread pool (default 512 threads). Each active query occupies one thread for its duration. **Mitigation**: Acceptable for MVP concurrency levels. Phase 2 async execution will remove this limitation.

**[No prepared statements]** → JDBC and other drivers may attempt Extended Query by default. **Mitigation**: Most drivers fall back to Simple Query when Extended Query is not supported. `pgwire` handles rejecting unsupported message types gracefully. Users can configure drivers to use simple mode (e.g., `preferQueryMode=simple` in JDBC).

**[No SSL/TLS]** → Connections are plaintext. **Mitigation**: MVP is for local development and testing. SSL can be added to `pgwire`'s TLS configuration later.

**[No query cancellation]** → Long-running queries cannot be interrupted. **Mitigation**: MVP queries run against small datasets. Cancel support requires async execution or thread interruption — both are Phase 2 concerns.

**[pgwire crate dependency]** → Adds an external dependency for the wire protocol. **Mitigation**: `pgwire` is well-maintained, has a clean API, and is used by production projects. The handler trait boundary provides a clean abstraction if the crate needs to be replaced.
