## Why

Changes 1–6 built a complete single-node query pipeline (parse → plan → optimize → execute) with connectors that read from memory and files, but there is no way for external clients to connect and issue queries. The PostgreSQL wire protocol handler exposes the engine over TCP using a protocol that every major SQL client, driver, and tool already supports — psql, JDBC, Python's psycopg2, DBeaver, DataGrip, and hundreds more. Without it, the engine is a library, not a server.

## What Changes

- Create `crates/protocol/` crate (package name: `arneb-protocol`)
- Implement PostgreSQL wire protocol (v3) message parsing and serialization — startup, query, and termination message flows
- Implement a connection handler that accepts TCP connections, authenticates (trust/no-password for MVP), and manages per-connection session state
- Implement the Simple Query flow: receive SQL text → parse → plan → execute → encode Arrow RecordBatches as PostgreSQL DataRow messages → send results back to client
- Map Arrow/trino `DataType` to PostgreSQL type OIDs and implement value encoding (text format for MVP, binary format deferred)
- Map `ArnebError` variants to PostgreSQL `ErrorResponse` messages with appropriate SQLSTATE codes
- Provide a `ProtocolServer` entry point that binds to a TCP address and spawns per-connection tasks on the Tokio runtime

## Capabilities

### New Capabilities

- `pg-messages`: PostgreSQL wire protocol v3 message types — parsing incoming byte streams into typed messages (Startup, Query, Terminate, etc.) and serializing outgoing messages (AuthenticationOk, RowDescription, DataRow, CommandComplete, ErrorResponse, ReadyForQuery, etc.). Length-prefixed binary framing.
- `pg-connection`: Per-connection session handler that manages the connection lifecycle — startup handshake, authentication (trust mode), session state (current database/schema), query dispatch via Simple Query flow, and graceful termination. Integrates with the query pipeline (sql-parser → planner → execution) to process each query.
- `pg-encoding`: Type mapping and value encoding between Arrow/trino types and PostgreSQL wire format. Maps `DataType` → PostgreSQL type OIDs, encodes `RecordBatch` columns as text-format `DataRow` messages, and translates `ArnebError` into `ErrorResponse` with SQLSTATE codes.
- `pg-server`: TCP server entry point — binds to a configurable address, accepts connections, spawns per-connection async tasks. Provides `ProtocolServer` struct that holds shared state (CatalogManager, ConnectorRegistry) and a `start()` method returning a future.

### Modified Capabilities

(No existing capabilities modified)

## Impact

- **New crate**: `crates/protocol/`
- **New dependencies**: `tokio` (async runtime, TCP), `pgwire` or hand-rolled protocol implementation (to be evaluated in design), `bytes` (buffer management)
- **Dependencies on existing crates**: `arneb-common` (types, errors), `arneb-sql-parser` (parse), `arneb-planner` (plan), `arneb-catalog` (CatalogManager), `arneb-execution` (ExecutionContext, ExecutionPlan), `arneb-connectors` (ConnectorRegistry)
- **Downstream**: The `server-integration` crate (Change 8) will instantiate `ProtocolServer` with configured catalogs and connectors, wiring everything into the main binary
