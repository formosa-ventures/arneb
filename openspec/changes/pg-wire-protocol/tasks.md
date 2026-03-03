## 1. Crate Setup

- [x] 1.1 Add `crates/protocol` to workspace members in root `Cargo.toml`
- [x] 1.2 Add `tokio`, `pgwire`, `async-trait`, `futures`, and `bytes` to workspace dependencies in root `Cargo.toml`
- [x] 1.3 Create `crates/protocol/Cargo.toml` with package name `trino-protocol`, dependencies: `trino-common`, `trino-sql-parser`, `trino-catalog`, `trino-planner`, `trino-execution`, `trino-connectors`, `tokio` (features: rt-multi-thread, net, macros), `pgwire`, `async-trait`, `futures`, `bytes`, `tracing`, `thiserror`
- [x] 1.4 Create `crates/protocol/src/lib.rs` with module declarations (`server`, `handler`, `encoding`, `error`, `session`) and public re-exports (`ProtocolServer`, `ProtocolConfig`)

## 2. Error Types (`error` module)

- [x] 2.1 Define `ProtocolError` enum with variants: `Io(std::io::Error)`, `Pipeline(TrinoError)` using `thiserror`
- [x] 2.2 Implement `trino_error_to_pg_error()` function mapping TrinoError variants to pgwire ErrorResponse with SQLSTATE codes (ParseError→42601, PlanError→42P01/42703, ExecutionError→XX000, ConnectorError→58030, CatalogError→3D000, ConfigError→F0000)

## 3. Type Encoding (`encoding` module)

- [x] 3.1 Implement `datatype_to_pg_type()` function mapping Arrow/trino `DataType` to pgwire `Type` (Int32→INT4, Utf8→VARCHAR, Boolean→BOOL, etc.) with TEXT as fallback for unsupported types
- [x] 3.2 Implement `column_info_to_field_info()` function converting `Vec<ColumnInfo>` to `Vec<FieldInfo>` for RowDescription messages, using text format (format code 0)
- [x] 3.3 Implement `encode_record_batches()` function that converts `Vec<RecordBatch>` into a stream of pgwire DataRow entries, encoding each column value as text format and handling NULLs
- [x] 3.4 Implement per-type text encoding helpers: Boolean→"t"/"f", integers→decimal string, floats→decimal string, Decimal128→scaled decimal string, Date32→"YYYY-MM-DD", Timestamp→"YYYY-MM-DD HH:MM:SS", Utf8→passthrough, Binary→hex

## 4. Session State (`session` module)

- [x] 4.1 Session state managed by pgwire's ClientInfo internally; session module reserved for future custom state
- [x] 4.2 Startup parameters saved to client metadata via pgwire's `save_startup_parameters_to_metadata`

## 5. Connection Handler (`handler` module)

- [x] 5.1 Define `ConnectionHandler` struct holding `Arc<CatalogManager>` and `Arc<ConnectorRegistry>`
- [x] 5.2 Implement `StartupHandler` for `ConnectionHandler` — save startup params, send AuthenticationOk via `finish_authentication`, pgwire handles SSLRequest and protocol version
- [x] 5.3 Implement `SimpleQueryHandler` for `ConnectionHandler` — receive query string, handle empty queries with EmptyQueryResponse
- [x] 5.4 Implement the query pipeline inside the SimpleQueryHandler: parse SQL → create QueryPlanner → plan with CatalogManager → create ExecutionContext → register data sources via ConnectorRegistry → create physical plan → execute
- [x] 5.5 Wrap the synchronous query pipeline execution in `tokio::task::spawn_blocking`
- [x] 5.6 Encode execution results: convert schema to FieldInfo (RowDescription), encode RecordBatches as DataRow stream, generate CommandComplete tag ("SELECT N")
- [x] 5.7 Handle pipeline errors: catch TrinoError from any stage, convert to ErrorResponse using `trino_error_to_pg_error()`, send ErrorResponse + ReadyForQuery
- [x] 5.8 Implement `PgWireHandlerFactory` for the handler factory — return Arc'd handler instances for simple_query_handler and startup_handler

## 6. TCP Server (`server` module)

- [x] 6.1 Define `ProtocolConfig` struct with `bind_address: String`, implement `Default` with "127.0.0.1:5433"
- [x] 6.2 Define `ProtocolServer` struct holding `ProtocolConfig`, `Arc<CatalogManager>`, `Arc<ConnectorRegistry>`
- [x] 6.3 Implement `ProtocolServer::new()` constructor
- [x] 6.4 Implement `ProtocolServer::start()` — bind TcpListener, accept connections in a loop, spawn per-connection tasks using `pgwire::tokio::process_socket`, handle accept errors with tracing::warn

## 7. Tests — Error Mapping

- [x] 7.1 Test `trino_error_to_pg_error()` maps ParseError to SQLSTATE 42601
- [x] 7.2 Test `trino_error_to_pg_error()` maps PlanError to SQLSTATE 42P01
- [x] 7.3 Test `trino_error_to_pg_error()` maps ExecutionError to SQLSTATE XX000
- [x] 7.4 Test `trino_error_to_pg_error()` maps CatalogError to SQLSTATE 3D000

## 8. Tests — Type Encoding

- [x] 8.1 Test `datatype_to_pg_type()` maps Int32→INT4, Utf8→VARCHAR, Boolean→BOOL, Float64→FLOAT8
- [x] 8.2 Test `datatype_to_pg_type()` returns TEXT for unsupported types
- [x] 8.3 Test `column_info_to_field_info()` converts a multi-column schema correctly
- [x] 8.4 Test text encoding of Int32 values (positive, negative, zero)
- [x] 8.5 Test text encoding of Boolean values ("t"/"f")
- [x] 8.6 Test text encoding of Float64 values
- [x] 8.7 Test text encoding of Utf8 values (passthrough)
- [x] 8.8 Test text encoding of NULL values (returns None)
- [x] 8.9 Test text encoding of Date32 and Timestamp values
- [x] 8.10 Test `encode_record_batches()` produces correct number of DataRow entries with correct values

## 9. Tests — Integration

- [x] 9.1 Integration test: start server on a random port, connect with a raw TCP client, perform startup handshake, verify AuthenticationOk and ReadyForQuery received
- [x] 9.2 Integration test: register a memory table, connect, send `SELECT id, name FROM users`, verify RowDescription + DataRow + CommandComplete + ReadyForQuery
- [x] 9.3 Integration test: connect, send an invalid SQL query, verify ErrorResponse + ReadyForQuery
- [x] 9.4 Integration test: connect, send Terminate, verify connection closes gracefully
- [x] 9.5 Integration test: register a memory table, connect, query the table, verify correct result rows (combined with 9.2)

## 10. Quality & Build Verification

- [x] 10.1 `cargo build` compiles without warnings
- [x] 10.2 `cargo test -p trino-protocol` all tests pass (19 unit + 4 integration)
- [x] 10.3 `cargo clippy -- -D warnings` clean
- [x] 10.4 `cargo fmt -- --check` clean
