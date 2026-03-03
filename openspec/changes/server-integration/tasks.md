## 1. Crate Setup

- [x] 1.1 Add `crates/server` to workspace members in root `Cargo.toml`
- [x] 1.2 Add `clap` (features: derive) and `tracing-subscriber` (features: env-filter) to workspace dependencies in root `Cargo.toml`
- [x] 1.3 Add `anyhow` to workspace dependencies in root `Cargo.toml`
- [x] 1.4 Create `crates/server/Cargo.toml` as a binary crate (`[[bin]] name = "trino-alt"`) with dependencies: `trino-common`, `trino-catalog`, `trino-connectors`, `trino-protocol`, `tokio` (features: rt-multi-thread, net, macros, signal), `clap` (features: derive), `tracing`, `tracing-subscriber` (features: env-filter), `anyhow`, `serde` (features: derive), `toml`
- [x] 1.5 Create `crates/server/src/main.rs` with a placeholder `main()` that prints "trino-alt" and exits

## 2. Configuration Types

- [x] 2.1 Define `TableConfig` struct with fields: `name: String`, `path: String`, `format: String`, `schema: Option<Vec<ColumnSchema>>` — derive `Deserialize`
- [x] 2.2 Define `ColumnSchema` struct with fields: `name: String`, `r#type: String` — derive `Deserialize`
- [x] 2.3 Define `AppConfig` struct with `#[serde(flatten)] server: ServerConfig` and `#[serde(default)] tables: Vec<TableConfig>` — derive `Deserialize`
- [x] 2.4 Implement `AppConfig::load(path: Option<&Path>)` that loads from TOML file (or default `./trino-alt.toml`), applies env overrides via `server.apply_env_overrides()`, validates via `server.validate()`
- [x] 2.5 Implement `parse_data_type(type_name: &str) -> Result<DataType>` mapping string type names (`boolean`, `int8`, `int16`, `int32`, `int64`, `float32`, `float64`, `utf8`, `date32`, `timestamp`) to `DataType` enum values, returning error for unknown types

## 3. CLI Argument Parsing

- [x] 3.1 Define `CliArgs` struct using `clap` derive: `--config` (Option<PathBuf>), `--bind` (Option<String>), `--port` (Option<u16>)
- [x] 3.2 In `main()`, parse CLI args and load `AppConfig` using the `--config` path
- [x] 3.3 Apply CLI overrides: if `--bind` is set, overwrite `config.server.bind_address`; if `--port` is set, overwrite `config.server.port`
- [x] 3.4 Re-validate config after CLI overrides via `config.server.validate()`

## 4. Tracing Initialization

- [x] 4.1 Initialize `tracing_subscriber::fmt()` with `EnvFilter::from_default_env()` defaulting to `"info"` — call this before any other subsystem initialization

## 5. Catalog and Connector Wiring

- [x] 5.1 Create `CatalogManager::new("memory", "default")`
- [x] 5.2 Create a `MemoryCatalog` (from connectors) with an empty `MemorySchema` registered as `"default"`, register it in `CatalogManager` as `"memory"`
- [x] 5.3 Create `ConnectorRegistry::new()` and register a `MemoryConnectorFactory` under `"memory"`

## 6. Declarative Table Registration

- [x] 6.1 If `config.tables` is non-empty: create a `FileConnectorFactory`, iterate over `config.tables`, call `factory.register_table(name, path, format, schema)` for each entry
- [x] 6.2 For CSV tables: validate that `schema` is present, parse each `ColumnSchema` using `parse_data_type()`, convert to `Vec<ColumnInfo>`
- [x] 6.3 For Parquet tables: pass `schema = None` (auto-detected from file metadata)
- [x] 6.4 After registering all tables: create a `FileSchema` wrapping the factory, create a `FileCatalog` with `"default"` schema, register the catalog in `CatalogManager` as `"file"`
- [x] 6.5 Register the `FileConnectorFactory` in `ConnectorRegistry` under `"file"`
- [x] 6.6 If `config.tables` is empty, skip file catalog/connector registration entirely

## 7. Server Startup and Shutdown

- [x] 7.1 Derive `ProtocolConfig` from `ServerConfig`: `bind_address = format!("{}:{}", config.server.bind_address, config.server.port)`
- [x] 7.2 Create `ProtocolServer::new(protocol_config, Arc::new(catalog_manager), Arc::new(connector_registry))`
- [x] 7.3 Log startup banner at `info` level: server name, listening address, number of catalogs, number of registered tables
- [x] 7.4 Run `tokio::select!` between `server.start()` and `tokio::signal::ctrl_c()`
- [x] 7.5 On Ctrl+C: log `"shutting down"` at `info` level, return `Ok(())`
- [x] 7.6 Wrap `main()` body with `anyhow::Result<()>` return type and `#[tokio::main]`

## 8. Tests — Configuration

- [x] 8.1 Test `AppConfig` deserialization from a TOML string with `[[tables]]` entries (both CSV and Parquet)
- [x] 8.2 Test `AppConfig` deserialization from a TOML string with no `[[tables]]` section (defaults to empty vec)
- [x] 8.3 Test `parse_data_type()` maps all 10 supported type names correctly
- [x] 8.4 Test `parse_data_type()` returns error for unknown type name
- [x] 8.5 Test `AppConfig` with `#[serde(flatten)]` correctly captures `bind_address`, `port` at the top level alongside `[[tables]]`

## 9. Tests — Integration

- [x] 9.1 Integration test: build full server state (CatalogManager + ConnectorRegistry + memory connector), start ProtocolServer on random port, connect with raw TCP client, verify startup handshake (AuthenticationOk + ReadyForQuery)
- [x] 9.2 Integration test: configure a Parquet table via `FileConnectorFactory`, wire up full server state, start server, connect, query the table, verify RowDescription + DataRow + CommandComplete
- [x] 9.3 Integration test: start server with empty catalogs, connect, send `SELECT * FROM nonexistent`, verify ErrorResponse
- [x] 9.4 Test that `ProtocolConfig` derivation from `ServerConfig` produces correct `"host:port"` format

## 10. Quality & Build Verification

- [x] 10.1 `cargo build` compiles without warnings
- [x] 10.2 `cargo test -p trino-server` all tests pass
- [x] 10.3 `cargo clippy -- -D warnings` clean
- [x] 10.4 `cargo fmt -- --check` clean
- [x] 10.5 `cargo run --bin trino-alt -- --help` prints usage and exits
