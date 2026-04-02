## Context

arneb has seven crates implementing a complete single-node query pipeline: `common` (shared types/config), `sql-parser` (SQL → AST), `catalog` (metadata resolution), `planner` (AST → logical plan), `execution` (physical operators on Arrow RecordBatches), `connectors` (memory + file data sources), and `protocol` (PostgreSQL wire protocol handler). The protocol crate's `ProtocolServer` accepts TCP connections and runs the full pipeline per query, but requires pre-wired `Arc<CatalogManager>` and `Arc<ConnectorRegistry>` dependencies.

The common crate provides `ServerConfig` with TOML + env var loading (`ServerConfig::load()`), defaulting to `127.0.0.1:5432`. The protocol crate provides `ProtocolConfig` with a `bind_address: String` field (e.g., `"127.0.0.1:5433"`). These need to be reconciled into a single configuration flow.

The server crate does not exist yet. No `crates/server/` directory.

## Goals / Non-Goals

**Goals:**

- Produce a `arneb` binary that initializes all subsystems and starts accepting PostgreSQL wire protocol connections
- Load configuration from TOML file + environment variables + CLI arguments with clear precedence (CLI > env > file > defaults)
- Register built-in connectors (memory, file) so queries work out of the box
- Support declarative table registration in the config file so users can query CSV/Parquet files without code
- Initialize structured logging via `tracing-subscriber`
- Handle Ctrl+C / SIGTERM for graceful shutdown
- Print a startup banner with config summary and listening address

**Non-Goals:**

- No dynamic DDL (CREATE TABLE, DROP TABLE) — tables are configured at startup
- No hot-reload of configuration — restart required for config changes
- No daemon/background mode — runs in the foreground
- No health check endpoint or metrics — pure query serving
- No multi-node coordination (Phase 2)

## Decisions

### D1: Binary crate with local `AppConfig` wrapping `ServerConfig`

**Choice**: Create `crates/server` as a binary crate (`[[bin]] name = "arneb"`). Define a local `AppConfig` struct that uses `#[serde(flatten)]` to embed `ServerConfig` and adds a `tables` array for declarative table registration.

```toml
# arneb.toml
bind_address = "0.0.0.0"
port = 5433

[[tables]]
name = "lineitem"
path = "/data/lineitem.parquet"
format = "parquet"

[[tables]]
name = "orders"
path = "/data/orders.csv"
format = "csv"
schema = [
  { name = "id", type = "int32" },
  { name = "customer", type = "utf8" },
  { name = "total", type = "float64" },
]
```

**Rationale**: `ServerConfig` in the common crate handles core server settings (bind, port, threads, memory) and should not be coupled to table registration details. `AppConfig` keeps the table registration logic local to the server crate, respecting the "no changes to existing crate code" boundary. `#[serde(flatten)]` means all `ServerConfig` fields appear at the top level of the TOML — no nesting.

**Alternative**: Extend `ServerConfig` with a `tables` field. Rejected — pollutes the shared common crate with server-specific concerns. Other crates (protocol, connectors) should not know about the config-driven table registration.

### D2: Config precedence — CLI > env > file > defaults

**Choice**: Use `clap` for CLI argument parsing with three overrides: `--config <path>`, `--bind <address>`, `--port <port>`. The loading sequence:

1. Parse CLI args via `clap`
2. Load `AppConfig` from TOML (explicit `--config` path, or default `./arneb.toml`, or defaults)
3. Apply env var overrides (`ARNEB_BIND_ADDRESS`, `ARNEB_PORT`, etc.) via `ServerConfig::apply_env_overrides()`
4. Apply CLI overrides (`--bind`, `--port`) on top
5. Validate via `ServerConfig::validate()`

**Rationale**: This follows the standard 12-factor app configuration hierarchy. `clap` is the de facto Rust CLI library and integrates well with the derive API. The existing `ServerConfig::load()` already handles steps 2-3 and 5 — we just add CLI override after env overrides.

**Alternative**: Skip `clap`, use env vars only. Rejected — CLI args provide the best developer experience for quick overrides (`arneb --port 5433`) without polluting the shell environment.

### D3: Derive `ProtocolConfig` from `ServerConfig`

**Choice**: Construct `ProtocolConfig { bind_address: format!("{}:{}", config.bind_address, config.port) }` from the loaded `ServerConfig`.

**Rationale**: `ProtocolConfig` takes a combined `host:port` string. `ServerConfig` stores them separately (for env var / CLI granularity). The derivation is a trivial format operation. No need for an additional config layer.

### D4: Declarative table registration at startup

**Choice**: Parse `[[tables]]` entries from the config file and register each as a data source at startup. Each entry specifies `name`, `path`, `format` (csv/parquet), and optionally `schema` (column definitions, required for CSV). Tables are registered under the `file` catalog's `default` schema.

**Rationale**: Without DDL support, the server needs some way to expose data. Config-driven registration is the simplest approach — users list their data files in `arneb.toml` and query them immediately via SQL. Parquet files self-describe their schema; CSV files require explicit schema definition.

**Alternative**: Auto-discover files from a `--data-dir` directory. Deferred — requires format detection, schema inference, and naming conventions. Config-based registration is explicit and predictable.

### D5: Tracing initialization with `tracing-subscriber`

**Choice**: Initialize `tracing_subscriber::fmt()` with env-filter support (`RUST_LOG`). Default level: `info`. Format: human-readable compact format for terminal use.

**Rationale**: The codebase uses `tracing` throughout (protocol crate uses `tracing::info!`, `tracing::debug!`, `tracing::warn!`). `tracing-subscriber` is the standard subscriber for `tracing`. Env-filter lets users control verbosity (`RUST_LOG=debug`, `RUST_LOG=trino_protocol=debug`).

### D6: Graceful shutdown via `tokio::select!` + `ctrl_c`

**Choice**: Run the server with `tokio::select!` between `protocol_server.start()` and `tokio::signal::ctrl_c()`. When Ctrl+C is received, log the shutdown and exit the process.

**Rationale**: `ProtocolServer::start()` runs an infinite accept loop. The only clean way to stop it is to cancel its future. `tokio::select!` provides exactly this — when ctrl_c fires, the server future is dropped. In-flight connections will terminate as their tasks are dropped. This is sufficient for MVP — graceful connection draining would be a Phase 2 enhancement.

### D7: Default catalogs — memory (empty) + file (from config)

**Choice**: Always register a `memory` catalog with an empty `default` schema, and a `file` catalog populated from config `[[tables]]` entries. The `CatalogManager` defaults to `catalog = "memory"`, `schema = "default"`.

**Rationale**: The `memory` catalog serves as the default namespace (matching the convention from earlier changes). The `file` catalog holds user data. If no `[[tables]]` entries exist, the server starts with empty catalogs — queries return "table not found" errors, which is correct and informative.

### D8: Startup sequence

**Choice**: The `main()` function follows this exact sequence:

1. Parse CLI args (`clap`)
2. Load config (`AppConfig` from TOML + env + CLI overrides)
3. Initialize tracing subscriber
4. Log startup banner with config summary
5. Create `CatalogManager` (default catalog = "memory", default schema = "default")
6. Create `ConnectorRegistry`
7. Register `memory` connector + catalog (empty)
8. Register `file` connector + catalog (from config tables)
9. Create `ProtocolServer` with derived `ProtocolConfig`
10. Log listening address
11. `tokio::select!` between `server.start()` and `ctrl_c()`
12. Log shutdown

**Rationale**: Each step has clear dependencies on the previous. The sequence is linear — no parallelism needed since initialization is fast. Errors at any step abort startup with a descriptive message.

## Risks / Trade-offs

**[Default port 5432 conflicts with PostgreSQL]** → `ServerConfig` defaults to port 5432, which conflicts with a local PostgreSQL installation. **Mitigation**: Users set `port = 5433` in config or `--port 5433` on CLI. The startup banner prominently shows the listening port. Documentation will recommend 5433.

**[CSV schema must be manually specified]** → CSV files have no embedded schema metadata, so users must declare column names and types in the config. **Mitigation**: Error messages will clearly indicate when schema is missing. Parquet files auto-detect schema and require no manual config.

**[No graceful connection draining]** → On shutdown, in-flight queries are abruptly terminated. **Mitigation**: MVP queries run against small local datasets and complete quickly. Graceful draining (send ReadyForQuery then close) requires protocol-level shutdown coordination — Phase 2.

**[clap adds a dependency]** → Adds `clap` (with derive feature) to the workspace. **Mitigation**: `clap` is the standard Rust CLI library, widely used, well-maintained. The derive feature adds compile time but minimal runtime cost. It's only used by the server binary, not by library crates.
