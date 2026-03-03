## Why

trino-alt has a complete single-node query pipeline (SQL parsing → logical planning → physical execution) with connectors for in-memory tables and CSV/Parquet files, and a PostgreSQL wire protocol handler. However, there is no runnable binary that wires these components together. Users cannot start a trino-alt server. The server crate is the final piece of the MVP — it creates the main binary that initializes all subsystems, registers built-in connectors, and starts accepting client connections.

## What Changes

- New `crates/server` binary crate that serves as the main entry point for trino-alt
- Wires together: `ServerConfig` (from common) → `CatalogManager` (from catalog) → `ConnectorRegistry` with memory + file connectors → `ProtocolServer` (from protocol)
- Initializes `tracing` subscriber for structured logging
- Derives `ProtocolConfig` from `ServerConfig` (combining `bind_address` + `port` into the socket address, using port 5433 to avoid PostgreSQL conflicts)
- Registers the `memory` connector (with an empty default catalog) and the `file` connector on startup
- Handles graceful shutdown on Ctrl+C / SIGTERM via `tokio::signal`
- Provides minimal CLI: `--config <path>` for config file, `--bind <address>`, `--port <port>` overrides

## Capabilities

### New Capabilities
- `server-startup`: Complete server lifecycle — CLI argument parsing, config loading, tracing initialization, catalog/connector wiring, protocol server start, and graceful shutdown handling

### Modified Capabilities

_(none)_

## Impact

- **New crate**: `crates/server` (binary, not a library)
- **Dependencies**: All existing crates (`trino-common`, `trino-catalog`, `trino-connectors`, `trino-protocol`), plus `tokio` (full runtime), `tracing-subscriber`, `clap` (CLI parsing)
- **Workspace**: Add `crates/server` to workspace members, add `clap` and `tracing-subscriber` to workspace dependencies
- **Build artifact**: Produces the `trino-alt` binary (`[[bin]] name = "trino-alt"`)
- **No changes** to existing crate code — pure orchestration layer
