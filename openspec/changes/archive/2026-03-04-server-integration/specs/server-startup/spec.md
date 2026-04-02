## ADDED Requirements

### Requirement: CLI argument parsing
The system SHALL accept command-line arguments using `clap` with derive API. Supported arguments SHALL be: `--config <path>` (path to TOML config file), `--bind <address>` (override bind address), and `--port <port>` (override listen port). All arguments SHALL be optional. The binary name SHALL be `arneb`.

#### Scenario: No arguments — use defaults
- **WHEN** the binary is invoked with no arguments
- **THEN** the server loads config from `./arneb.toml` if it exists, otherwise uses defaults (bind `127.0.0.1`, port `5432`)

#### Scenario: Explicit config file path
- **WHEN** the binary is invoked with `--config /path/to/config.toml`
- **THEN** the server loads config from that file and returns an error if the file does not exist

#### Scenario: CLI bind and port override
- **WHEN** the binary is invoked with `--bind 0.0.0.0 --port 5433`
- **THEN** the server uses `0.0.0.0:5433` regardless of values in the config file or environment variables

#### Scenario: Help text
- **WHEN** the binary is invoked with `--help`
- **THEN** the system prints usage information listing all supported arguments and exits

### Requirement: Configuration loading with precedence
The system SHALL load configuration following the precedence order: CLI arguments > environment variables > config file > defaults. The system SHALL use `ServerConfig::load()` for file + env + validation, then apply CLI overrides on top. The server crate SHALL define a local `AppConfig` struct that embeds `ServerConfig` via `#[serde(flatten)]` and adds a `tables` array for declarative table registration.

#### Scenario: CLI overrides env var
- **WHEN** `ARNEB_PORT=9999` is set and the binary is invoked with `--port 5433`
- **THEN** the server listens on port `5433` (CLI wins)

#### Scenario: Env var overrides file
- **WHEN** the config file sets `port = 8080` and `ARNEB_PORT=9999` is set
- **THEN** the server listens on port `9999` (env wins over file)

#### Scenario: File overrides defaults
- **WHEN** the config file sets `port = 8080` and no env vars or CLI args are set
- **THEN** the server listens on port `8080` (file wins over default `5432`)

#### Scenario: Invalid config file
- **WHEN** the config file contains invalid TOML syntax
- **THEN** the server prints an error message and exits with a non-zero exit code

#### Scenario: Config validation failure
- **WHEN** the config resolves to `port = 0` after all overrides
- **THEN** the server prints a validation error and exits with a non-zero exit code

### Requirement: Tracing initialization
The system SHALL initialize `tracing-subscriber` with a `fmt` subscriber and `EnvFilter` for log-level control. The default log level SHALL be `info`. Users SHALL control verbosity via the `RUST_LOG` environment variable (e.g., `RUST_LOG=debug`, `RUST_LOG=trino_protocol=debug`). Tracing SHALL be initialized before any other subsystem so all startup logs are captured.

#### Scenario: Default log level
- **WHEN** the server starts without `RUST_LOG` set
- **THEN** log output includes `info` level messages and above, but not `debug` or `trace`

#### Scenario: Custom log level
- **WHEN** the server starts with `RUST_LOG=debug`
- **THEN** log output includes `debug` level messages and above

#### Scenario: Per-crate log level
- **WHEN** the server starts with `RUST_LOG=trino_protocol=debug,info`
- **THEN** the `trino_protocol` crate logs at `debug` level while other crates log at `info` level

### Requirement: Default catalog and connector registration
The system SHALL register a `memory` catalog with an empty `default` schema, and register a `MemoryConnectorFactory` in the `ConnectorRegistry` under the name `"memory"`. The `CatalogManager` SHALL be created with default catalog `"memory"` and default schema `"default"`.

#### Scenario: Server starts with empty memory catalog
- **WHEN** the server starts with no `[[tables]]` in the config
- **THEN** the `CatalogManager` has a `"memory"` catalog with a `"default"` schema containing no tables
- **AND** the `ConnectorRegistry` has a `"memory"` connector registered

#### Scenario: Query against empty catalog
- **WHEN** a client connects and sends `SELECT * FROM nonexistent`
- **THEN** the server returns an ErrorResponse indicating the table was not found

### Requirement: Declarative file table registration from config
The system SHALL parse `[[tables]]` entries from the config file. Each entry SHALL specify `name` (table name), `path` (file system path), and `format` (`"csv"` or `"parquet"`). CSV entries SHALL additionally require a `schema` array of `{ name, type }` objects defining column names and data types. Parquet entries SHALL NOT require a schema (it is read from file metadata). All tables SHALL be registered under the `"file"` catalog's `"default"` schema. A `FileConnectorFactory` SHALL be registered in the `ConnectorRegistry` under the name `"file"`.

#### Scenario: Register a Parquet table from config
- **WHEN** the config contains `[[tables]]` with `name = "lineitem"`, `path = "/data/lineitem.parquet"`, `format = "parquet"`
- **THEN** at startup, the server registers a table named `"lineitem"` in the `file.default` schema backed by the Parquet file
- **AND** a client can query `SELECT * FROM file.default.lineitem`

#### Scenario: Register a CSV table with schema
- **WHEN** the config contains `[[tables]]` with `name = "orders"`, `path = "/data/orders.csv"`, `format = "csv"`, and `schema = [{ name = "id", type = "int32" }, { name = "total", type = "float64" }]`
- **THEN** at startup, the server registers a table named `"orders"` with the specified column schema
- **AND** a client can query `SELECT id, total FROM file.default.orders`

#### Scenario: Missing file path at startup
- **WHEN** the config contains a `[[tables]]` entry with a `path` pointing to a nonexistent file
- **THEN** the server logs a warning about the missing file but continues startup (the table registration may fail at query time)

#### Scenario: CSV table without schema
- **WHEN** the config contains a CSV `[[tables]]` entry without a `schema` field
- **THEN** the server prints an error indicating that CSV tables require an explicit schema and exits with a non-zero exit code

#### Scenario: No tables configured
- **WHEN** the config contains no `[[tables]]` entries
- **THEN** the server starts successfully with only the empty `memory` catalog (no `file` catalog is registered)

#### Scenario: Supported data type names in schema
- **WHEN** a CSV table schema specifies column types
- **THEN** the system SHALL accept the following type names: `boolean`, `int8`, `int16`, `int32`, `int64`, `float32`, `float64`, `utf8`, `date32`, `timestamp`

### Requirement: ProtocolConfig derivation from ServerConfig
The system SHALL construct `ProtocolConfig` by combining `ServerConfig`'s `bind_address` and `port` fields into a single `bind_address` string in `"host:port"` format. No separate protocol configuration file or fields SHALL exist.

#### Scenario: Default config derivation
- **WHEN** `ServerConfig` has `bind_address = "127.0.0.1"` and `port = 5432`
- **THEN** `ProtocolConfig` is created with `bind_address = "127.0.0.1:5432"`

#### Scenario: Custom config derivation
- **WHEN** `ServerConfig` has `bind_address = "0.0.0.0"` and `port = 5433`
- **THEN** `ProtocolConfig` is created with `bind_address = "0.0.0.0:5433"`

### Requirement: Protocol server startup
The system SHALL create a `ProtocolServer` with the derived `ProtocolConfig`, `Arc<CatalogManager>`, and `Arc<ConnectorRegistry>`, then call `start()` to begin accepting TCP connections. The server SHALL log the listening address at `info` level before entering the accept loop.

#### Scenario: Successful startup
- **WHEN** the server initializes all subsystems and starts listening
- **THEN** the log output contains an `info` message with the listening address (e.g., `"listening on 127.0.0.1:5432"`)
- **AND** PostgreSQL clients can connect to that address

#### Scenario: Port already in use
- **WHEN** the configured port is already occupied by another process
- **THEN** the server prints an error about the address being in use and exits with a non-zero exit code

### Requirement: Startup banner
The system SHALL log a startup banner at `info` level after successful initialization, before entering the accept loop. The banner SHALL include: the server name (`arneb`), the listening address and port, the number of registered catalogs, and the number of registered tables.

#### Scenario: Banner with tables
- **WHEN** the server starts with 2 configured tables
- **THEN** the log output includes a banner showing the listening address and `"2 tables registered"`

#### Scenario: Banner with no tables
- **WHEN** the server starts with no configured tables
- **THEN** the log output includes a banner showing the listening address and `"0 tables registered"`

### Requirement: Graceful shutdown on signal
The system SHALL handle `SIGINT` (Ctrl+C) and `SIGTERM` signals for graceful shutdown. Upon receiving either signal, the server SHALL log a shutdown message at `info` level and exit the process with exit code 0. The shutdown SHALL be implemented using `tokio::select!` between the server's accept loop and `tokio::signal::ctrl_c()`.

#### Scenario: Ctrl+C shutdown
- **WHEN** the server is running and receives SIGINT (Ctrl+C)
- **THEN** the server logs `"shutting down"` at `info` level and exits with code 0

#### Scenario: Server continues running without signal
- **WHEN** the server is running and no shutdown signal is received
- **THEN** the server continues accepting and processing client connections indefinitely

### Requirement: Startup error handling
The system SHALL handle errors during startup (config loading, tracing init, catalog setup, port binding) by printing a descriptive error message to stderr and exiting with a non-zero exit code. The error message SHALL identify which subsystem failed and why. The `main()` function SHALL use `anyhow` for error propagation during startup.

#### Scenario: Config load error
- **WHEN** the config file path is specified but the file is malformed
- **THEN** the process exits with a non-zero code and stderr contains an error message mentioning the config file

#### Scenario: Table registration error
- **WHEN** a CSV table entry is missing the required schema
- **THEN** the process exits with a non-zero code and stderr contains an error message identifying the table entry
