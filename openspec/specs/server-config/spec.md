## ADDED Requirements

### Requirement: TOML configuration file loading
The system SHALL load server configuration from a TOML file. The default path SHALL be `./trino-alt.toml`. If the file does not exist, the system SHALL use default values for all settings.

#### Scenario: Loading from default path
- **WHEN** the server starts and `./trino-alt.toml` exists with `bind_address = "0.0.0.0"`
- **THEN** the config's bind_address is set to `"0.0.0.0"`

#### Scenario: Missing config file uses defaults
- **WHEN** the server starts and no config file exists at the default path
- **THEN** the system uses default values without returning an error

#### Scenario: Explicit config file path
- **WHEN** a config file path is explicitly provided (e.g., via CLI argument)
- **THEN** the system loads from that path and returns `ConfigError::FileNotFound` if the file does not exist

#### Scenario: Malformed TOML
- **WHEN** the config file contains invalid TOML syntax
- **THEN** a `ConfigError::ParseError` is returned with the parse error details

### Requirement: Environment variable override
The system SHALL support overriding any config value via environment variables. Environment variables SHALL be prefixed with `TRINO_` and use uppercase with underscores (e.g., `TRINO_BIND_ADDRESS`). Environment variables SHALL take precedence over file values.

#### Scenario: Env var overrides file value
- **WHEN** the config file has `port = 8080` and `TRINO_PORT=9090` is set
- **THEN** the effective port is `9090`

#### Scenario: Env var with no file
- **WHEN** no config file exists and `TRINO_PORT=7070` is set
- **THEN** the effective port is `7070` with all other values at defaults

#### Scenario: Invalid env var value
- **WHEN** `TRINO_PORT=not_a_number` is set
- **THEN** a `ConfigError::InvalidValue` is returned with key `"port"` and reason describing the expected type

### Requirement: MVP configuration parameters
The system SHALL define the following configuration parameters with their defaults:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `bind_address` | String | `"127.0.0.1"` | Server listen address |
| `port` | u16 | `5432` | Server listen port (PostgreSQL default) |
| `max_worker_threads` | usize | number of CPU cores | Execution thread pool size |
| `max_memory_mb` | usize | `1024` | Maximum memory usage in MB |

#### Scenario: Default configuration values
- **WHEN** a `ServerConfig` is created with `ServerConfig::default()`
- **THEN** bind_address is `"127.0.0.1"`, port is `5432`, max_memory_mb is `1024`

#### Scenario: Full config file
- **WHEN** a TOML file contains all four parameters with custom values
- **THEN** all values are loaded correctly and no defaults are used

### Requirement: Config validation
The system SHALL validate configuration values after loading. Invalid values SHALL produce `ConfigError::InvalidValue`.

#### Scenario: Port zero
- **WHEN** config has `port = 0`
- **THEN** a `ConfigError::InvalidValue` is returned indicating port must be > 0

#### Scenario: Zero memory limit
- **WHEN** config has `max_memory_mb = 0`
- **THEN** a `ConfigError::InvalidValue` is returned indicating memory must be > 0

### Requirement: Config Display and Debug
The `ServerConfig` struct SHALL implement `Debug` and `Display`. The `Display` implementation SHALL show all config values in a human-readable format suitable for startup logging.

#### Scenario: Logging config at startup
- **WHEN** the server logs its configuration at startup
- **THEN** the display output shows all parameter names and their effective values in a readable format
