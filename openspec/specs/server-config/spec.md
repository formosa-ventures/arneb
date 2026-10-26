# server-config Specification

## Purpose
Server configuration: TOML file, `ARNEB_*` environment overrides, CLI flags, and their validation.
## Requirements
### Requirement: TOML configuration file loading
The system SHALL load server configuration from a TOML file. The default path SHALL be `./arneb.toml`. If the file does not exist, the system SHALL use default values for all settings.

#### Scenario: Loading from default path
- **WHEN** the server starts and `./arneb.toml` exists with `bind_address = "0.0.0.0"`
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
The system SHALL support overriding any config value via environment variables. Environment variables SHALL be prefixed with `TRINO_` and use uppercase with underscores (e.g., `ARNEB_BIND_ADDRESS`). Environment variables SHALL take precedence over file values.

#### Scenario: Env var overrides file value
- **WHEN** the config file has `port = 8080` and `ARNEB_PORT=9090` is set
- **THEN** the effective port is `9090`

#### Scenario: Env var with no file
- **WHEN** no config file exists and `ARNEB_PORT=7070` is set
- **THEN** the effective port is `7070` with all other values at defaults

#### Scenario: Invalid env var value
- **WHEN** `ARNEB_PORT=not_a_number` is set
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

### Requirement: [execution] config block

The system SHALL accept an `[execution]` table in `arneb.toml` with the following keys:

```toml
[execution]
target_partitions = 14            # default: num_cpus::get()
parallel_file_scan = true         # default: true
parallel_hash_aggregate = true    # default: true
parallel_aggregate_min_groups = 1024  # default: 1024
channel_capacity = 4              # default: 4 (mpsc capacity per partition)
```

When omitted, defaults apply. Invalid values (e.g. `target_partitions = 0`) SHALL produce a clear startup error before serving traffic.

#### Scenario: Default target_partitions equals CPU count

- **GIVEN** `arneb.toml` has no `[execution]` block
- **WHEN** the server starts on a 14-core machine
- **THEN** `target_partitions` resolves to `14` (from `num_cpus::get()`)

#### Scenario: Override via toml

- **GIVEN** `arneb.toml` containing `[execution] target_partitions = 4`
- **WHEN** the server starts
- **THEN** `target_partitions` resolves to `4`

#### Scenario: Invalid value rejected

- **GIVEN** `arneb.toml` containing `[execution] target_partitions = 0`
- **WHEN** the server starts
- **THEN** startup fails with a clear error citing the invalid key

### Requirement: --target-partitions CLI override

The system SHALL accept a `--target-partitions=N` CLI flag on the `arneb` binary that overrides `[execution] target_partitions` from `arneb.toml`. Precedence (highest first): CLI > env (`ARNEB_TARGET_PARTITIONS`) > file > default.

#### Scenario: CLI overrides file

- **GIVEN** `arneb.toml` has `target_partitions = 4` and CLI flag `--target-partitions=8`
- **WHEN** the server starts
- **THEN** `target_partitions` resolves to `8`

### Requirement: PostgreSQL catalog entries
`[[catalogs]]` entries SHALL accept `type = "postgresql"` alongside `"hive"`.
A `postgresql` entry SHALL accept `connection_url`, `user`, `password`,
`max_connections`, `split_min_rows`, `max_splits`,
`unconstrained_numeric_scale`, `domain_compaction_threshold` and
`default_schema`. `domain_compaction_threshold` SHALL default to 1000 and SHALL
be at least 1. Hive-only fields (`metastore_uri`, `storage`) SHALL be ignored on
a `postgresql` entry, and PostgreSQL-only fields SHALL be ignored on a `hive`
entry. The effective non-secret settings of each mounted PostgreSQL catalog
SHALL be logged at startup. An unknown catalog `type` SHALL keep today's
behavior: logged and skipped.

#### Scenario: Mixed catalogs
- **WHEN** the config has one `hive` catalog and one `postgresql` catalog
- **THEN** both are mounted and both can be referenced in the same query

#### Scenario: Effective settings logged
- **WHEN** a `postgresql` catalog is mounted
- **THEN** a startup log line shows its name, host, database, `max_connections`, `split_min_rows`, `max_splits` and `domain_compaction_threshold`, without the password

#### Scenario: Invalid compaction threshold
- **WHEN** a `postgresql` entry sets `domain_compaction_threshold = 0`
- **THEN** startup fails with an error naming the catalog and the field

### Requirement: Heartbeat settings
`[cluster]` SHALL accept `heartbeat_interval_secs` (how often a worker sends
its heartbeat; default 2) and `heartbeat_timeout_secs` (how long the
coordinator waits before treating a silent worker as lost; default 10).
`ARNEB_HEARTBEAT_INTERVAL_SECS` and `ARNEB_HEARTBEAT_TIMEOUT_SECS` SHALL
override them. Both SHALL be at least 1, the timeout SHALL be greater than
the interval, and the effective values SHALL be logged at startup.

#### Scenario: Defaults
- **WHEN** neither setting nor variable is given
- **THEN** workers heartbeat every 2 s and the coordinator's timeout is 10 s

#### Scenario: Invalid combination
- **WHEN** `heartbeat_timeout_secs = 2` and `heartbeat_interval_secs = 2`
- **THEN** startup fails with an error naming both settings

### Requirement: [admission] config section
The system SHALL accept an optional `[admission]` TOML section with `max_running_queries` (integer, default `8`, `0` = unlimited), `max_queued_queries` (integer, default `256`) and `statement_timeout` (milliseconds or a duration string, default `0` = off). Unknown keys SHALL be rejected. `ARNEB_MAX_RUNNING_QUERIES`, `ARNEB_MAX_QUEUED_QUERIES` and `ARNEB_STATEMENT_TIMEOUT` SHALL override the file values. An unparseable value SHALL be a startup error. Coordinator and standalone nodes SHALL log the effective values on the `arneb::config` target at startup.

#### Scenario: Defaults
- **WHEN** the config has no `[admission]` section and no `ARNEB_*` admission variables are set
- **THEN** the coordinator logs `max_running_queries=8 max_queued_queries=256 statement_timeout="0"` on `arneb::config`

#### Scenario: Env override
- **WHEN** the file sets `max_running_queries = 4` and `ARNEB_MAX_RUNNING_QUERIES=16`
- **THEN** the effective cap is 16

#### Scenario: Invalid timeout
- **WHEN** `[admission] statement_timeout = "fast"`
- **THEN** startup fails with a configuration error naming `statement_timeout`

### Requirement: Per-query memory limit setting
`[memory] query_max_memory_per_node` (bytes, overridable by `ARNEB_QUERY_MAX_MEMORY_BYTES`) SHALL limit each query's memory on each node, as specified by `query-memory-limit`. It SHALL NOT be applied as a single limit shared by all queries on a node. Every node SHALL log the effective value, or `off`, on the `arneb::config` target at startup.

#### Scenario: Logged per node
- **WHEN** a worker starts with `ARNEB_QUERY_MAX_MEMORY_BYTES=2147483648`
- **THEN** it logs `query_max_memory_per_node=2147483648` on `arneb::config`, described as a per-query limit

### Requirement: Per-node cluster settings from the environment
The server SHALL read `ARNEB_CLUSTER_COORDINATOR_ADDRESS`,
`ARNEB_CLUSTER_WORKER_ID` and `ARNEB_CLUSTER_ADVERTISED_ADDRESS` and, when
set, use them in place of `[cluster]` `coordinator_address`, `worker_id` and
`advertised_address` (env > file). An empty value SHALL be rejected with an
error naming the variable.

#### Scenario: One config file for every worker
- **WHEN** two workers load the same config file, with different `ARNEB_CLUSTER_WORKER_ID` and `ARNEB_CLUSTER_ADVERTISED_ADDRESS` values
- **THEN** each registers with the coordinator under its own id and address

#### Scenario: Env wins over the file
- **WHEN** the file sets `worker_id = "a"` and `ARNEB_CLUSTER_WORKER_ID=b`
- **THEN** the worker registers as `b`

#### Scenario: Empty value
- **WHEN** `ARNEB_CLUSTER_WORKER_ID` is set to an empty string
- **THEN** startup fails with an error naming `ARNEB_CLUSTER_WORKER_ID`

### Requirement: Access control settings
The `[access_control]` section SHALL accept `rules_file`, the path of the
access rules file, overridden by `ARNEB_ACCESS_CONTROL_RULES_FILE`
(env > file). A file that cannot be read or parsed SHALL stop startup with
an error naming the path.

#### Scenario: Env override
- **WHEN** the config sets `rules_file = "/a.json"` and `ARNEB_ACCESS_CONTROL_RULES_FILE=/b.json`
- **THEN** the server loads `/b.json`

### Requirement: Query retry settings
`[cluster] retry_policy` SHALL accept `none` (default) or `query`, case
insensitive, overridden by `ARNEB_RETRY_POLICY`. `[cluster]
query_retry_attempts` SHALL default to 4 and SHALL be at least 1, overridden
by `ARNEB_QUERY_RETRY_ATTEMPTS`. Any other policy, including `task`, or an
invalid count SHALL stop startup with an error naming the setting. The
coordinator SHALL log the effective policy and attempt count on
`arneb::config`.

#### Scenario: Task policy
- **WHEN** `retry_policy = "task"`
- **THEN** the coordinator refuses to start, saying `task` is not supported

#### Scenario: Env override
- **WHEN** the file sets `retry_policy = "query"` and `ARNEB_RETRY_POLICY=none` is set
- **THEN** retry is off

### Requirement: [tls] config section
The system SHALL accept an optional `[tls]` TOML section with `cert_path` and `key_path` (both required when the section is present), plus `pgwire` (`"required"` by default, or `"optional"`). Unknown keys SHALL be rejected. `ARNEB_TLS_CERT_PATH`, `ARNEB_TLS_KEY_PATH` and `ARNEB_TLS_PGWIRE` SHALL override the file values, and setting both path variables SHALL enable TLS without a `[tls]` section. The effective TLS mode SHALL be logged at startup on the `arneb::config` target, for example `tls_pgwire="required" tls_trino="https"`, or `tls="off"`.

#### Scenario: Section omitted
- **WHEN** the config has no `[tls]` section and no `ARNEB_TLS_*` variables are set
- **THEN** both listeners run without TLS and the server logs `tls="off"`

#### Scenario: Only one path configured
- **WHEN** `[tls]` sets `cert_path` but not `key_path`
- **THEN** startup fails with a configuration error naming `key_path`

#### Scenario: Unknown pgwire mode
- **WHEN** `[tls] pgwire = "prefer"`
- **THEN** startup fails with a configuration error listing `required` and `optional`

### Requirement: Web UI bind address
The system SHALL accept an optional `[web] bind_address`, overridable by `ARNEB_WEB_BIND_ADDRESS`, for the Web UI listener; the Web UI port stays at the pgwire port + 1000. When `[web] bind_address` is unset, the Web UI SHALL bind to `127.0.0.1` if `[auth] type = "password"`, and to the server's `bind_address` otherwise. An explicitly configured value SHALL always be used. The effective Web UI bind address and the reason for it (`default`, `loopback (auth=password)` or `explicit`) SHALL be logged on the `arneb::config` target. A WARN SHALL be logged when password auth is on and the Web UI is explicitly bound to a non-loopback address. An unparseable address SHALL be a startup error.

#### Scenario: No auth, no web setting
- **WHEN** `[auth]` is absent, `bind_address = "0.0.0.0"`, and `[web]` is not configured
- **THEN** the Web UI binds to `0.0.0.0:<port+1000>` exactly as before, with reason `default`

#### Scenario: Password auth defaults to loopback
- **WHEN** `[auth] type = "password"`, `bind_address = "0.0.0.0"`, and `[web] bind_address` is unset
- **THEN** the Web UI binds to `127.0.0.1:<port+1000>` and logs reason `loopback (auth=password)`

#### Scenario: Explicit bind under password auth
- **WHEN** `[auth] type = "password"` and `ARNEB_WEB_BIND_ADDRESS=0.0.0.0`
- **THEN** the Web UI binds to `0.0.0.0:<port+1000>`, logs reason `explicit`, and logs a WARN that the Web UI is unauthenticated

### Requirement: Cluster security settings
The `[cluster]` section SHALL accept an optional `secret` (overridable by `ARNEB_CLUSTER_SECRET`) and an optional `[cluster.tls]` subsection with `cert_path`, `key_path` and `ca_path`, all required when the subsection is present (overridable by `ARNEB_CLUSTER_TLS_CERT_PATH`, `ARNEB_CLUSTER_TLS_KEY_PATH` and `ARNEB_CLUSTER_TLS_CA_PATH`). A secret shorter than 32 characters SHALL be a startup error. The secret SHALL be redacted from `Debug` and `Display` output and from logs. Startup SHALL log whether cluster auth and cluster TLS are on, without the secret value, and SHALL log a WARN when a secret is set without `[cluster.tls]` on a non-loopback bind address.

#### Scenario: Secret from the environment
- **WHEN** `ARNEB_CLUSTER_SECRET` holds a 40-character value and the file has no `secret`
- **THEN** the node starts with cluster auth on and logs `cluster.auth="secret"` without printing the value

#### Scenario: Short secret
- **WHEN** `[cluster] secret` is 10 characters long
- **THEN** startup fails with a configuration error stating the 32-character minimum

#### Scenario: Secret without cluster TLS on a public interface
- **WHEN** a secret is set, `[cluster.tls]` is absent, and the bind address is `0.0.0.0`
- **THEN** the node starts and logs a WARN that the secret crosses the network unencrypted

