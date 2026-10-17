## ADDED Requirements

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
