## MODIFIED Requirements

### Requirement: MVP configuration parameters
The system SHALL extend `ServerConfig` with a `cluster` field of type `ClusterConfig`, deserialized from the `[cluster]` TOML section. The existing parameters (`bind_address`, `port`, `max_worker_threads`, `max_memory_mb`) SHALL remain unchanged.

#### Scenario: Config with cluster section
- **WHEN** a TOML file contains both top-level server settings and a `[cluster]` section
- **THEN** both `ServerConfig` core fields and `ClusterConfig` fields are loaded correctly

#### Scenario: Config without cluster section
- **WHEN** a TOML file contains no `[cluster]` section
- **THEN** `ClusterConfig` defaults are used (role = "standalone", discovery_port = 9090)

### Requirement: Environment variable override for cluster settings
The system SHALL support `TRINO_CLUSTER_ROLE`, `TRINO_COORDINATOR_ADDRESS`, `TRINO_DISCOVERY_PORT`, and `TRINO_WORKER_ID` environment variables within the existing `apply_env_overrides()` method. These SHALL override `[cluster]` values from the config file.

#### Scenario: Env var overrides cluster role
- **WHEN** the config file has `[cluster] role = "standalone"` and `TRINO_CLUSTER_ROLE=worker` is set
- **THEN** the effective cluster role is `"worker"`

### Requirement: Config validation for cluster settings
The system SHALL extend `validate()` to include cluster config validation: role must be one of `standalone`, `coordinator`, `worker`; workers require non-empty `coordinator_address`; `discovery_port` must be > 0.

#### Scenario: Invalid cluster role in config
- **WHEN** the config resolves to `cluster.role = "invalid"`
- **THEN** `validate()` returns a `ConfigError::InvalidValue` with a message listing valid roles

#### Scenario: Worker missing coordinator address
- **WHEN** the config resolves to `cluster.role = "worker"` and `cluster.coordinator_address` is empty
- **THEN** `validate()` returns a `ConfigError::InvalidValue` indicating coordinator_address is required

### Requirement: Config Display for cluster settings
The `ServerConfig` `Display` implementation SHALL include the cluster role, and for worker mode, the coordinator_address and worker_id in the human-readable output.

#### Scenario: Display coordinator config
- **WHEN** the server config is displayed for a coordinator node
- **THEN** the output includes `"role: coordinator"` and `"discovery_port: 9090"`

#### Scenario: Display worker config
- **WHEN** the server config is displayed for a worker node
- **THEN** the output includes `"role: worker"`, `"coordinator_address: 10.0.0.1:9090"`, and `"worker_id: worker-1"`
