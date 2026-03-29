## ADDED Requirements

### Requirement: Cluster configuration section
The system SHALL define a `ClusterConfig` struct with the following fields: `role` (String, default `"standalone"`), `coordinator_address` (String, default empty), `discovery_port` (u16, default `9090`), `worker_id` (String, default empty). This struct SHALL be embedded in `ServerConfig` under the `[cluster]` TOML section.

#### Scenario: Default cluster config
- **WHEN** no `[cluster]` section is present in the TOML file
- **THEN** `ClusterConfig` defaults to role `"standalone"`, empty coordinator_address, discovery_port `9090`, and empty worker_id

#### Scenario: Full cluster config
- **WHEN** the TOML file contains:
  ```toml
  [cluster]
  role = "worker"
  coordinator_address = "10.0.0.1:9090"
  discovery_port = 9091
  worker_id = "worker-1"
  ```
- **THEN** all fields are loaded correctly from the config file

### Requirement: Cluster config environment variable overrides
The system SHALL support environment variable overrides for cluster configuration: `TRINO_CLUSTER_ROLE`, `TRINO_COORDINATOR_ADDRESS`, `TRINO_DISCOVERY_PORT`, `TRINO_WORKER_ID`. Environment variables SHALL take precedence over file values.

#### Scenario: Env var overrides role
- **WHEN** the config file has `role = "standalone"` and `TRINO_CLUSTER_ROLE=coordinator` is set
- **THEN** the effective role is `"coordinator"`

#### Scenario: Env var sets coordinator address
- **WHEN** `TRINO_COORDINATOR_ADDRESS=10.0.0.1:9090` is set and no config file exists
- **THEN** the effective coordinator_address is `"10.0.0.1:9090"`

### Requirement: Cluster config validation
The system SHALL validate cluster configuration after loading. Workers SHALL require a non-empty `coordinator_address`. The `role` field SHALL accept only `"standalone"`, `"coordinator"`, or `"worker"`. The `discovery_port` SHALL be greater than 0.

#### Scenario: Worker without coordinator address
- **WHEN** role is `"worker"` and coordinator_address is empty
- **THEN** validation returns an error indicating coordinator_address is required for worker mode

#### Scenario: Invalid role value
- **WHEN** role is `"leader"` (not a valid role)
- **THEN** validation returns an error indicating valid role values

#### Scenario: Valid coordinator config
- **WHEN** role is `"coordinator"` and discovery_port is `9090`
- **THEN** validation passes (coordinator does not require coordinator_address)

### Requirement: Auto-generated worker_id
The system SHALL auto-generate a worker_id if the field is empty when role is `"worker"`. The generated id SHALL follow the format `"{hostname}-{random_suffix}"` where random_suffix is a 6-character alphanumeric string.

#### Scenario: Empty worker_id for worker role
- **WHEN** role is `"worker"` and worker_id is empty
- **THEN** a worker_id is auto-generated using the hostname and a random suffix

#### Scenario: Explicit worker_id
- **WHEN** role is `"worker"` and worker_id is `"worker-1"`
- **THEN** the provided worker_id is used as-is
