## MODIFIED Requirements

### Requirement: CLI argument parsing
The system SHALL accept an additional `--role` command-line argument using `clap` with derive API. The `--role` argument SHALL accept values `standalone` (default), `coordinator`, or `worker`. The existing `--config`, `--bind`, and `--port` arguments SHALL remain unchanged.

#### Scenario: No role argument — defaults to standalone
- **WHEN** the binary is invoked with no `--role` argument
- **THEN** the server starts in standalone mode (backwards compatible with current behavior)

#### Scenario: Coordinator role
- **WHEN** the binary is invoked with `--role coordinator`
- **THEN** the server starts in coordinator mode

#### Scenario: Worker role
- **WHEN** the binary is invoked with `--role worker`
- **THEN** the server starts in worker mode

#### Scenario: Invalid role
- **WHEN** the binary is invoked with `--role leader`
- **THEN** the system prints an error indicating valid role values and exits with a non-zero exit code

### Requirement: Role-based startup branching
The system SHALL branch the startup sequence based on the resolved role after config loading and tracing initialization. The common startup path (parse CLI → load config → init tracing) SHALL be shared across all roles. After common startup, the system SHALL call `start_standalone()`, `start_coordinator()`, or `start_worker()` based on the role.

#### Scenario: Standalone startup
- **WHEN** role is `standalone`
- **THEN** the server starts coordinator + worker in the same process using in-process communication
- **AND** the pgwire listener accepts client connections
- **AND** existing single-node behavior is preserved

#### Scenario: Coordinator startup
- **WHEN** role is `coordinator`
- **THEN** the server starts pgwire listener + Flight RPC server + NodeRegistry + DistributedQueryRunner
- **AND** logs indicate coordinator mode

#### Scenario: Worker startup
- **WHEN** role is `worker`
- **THEN** the server starts Flight RPC server + TaskManager + heartbeat loop
- **AND** no pgwire listener is started
- **AND** logs indicate worker mode with worker_id

### Requirement: Startup banner role indication
The system SHALL include the server role in the startup banner. The banner SHALL show `"role: standalone"`, `"role: coordinator"`, or `"role: worker"` alongside the existing listening address and table count information.

#### Scenario: Coordinator banner
- **WHEN** the server starts as coordinator
- **THEN** the log output includes `"role: coordinator"` and the pgwire and Flight RPC listening addresses

#### Scenario: Worker banner
- **WHEN** the server starts as worker
- **THEN** the log output includes `"role: worker"`, the worker_id, and the Flight RPC listening address
