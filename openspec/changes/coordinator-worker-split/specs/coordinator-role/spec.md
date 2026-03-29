## ADDED Requirements

### Requirement: Coordinator startup
The system SHALL start in coordinator mode when `--role coordinator` is specified. The coordinator SHALL initialize the pgwire listener, Flight RPC server, NodeRegistry, NodeScheduler, and DistributedQueryRunner. The coordinator SHALL NOT execute data-processing tasks locally.

#### Scenario: Coordinator starts successfully
- **WHEN** the server is started with `--role coordinator` and a valid config
- **THEN** the pgwire listener starts on the configured `bind_address:port`
- **AND** the Flight RPC server starts on the configured `discovery_port`
- **AND** the log output contains an `info` message indicating coordinator mode

#### Scenario: Coordinator accepts client SQL connections
- **WHEN** the coordinator is running and a PostgreSQL client connects
- **THEN** the coordinator accepts the connection and processes SQL queries through the distributed pipeline (parse → plan → optimize → fragment → schedule → collect)

#### Scenario: Coordinator accepts worker heartbeats
- **WHEN** the coordinator is running and a worker sends a heartbeat via Flight RPC
- **THEN** the NodeRegistry is updated with the worker's status and the heartbeat is acknowledged

### Requirement: Coordinator query handling
The system SHALL route incoming SQL queries through the DistributedQueryRunner instead of direct local execution when running in coordinator mode. The coordinator SHALL parse, plan, optimize, and fragment the query, then schedule execution across registered workers.

#### Scenario: Query with no registered workers
- **WHEN** a client sends a SQL query and no workers are registered in the NodeRegistry
- **THEN** the coordinator returns an error indicating no active workers are available

#### Scenario: Query with registered workers
- **WHEN** a client sends `SELECT * FROM file.default.lineitem` and at least one worker is registered
- **THEN** the coordinator fragments the plan, schedules stages to workers, collects results, and returns rows to the client via pgwire

### Requirement: Coordinator shutdown
The system SHALL handle graceful shutdown of the coordinator on SIGINT/SIGTERM. On shutdown, the coordinator SHALL stop accepting new client connections, stop accepting new worker registrations, and log a shutdown message.

#### Scenario: Coordinator Ctrl+C shutdown
- **WHEN** the coordinator receives SIGINT
- **THEN** the pgwire listener stops accepting connections
- **AND** the Flight RPC server shuts down
- **AND** the server logs `"coordinator shutting down"` at `info` level and exits with code 0
