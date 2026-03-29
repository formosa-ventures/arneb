## ADDED Requirements

### Requirement: Worker startup
The system SHALL start in worker mode when `--role worker` is specified. The worker SHALL initialize the Flight RPC server, TaskManager, and a heartbeat loop to the coordinator. The worker SHALL NOT start a pgwire listener or accept direct client SQL connections.

#### Scenario: Worker starts successfully
- **WHEN** the server is started with `--role worker` and a valid `coordinator_address` in the config
- **THEN** the Flight RPC server starts on the configured `discovery_port`
- **AND** the heartbeat loop begins sending heartbeats to the coordinator
- **AND** the log output contains an `info` message indicating worker mode with the worker_id

#### Scenario: Worker without coordinator address
- **WHEN** the server is started with `--role worker` and `coordinator_address` is empty
- **THEN** the server prints an error indicating that `coordinator_address` is required for worker mode and exits with a non-zero exit code

### Requirement: Worker heartbeat
The system SHALL send heartbeats to the coordinator every 10 seconds via Flight RPC. Each heartbeat SHALL include the worker's id, Flight RPC address, current status, capacity (max_splits), and number of active tasks.

#### Scenario: Periodic heartbeat
- **WHEN** the worker is running
- **THEN** it sends a heartbeat to the coordinator every 10 seconds

#### Scenario: Coordinator unreachable
- **WHEN** the worker cannot reach the coordinator for a heartbeat
- **THEN** the worker logs a warning and retries on the next heartbeat interval
- **AND** the worker continues accepting and executing tasks from previously submitted assignments

### Requirement: Worker task execution
The system SHALL accept task assignments via Flight RPC. When a task is received, the TaskManager SHALL create an ExecutionContext, execute the plan fragment, and store results in an OutputBuffer. Results SHALL be served via Flight RPC `do_get` when the coordinator requests them.

#### Scenario: Task assignment and execution
- **WHEN** the coordinator sends a TaskRequest via Flight RPC containing a serialized plan fragment
- **THEN** the TaskManager deserializes the fragment, executes it, and stores the output RecordBatches in an OutputBuffer
- **AND** the task status transitions from Running to Completed

#### Scenario: Task execution failure
- **WHEN** a task fails during execution (e.g., data source error)
- **THEN** the task status transitions to Failed with an error message
- **AND** the error is reported to the coordinator when it queries task status

### Requirement: Worker shutdown
The system SHALL handle graceful shutdown of the worker on SIGINT/SIGTERM. On shutdown, the worker SHALL stop accepting new tasks, allow in-flight tasks to complete (with a timeout), and log a shutdown message.

#### Scenario: Worker Ctrl+C shutdown
- **WHEN** the worker receives SIGINT
- **THEN** the Flight RPC server stops accepting new tasks
- **AND** the server logs `"worker shutting down"` at `info` level and exits with code 0
