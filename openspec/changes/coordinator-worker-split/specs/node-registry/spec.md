## ADDED Requirements

### Requirement: NodeRegistry worker tracking
The system SHALL maintain a `NodeRegistry` that stores information about all known workers. Each worker entry SHALL include: `worker_id` (unique identifier), `address` (Flight RPC host:port), `status` (Active, Draining, Dead), `capacity` (max concurrent splits), `active_tasks` (current running task count), and `last_heartbeat` (timestamp of last heartbeat). The `NodeRegistry` SHALL be thread-safe (`Arc<RwLock>`).

#### Scenario: Register a new worker
- **WHEN** a heartbeat is received from a previously unknown worker_id
- **THEN** a new WorkerInfo entry is created with status Active and the provided capacity
- **AND** last_heartbeat is set to the current time

#### Scenario: Update existing worker
- **WHEN** a heartbeat is received from a known worker_id
- **THEN** the WorkerInfo entry is updated with the latest active_tasks count and last_heartbeat timestamp

#### Scenario: Concurrent access
- **WHEN** multiple goroutines read the registry (for scheduling) while a heartbeat update writes to it
- **THEN** reads and writes are correctly serialized via RwLock without data corruption

### Requirement: Heartbeat-based health monitoring
The system SHALL run a background task on the coordinator that checks for stale workers every 15 seconds. If no heartbeat has been received from a worker for 30 seconds, the worker's status SHALL be set to Dead. Dead workers SHALL NOT be assigned new tasks.

#### Scenario: Worker goes stale
- **WHEN** a worker's last_heartbeat is more than 30 seconds ago
- **THEN** the background task sets the worker's status to Dead
- **AND** the worker is excluded from scheduling decisions

#### Scenario: Worker recovers
- **WHEN** a Dead worker sends a new heartbeat
- **THEN** its status transitions back to Active and it becomes eligible for task assignment again

### Requirement: Active workers query
The system SHALL provide a method `active_workers() -> Vec<WorkerInfo>` that returns all workers with status Active, sorted by worker_id for deterministic ordering.

#### Scenario: Mixed worker statuses
- **WHEN** the registry contains 3 workers: one Active, one Draining, one Dead
- **THEN** `active_workers()` returns only the Active worker

#### Scenario: No active workers
- **WHEN** all workers are Dead or the registry is empty
- **THEN** `active_workers()` returns an empty vector

### Requirement: Worker removal
The system SHALL provide a method to remove a worker from the registry. This is used when a worker explicitly deregisters during graceful shutdown.

#### Scenario: Worker deregistration
- **WHEN** a worker sends a deregistration message during shutdown
- **THEN** the worker is removed from the NodeRegistry
- **AND** subsequent `active_workers()` calls do not include this worker
