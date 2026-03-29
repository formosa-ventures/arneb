## ADDED Requirements

### Requirement: FlightService implementation
The system SHALL implement the Arrow Flight `FlightService` trait using `tonic` and `arrow-flight`. The service SHALL handle `do_get` requests for streaming `RecordBatch` data from output buffers.

### Requirement: do_get handler
The system SHALL implement `do_get` that decodes a `Ticket` containing a serialized `OutputRequest { task_id: String, partition_id: u32 }`, looks up the corresponding `OutputBuffer`, and streams `RecordBatch`es from the requested partition.

#### Scenario: Streaming data from a task partition
- **WHEN** a client calls `do_get` with a ticket for task "task-1", partition 0
- **AND** the OutputBuffer for "task-1" has 3 RecordBatches in partition 0
- **THEN** the server streams all 3 RecordBatches to the client

#### Scenario: Unknown task ID
- **WHEN** a client calls `do_get` with a ticket for an unknown task ID
- **THEN** the server returns a gRPC `NOT_FOUND` error

### Requirement: get_flight_info handler
The system SHALL implement `get_flight_info` returning the output schema and endpoint information for a given task partition. The endpoint SHALL contain the server's own address.

#### Scenario: Getting flight info
- **WHEN** a client calls `get_flight_info` for a known task
- **THEN** the response contains the Arrow schema and at least one endpoint

### Requirement: Configurable server port
The system SHALL start the Flight server on a configurable port (separate from the PostgreSQL wire protocol port). The default SHALL be 8815.

#### Scenario: Starting on custom port
- **WHEN** the Flight server is configured with port 9000
- **THEN** the server listens on port 9000

### Requirement: Shared state with task registry
The system SHALL accept an `Arc<TaskRegistry>` (or equivalent shared state) at construction time, allowing the Flight service to look up OutputBuffers by task ID.
