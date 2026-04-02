## ADDED Requirements

### Requirement: ExchangeClient struct
The system SHALL implement an `ExchangeClient` struct that connects to a remote Arrow Flight server and fetches RecordBatch streams. It SHALL be constructed with a `target_address: String` (e.g., "http://worker-1:8815").

### Requirement: fetch_partition
The system SHALL implement `async fetch_partition(task_id: &str, partition_id: u32) -> Result<SendableRecordBatchStream>` that creates a `Ticket` with the serialized `OutputRequest`, calls `do_get` on the remote Flight server, and wraps the `FlightRecordBatchStream` into a `SendableRecordBatchStream`.

#### Scenario: Fetching a remote partition
- **WHEN** `fetch_partition("task-1", 0)` is called
- **AND** the remote server has data for task-1 partition 0
- **THEN** a stream of RecordBatches is returned

#### Scenario: Remote server unavailable
- **WHEN** `fetch_partition("task-1", 0)` is called
- **AND** the remote server is not reachable
- **THEN** `Err` is returned with a connection error

### Requirement: Lazy connection
The system SHALL establish the gRPC connection lazily on the first `fetch_partition` call, not at construction time. The connection SHALL be reused for subsequent calls.

#### Scenario: Connection reuse
- **WHEN** `fetch_partition` is called twice for different partitions on the same server
- **THEN** the same underlying gRPC connection is used

### Requirement: Error handling
The system SHALL map gRPC errors to appropriate `ArnebError` variants. Connection failures SHALL be reported as `ArnebError::Internal` with the original error message preserved.

#### Scenario: gRPC NOT_FOUND error
- **WHEN** the remote server returns NOT_FOUND for an unknown task
- **THEN** the error is mapped to `ArnebError::Internal` with a descriptive message
