## ADDED Requirements

### Requirement: Proto file definition
The system SHALL define a `trino_rpc.proto` file at `crates/rpc/proto/trino_rpc.proto` containing all task management messages and service definitions. The proto file SHALL use `syntax = "proto3"` and package name `trino.rpc`.

### Requirement: TaskRequest message
The system SHALL define a `TaskRequest` protobuf message with fields: `string fragment_id`, `repeated SplitAssignment split_assignments`, `OutputPartitioning output_partitioning`. `SplitAssignment` SHALL contain `string connector_id` and `bytes split_info`. `OutputPartitioning` SHALL contain `uint32 partition_count` and `string partitioning_scheme`.

#### Scenario: Submitting a task
- **WHEN** a coordinator sends a `TaskRequest` with fragment_id "frag-1" and 2 split assignments
- **THEN** the message serializes and deserializes correctly with all fields preserved

### Requirement: TaskStatus message
The system SHALL define a `TaskStatus` protobuf message with fields: `string task_id`, `string state`, `uint64 rows_processed`, `uint64 bytes_processed`, `string error` (empty if no error).

#### Scenario: Reporting task progress
- **WHEN** a worker reports status with task_id "task-1", state "RUNNING", rows_processed 50000
- **THEN** the message contains all fields accurately

### Requirement: Heartbeat message
The system SHALL define a `Heartbeat` protobuf message with fields: `string worker_id`, `uint64 timestamp_ms`, `uint32 available_capacity` (number of additional tasks the worker can accept).

#### Scenario: Worker heartbeat
- **WHEN** a worker sends a heartbeat with worker_id "worker-1" and available_capacity 4
- **THEN** the coordinator receives the correct capacity information

### Requirement: TaskService gRPC service
The system SHALL define a `TaskService` gRPC service with RPCs: `SubmitTask(TaskRequest) returns (TaskStatus)`, `GetTaskStatus(TaskStatusRequest) returns (TaskStatus)`, `SendHeartbeat(Heartbeat) returns (HeartbeatAck)`. `TaskStatusRequest` SHALL contain `string task_id`. `HeartbeatAck` SHALL be an empty message.

### Requirement: tonic-build compilation
The system SHALL use `tonic-build` in a `build.rs` to compile the proto file into Rust code. The generated code SHALL be importable from the rpc crate.
