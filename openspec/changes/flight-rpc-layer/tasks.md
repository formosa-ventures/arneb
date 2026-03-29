## 1. RPC Crate Setup

- [x] 1.1 Create crates/rpc/Cargo.toml with deps: tonic, prost, arrow-flight, arrow, tokio, common
- [x] 1.2 Add rpc to workspace Cargo.toml members
- [x] 1.3 Create crates/rpc/proto/trino_rpc.proto with task messages
- [x] 1.4 Create build.rs with tonic_build for proto compilation
- [x] 1.5 Create crates/rpc/src/lib.rs with module declarations

## 2. Protobuf Definitions

- [x] 2.1 Define TaskRequest message (fragment_id, split_assignments, output_partitioning)
- [x] 2.2 Define TaskStatus message (task_id, state, rows_processed, bytes_processed, error)
- [x] 2.3 Define Heartbeat message (worker_id, timestamp, available_capacity)
- [x] 2.4 Define TaskService gRPC service (SubmitTask, GetTaskStatus, SendHeartbeat)
- [x] 2.5 Generate Rust code and verify compilation

## 3. OutputBuffer

- [x] 3.1 Implement OutputBuffer with per-partition mpsc channels
- [x] 3.2 Implement write_batch(partition_id, batch) method
- [x] 3.3 Implement read_stream(partition_id) returning RecordBatch stream
- [x] 3.4 Implement finish() to signal no more data for a partition
- [x] 3.5 Write tests for single and multi-partition scenarios

## 4. Arrow Flight Server

- [x] 4.1 Implement FlightService trait for data exchange
- [x] 4.2 Implement do_get: decode ticket → find OutputBuffer → stream RecordBatches
- [x] 4.3 Implement get_flight_info: return schema and endpoint info
- [x] 4.4 Start Flight server on configurable port
- [x] 4.5 Write tests with in-process Flight client/server

## 5. Task Management Service

- [x] 5.1 Implement TaskService gRPC handlers (submit, status, heartbeat)
- [x] 5.2 Wire task service into the same tonic server as Flight
- [x] 5.3 Write tests for task submission and status reporting

## 6. ExchangeClient

- [x] 6.1 Implement ExchangeClient connecting to remote Flight server
- [x] 6.2 Implement fetch_partition(task_id, partition_id) → SendableRecordBatchStream
- [x] 6.3 Handle connection errors and retries (basic: fail fast for now)
- [x] 6.4 Write tests with mock Flight server

## 7. ExchangeOperator

- [x] 7.1 Implement ExchangeOperator holding Vec<ExchangeClient>
- [x] 7.2 Implement execute() merging all client streams into single output
- [x] 7.3 Implement schema() from upstream fragment metadata
- [x] 7.4 Write tests for ExchangeOperator with mock data

## 8. Integration Tests

- [x] 8.1 End-to-end test: two processes exchanging RecordBatches via Flight
- [x] 8.2 Test OutputBuffer → Flight Server → ExchangeClient → ExchangeOperator pipeline
- [x] 8.3 Verify all existing tests pass
