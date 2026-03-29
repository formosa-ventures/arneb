## Why

Distributed query execution requires high-performance inter-node data exchange. Workers must send intermediate results to other workers (shuffle) and back to the coordinator (final results). Arrow Flight SQL provides zero-copy columnar data transfer over gRPC, achieving 20-100x speedup over traditional serialization approaches. This is the communication backbone of the distributed engine.

## What Changes

- Create new `crates/rpc/` crate (trino-rpc)
- Implement Arrow Flight SQL server using tonic + arrow-flight for RecordBatch streaming
- Define protobuf messages for task management (TaskRequest, TaskStatus, Heartbeat)
- Implement OutputBuffer: bounded, partition-aware buffer for task output
- Implement ExchangeClient: async client fetching data from remote workers via Flight
- Implement ExchangeOperator: physical operator replacing ExchangeNode, uses ExchangeClient

## Capabilities

### New Capabilities
- `flight-server`: Arrow Flight SQL server serving RecordBatch streams between nodes
- `task-protocol`: Protobuf message definitions for task submission, status, and heartbeat
- `output-buffer`: Bounded partition-aware buffer for task output with async read/write
- `exchange-client`: Async Flight client fetching remote data as SendableRecordBatchStream
- `exchange-operator`: Physical operator wrapping ExchangeClient

### Modified Capabilities
- `execution-operators`: Add ExchangeOperator to operator set

## Impact

- **Crates**: rpc (new crate)
- **New crate**: crates/rpc/ with Cargo.toml
- **Dependencies**: tonic 0.12+, prost 0.13+, arrow-flight 54, tonic-build (build dep)
