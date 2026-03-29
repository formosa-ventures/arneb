## Context

After plan fragmentation (Change 5), queries are split into fragments that execute on different nodes. These fragments need to exchange data. Arrow Flight SQL is the ideal transport — it sends Arrow RecordBatches over gRPC with zero serialization overhead.

## Goals / Non-Goals

**Goals:**
- Flight server that streams RecordBatches from output buffers
- Protobuf definitions for task lifecycle messages
- OutputBuffer with bounded memory, partition support, and async notification
- ExchangeClient for consuming remote task output
- ExchangeOperator as a physical plan node

**Non-Goals:**
- Full Flight SQL query execution (we use Flight for data transfer only, not SQL submission)
- TLS/authentication for inter-node communication — deferred
- Flow control / backpressure beyond buffer bounds

## Decisions

1. **Separate Flight services**: One for data exchange (do_get/do_put for RecordBatches) and one for task management (custom gRPC service for submit/status/heartbeat). Both share the same tonic server.

2. **OutputBuffer design**: `Vec<tokio::sync::mpsc::Sender<RecordBatch>>` per partition. Writers select partition by index. Readers receive via the corresponding mpsc::Receiver. Bounded by channel capacity (configurable, default 32 batches).

3. **Ticket format for Flight**: Ticket contains serialized `OutputRequest { task_id: String, partition_id: u32 }`. Server looks up the task's OutputBuffer and streams from the requested partition.

4. **ExchangeClient**: Connects to remote Flight server, calls do_get with ticket, wraps the FlightRecordBatchStream into SendableRecordBatchStream.

5. **ExchangeOperator**: Holds a list of ExchangeClient instances (one per upstream task partition). Merges their streams into a single output stream. Schema comes from the upstream fragment's output schema.

6. **Proto file location**: `crates/rpc/proto/trino_rpc.proto` compiled via tonic-build in build.rs.

## Risks / Trade-offs

- **gRPC overhead for local exchange**: When coordinator and worker are on the same node, gRPC adds unnecessary overhead. Could optimize with in-process channels later.
- **Buffer memory**: Each task's OutputBuffer consumes memory. Need monitoring and limits.
