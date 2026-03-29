## Why

The execution engine currently materializes all query results in memory (`Vec<RecordBatch>`) before returning them to the caller. This blocks the protocol layer from streaming results to clients as they are produced, forces unnecessary memory consumption for large result sets, and requires a `spawn_blocking` bridge in the async protocol handler. Converting to async streaming execution enables batch-at-a-time result delivery, reduces peak memory usage, and aligns the execution layer with the async runtime used by the protocol server — a prerequisite for Phase 2 distributed execution with inter-node data exchange.

## What Changes

- **BREAKING**: `ExecutionPlan::execute()` signature changes from `fn execute(&self) -> Result<Vec<RecordBatch>, ExecutionError>` to `async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError>`
- **BREAKING**: `DataSource::scan()` signature changes from `fn scan(&self) -> Result<Vec<RecordBatch>, ExecutionError>` to `async fn scan(&self) -> Result<SendableRecordBatchStream, ExecutionError>`
- Define `SendableRecordBatchStream` type alias and `RecordBatchStream` trait in the `common` crate as a shared streaming abstraction
- Provide adapter utilities to convert between `Vec<RecordBatch>` and streams
- Convert all 8 physical operators (Scan, Filter, Projection, NestedLoopJoin, HashAggregate, Sort, Limit, Explain) to produce `SendableRecordBatchStream` output
- Streaming operators (Filter, Projection, Limit, Scan) process input batch-at-a-time without full materialization
- Pipeline-breaking operators (Sort, Aggregate, Join) collect all input then stream output batches
- Protocol handler removes `spawn_blocking` bridge and calls async execution directly
- Add `futures` 0.3 dependency to `common` and `execution` crates

## Capabilities

### New Capabilities

- `record-batch-stream`: `RecordBatchStream` trait and `SendableRecordBatchStream` type alias for streaming Arrow RecordBatch results. Adapter utilities for converting between streams and materialized vectors.
- `async-execution-plan`: Async `ExecutionPlan` trait that returns `SendableRecordBatchStream` from `execute()`. Defines the async contract for all physical operators.
- `streaming-operators`: All 8 physical operators converted to async streaming. Streaming operators (Filter, Projection, Limit, Scan) apply transformations batch-at-a-time. Pipeline breakers (Sort, Aggregate, Join) collect then stream. Explain produces a single-batch stream.

### Modified Capabilities

- `execution-operators`: Operator signatures change from sync `Vec<RecordBatch>` return to async `SendableRecordBatchStream` return. All operator behavior is preserved; only the execution model changes.
- `datasource`: `DataSource::scan()` becomes async and returns `SendableRecordBatchStream` instead of `Vec<RecordBatch>`. `InMemoryDataSource` adapts its stored batches into a stream.
- `pg-server`: Connection handler removes `tokio::task::spawn_blocking` and invokes execution directly in the async context. Result batches are streamed to the client as they arrive from the execution pipeline.

## Impact

- **Crates modified**: `common` (new stream types), `execution` (all operators, DataSource trait, ExecutionContext), `protocol` (remove spawn_blocking), `connectors` (update DataSource implementations)
- **Breaking API changes**: `ExecutionPlan::execute()` and `DataSource::scan()` signatures change. All downstream implementors must update.
- **New dependency**: `futures` 0.3 added to `common` and `execution` crates (already available in workspace via pgwire transitive dependency)
- **Test impact**: All operator tests become async (`#[tokio::test]`). Stream collection helpers needed for assertions.
- **Connector impact**: `MemoryTable`, `FileDataSource` (CSV/Parquet) must update their `DataSource::scan()` implementations to return streams
