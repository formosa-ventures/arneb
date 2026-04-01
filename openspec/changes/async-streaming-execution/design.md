## Context

arneb Phase 1 is complete with synchronous execution: `ExecutionPlan::execute()` returns `Result<Vec<RecordBatch>>` and the protocol layer bridges to async pgwire via `tokio::task::spawn_blocking`. This works for small datasets but materializes entire result sets in memory before any data reaches the client. Phase 2 requires async streaming for distributed query execution (exchange operators, inter-node communication). This change introduces the streaming foundation.

Current operator categories:
- **Streaming** (can process batch-at-a-time): ScanExec, FilterExec, ProjectionExec, LimitExec
- **Pipeline breakers** (must consume all input first): SortExec, HashAggregateExec, NestedLoopJoinExec
- **Metadata-only** (no data input): ExplainExec

The `futures` crate is already a transitive dependency via pgwire/tokio. The `async-trait` crate is not needed — Rust 1.75+ supports `async fn` in traits natively (with `-> impl Future` desugaring), but since `ExecutionPlan` is used as `dyn ExecutionPlan`, we use `async-trait` or manual boxing. We will use `async-trait` for ergonomics.

## Goals / Non-Goals

**Goals:**

- Convert `ExecutionPlan::execute()` and `DataSource::scan()` to async, returning `SendableRecordBatchStream`
- Define `RecordBatchStream` trait and `SendableRecordBatchStream` type in `common` for cross-crate sharing
- Implement streaming (batch-at-a-time) execution for Filter, Projection, Limit, and Scan operators
- Collect-then-stream for Sort, Aggregate, and Join operators (behavioral parity with Phase 1)
- Remove `spawn_blocking` from the protocol handler
- Provide `collect_stream()` utility for tests and pipeline breakers
- All existing tests continue to pass (converted to async)

**Non-Goals:**

- No parallel execution within a single operator (single-threaded per query)
- No spill-to-disk for pipeline breakers (still fully in-memory)
- No backpressure or flow control between operators (consumer pulls at its own pace)
- No cancellation propagation (query cancel support is a separate change)
- No streaming Sort or Aggregate (would require fundamentally different algorithms)
- No changes to the LogicalPlan or QueryPlanner — only the physical execution layer changes

## Decisions

### D1: SendableRecordBatchStream type definition

**Choice**: Define in `common` crate:
```rust
pub trait RecordBatchStream: Stream<Item = Result<RecordBatch, ArnebError>> + Send + Unpin {
    fn schema(&self) -> Arc<arrow::datatypes::Schema>;
}

pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream>>;
```

**Rationale**: Placing the stream type in `common` (not `execution`) allows both `execution` and `protocol` to depend on it without circular dependencies. The `RecordBatchStream` trait extends `Stream` with a `schema()` method so consumers can access the output schema without polling. `Send` bound enables use across tokio task boundaries. `Unpin` simplifies poll implementations.

**Alternative**: Use a bare `Pin<Box<dyn Stream<Item = ...> + Send>>` without the trait. Rejected because consumers need schema access without polling, and a named trait provides better documentation.

### D2: async-trait for ExecutionPlan and DataSource

**Choice**: Use the `async-trait` crate for `ExecutionPlan::execute()` and `DataSource::scan()`.

**Rationale**: Native async fn in traits (Rust 1.75+) does not support `dyn Trait` dispatch — it returns `impl Future` which is not object-safe. `async-trait` desugars to `Pin<Box<dyn Future>>` which works with `Arc<dyn ExecutionPlan>`. The performance cost of the extra heap allocation per `execute()` call is negligible since it happens once per operator, not per batch.

**Alternative**: Manual `fn execute(&self) -> Pin<Box<dyn Future<...> + Send + '_>>`. More boilerplate for every operator with no practical benefit. `async-trait` is the standard approach.

### D3: Streaming operators use stream combinators

**Choice**: Streaming operators (Filter, Projection, Limit) wrap the child's output stream using `futures::stream` combinators or custom `Stream` implementations.

- **FilterExec**: Maps over input stream, applies predicate to each batch, filters out empty batches
- **ProjectionExec**: Maps over input stream, evaluates expressions on each batch
- **LimitExec**: Takes from input stream, tracking row counts, terminates early when limit reached
- **ScanExec**: Calls `DataSource::scan()` which already returns a stream

**Rationale**: Batch-at-a-time processing avoids materializing the full result set. Each batch flows through the pipeline independently, reducing peak memory to one batch per operator level.

**Alternative**: Collect all input then process (current behavior). Rejected because it defeats the purpose of streaming.

### D4: Pipeline breakers collect then emit

**Choice**: SortExec, HashAggregateExec, and NestedLoopJoinExec call `collect_stream()` on their input(s), perform their operation on the materialized data, then wrap the result in a stream via `futures::stream::iter()`.

**Rationale**: These operators fundamentally require all input before producing output (sort needs all rows to determine order, aggregate needs all rows for final values, nested-loop join needs the full right side for each left row). The streaming wrapper around the output maintains the uniform `SendableRecordBatchStream` interface.

**Alternative**: Implement true streaming variants (e.g., streaming sort with merge, streaming hash aggregate with partial results). Deferred to a future optimization change — these would require different algorithms and significantly more complexity.

### D5: Adapter utilities in common

**Choice**: Provide two adapter functions:
- `collect_stream(stream) -> Result<Vec<RecordBatch>>`: Materializes a stream into a vector (for pipeline breakers and tests)
- `stream_from_batches(schema, batches) -> SendableRecordBatchStream`: Wraps a `Vec<RecordBatch>` into a stream (for InMemoryDataSource and pipeline breaker output)

**Rationale**: These are the two conversion points needed throughout the codebase. Centralizing them in `common` avoids duplication and ensures consistent error handling.

### D6: Protocol handler direct async execution

**Choice**: The pgwire connection handler calls `execution_context.create_physical_plan()` and then `plan.execute()` directly in the async context. Result batches are consumed from the stream and encoded into PostgreSQL DataRow messages one batch at a time.

**Rationale**: With async execution, there is no need for `spawn_blocking`. Direct async execution is simpler, avoids thread pool overhead, and enables future streaming of results to the client as batches arrive (rather than buffering all rows before sending).

**Trade-off**: Long-running synchronous computation within a streaming operator (e.g., expression evaluation on a large batch) will block the tokio thread. This is acceptable for Phase 2 scope — CPU-intensive work can be moved to `spawn_blocking` at a more granular level if profiling shows it is needed.

## Risks / Trade-offs

**[Blocking tokio runtime]** → Synchronous computation inside `Stream::poll_next()` (expression evaluation, Arrow compute kernels) blocks the tokio worker thread. **Mitigation**: Batches are typically small enough that per-batch computation completes quickly. If profiling reveals issues, individual operators can use `spawn_blocking` internally for heavy computation.

**[Breaking API change]** → All `DataSource` and `ExecutionPlan` implementors must update. **Mitigation**: There are a small number of implementors (8 operators, 3 DataSource implementations, 1 protocol handler). The change is mechanical — add async, wrap return values in streams.

**[Test complexity]** → All operator tests become async and need stream collection. **Mitigation**: The `collect_stream()` utility keeps test assertions nearly identical to current form. Only the setup (`#[tokio::test]`, `.await`) changes.

**[Pipeline breaker memory]** → Sort, Aggregate, and Join still materialize full input. **Mitigation**: This is unchanged from Phase 1 behavior. Spill-to-disk is a future optimization. The streaming interface makes it possible to add without further API changes.
