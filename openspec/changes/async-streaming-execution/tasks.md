## 1. Dependencies and Stream Types (`common` crate)

- [x] 1.1 Add `futures` 0.3 and `async-trait` dependencies to `crates/common/Cargo.toml`
- [x] 1.2 Define `RecordBatchStream` trait extending `Stream<Item = Result<RecordBatch, ArnebError>> + Send + Unpin` with `schema() -> Arc<Schema>` method
- [x] 1.3 Define `SendableRecordBatchStream` type alias as `Pin<Box<dyn RecordBatchStream>>`
- [x] 1.4 Implement `stream_from_batches(schema, batches) -> SendableRecordBatchStream` adapter
- [x] 1.5 Implement `async fn collect_stream(stream) -> Result<Vec<RecordBatch>, ArnebError>` adapter
- [x] 1.6 Write tests for `stream_from_batches` (non-empty, empty) and `collect_stream` (success, error propagation)

## 2. DataSource Trait (`execution` crate)

- [x] 2.1 Add `futures` and `async-trait` dependencies to `crates/execution/Cargo.toml`
- [x] 2.2 Change `DataSource::scan()` to `async fn scan(&self) -> Result<SendableRecordBatchStream, ExecutionError>` using `#[async_trait]`
- [x] 2.3 Update `InMemoryDataSource::scan()` to return `stream_from_batches()` wrapping stored batches
- [x] 2.4 Convert DataSource tests to async (`#[tokio::test]`)

## 3. ExecutionPlan Trait

- [x] 3.1 Change `ExecutionPlan::execute()` to `async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError>` using `#[async_trait]`

## 4. Streaming Operators

- [x] 4.1 Convert `ScanExec::execute()` to async — await `DataSource::scan()` and return stream directly
- [x] 4.2 Convert `FilterExec::execute()` to async — return a stream that applies predicate to each input batch, skipping empty results
- [x] 4.3 Convert `ProjectionExec::execute()` to async — return a stream that evaluates expressions on each input batch
- [x] 4.4 Convert `LimitExec::execute()` to async — return a stream that tracks row counts, applies offset/limit, and terminates early
- [x] 4.5 Convert `ExplainExec::execute()` to async — return single-batch stream via `stream_from_batches()`

## 5. Pipeline-Breaking Operators

- [x] 5.1 Convert `SortExec::execute()` to async — collect input stream, sort, return result via `stream_from_batches()`
- [x] 5.2 Convert `HashAggregateExec::execute()` to async — collect input stream, aggregate, return result via `stream_from_batches()`
- [x] 5.3 Convert `NestedLoopJoinExec::execute()` to async — collect both input streams, join, return result via `stream_from_batches()`

## 6. ExecutionContext and Physical Planner

- [x] 6.1 Update `ExecutionContext::create_physical_plan()` — no signature change needed (returns `Arc<dyn ExecutionPlan>`)
- [x] 6.2 Verify recursive plan construction still works with the new async `ExecutionPlan` trait

## 7. Connector Updates

- [x] 7.1 Update `MemoryTable` (connectors crate) `DataSource::scan()` to async returning stream
- [x] 7.2 Update `FileDataSource` (CSV/Parquet connectors) `DataSource::scan()` to async returning stream
- [x] 7.3 Add `futures` and `async-trait` dependencies to `crates/connectors/Cargo.toml` if not already present

## 8. Protocol Handler Update

- [x] 8.1 Remove `tokio::task::spawn_blocking` from the pgwire connection handler's query execution path
- [x] 8.2 Call `plan.execute().await` directly in the async handler
- [x] 8.3 Consume `SendableRecordBatchStream` to encode result batches as PostgreSQL DataRow messages

## 9. Tests

- [x] 9.1 Convert all operator tests to async (`#[tokio::test]`), using `collect_stream()` for assertions
- [x] 9.2 Convert physical planner tests to async
- [x] 9.3 Convert end-to-end integration tests to async
- [x] 9.4 Verify streaming behavior: FilterExec processes batches independently (multi-batch input test)
- [x] 9.5 Verify LimitExec early termination (does not consume entire input stream)

## 10. Quality

- [x] 10.1 `cargo build` compiles without warnings
- [x] 10.2 `cargo test` — all tests pass
- [x] 10.3 `cargo clippy -- -D warnings` — clean
- [x] 10.4 `cargo fmt -- --check` — clean
