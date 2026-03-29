## ADDED Requirements

### Requirement: Async ExecutionPlan trait
The system SHALL define an `ExecutionPlan` trait with `schema() -> Vec<ColumnInfo>`, `async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError>`, and `display_name() -> &str`. The trait SHALL require `Send + Sync + Debug` bounds for use as `Arc<dyn ExecutionPlan>`. The `execute()` method SHALL use `#[async_trait]` to enable dynamic dispatch via `Arc<dyn ExecutionPlan>`.

#### Scenario: Executing an operator returns a stream
- **WHEN** `execute()` is awaited on any `Arc<dyn ExecutionPlan>`
- **THEN** it returns a `Result<SendableRecordBatchStream, ExecutionError>`
- **AND** the stream's schema matches the operator's `schema()` output

#### Scenario: Operator is Send + Sync
- **WHEN** an `Arc<dyn ExecutionPlan>` is created
- **THEN** it can be shared across threads and sent between tokio tasks

### Requirement: Async ExecutionContext
The system SHALL provide an `ExecutionContext` with `create_physical_plan()` that converts a `LogicalPlan` into an `Arc<dyn ExecutionPlan>`. The physical plan's `execute()` method SHALL be awaitable and return a `SendableRecordBatchStream`.

#### Scenario: End-to-end async query execution
- **WHEN** `ExecutionContext::create_physical_plan(logical_plan)` produces a physical plan and `plan.execute().await` is called
- **THEN** it returns a stream of `RecordBatch` results matching the query semantics
