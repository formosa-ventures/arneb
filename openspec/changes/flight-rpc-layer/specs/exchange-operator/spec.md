## ADDED Requirements

### Requirement: ExchangeOperator struct
The system SHALL implement an `ExchangeOperator` physical operator that reads data from remote tasks via a list of `ExchangeClient` instances. It SHALL hold `Vec<ExchangeSource>` where each `ExchangeSource` contains an `ExchangeClient`, `task_id`, and `partition_id`.

### Requirement: ExecutionPlan implementation
The system SHALL implement the `ExecutionPlan` trait for `ExchangeOperator`, providing `schema()`, `execute()`, and `display_name()` methods.

### Requirement: schema
The system SHALL implement `schema()` returning the output schema of the upstream fragment. The schema SHALL be provided at construction time.

### Requirement: execute
The system SHALL implement `execute()` that fetches data from all exchange sources and merges the resulting streams into a single output. Each exchange source is fetched concurrently using `tokio::spawn`. The merged output SHALL contain all RecordBatches from all sources.

#### Scenario: Single source exchange
- **WHEN** `ExchangeOperator` has 1 source with 3 RecordBatches
- **THEN** `execute()` returns all 3 batches

#### Scenario: Multiple source exchange
- **WHEN** `ExchangeOperator` has 3 sources with 2 batches each
- **THEN** `execute()` returns all 6 batches (order may vary across sources)

#### Scenario: Source failure
- **WHEN** one exchange source fails during fetching
- **THEN** `execute()` returns `Err` with the failure details

### Requirement: display_name
The system SHALL implement `display_name()` returning `"ExchangeOperator"`.

#### Scenario: Display name
- **WHEN** `display_name()` is called
- **THEN** `"ExchangeOperator"` is returned
