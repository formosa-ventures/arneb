## ADDED Requirements

### Requirement: ExchangeExec reads remote task output
The system SHALL provide an `ExchangeExec` physical operator that reads from a remote worker's OutputBuffer via `ExchangeClient::fetch_partition()`. It SHALL implement the `ExecutionPlan` trait and return a `SendableRecordBatchStream` of remote data.

#### Scenario: Read single partition from remote worker
- **WHEN** ExchangeExec is configured with a remote worker address and task_id
- **THEN** it connects via Flight `do_get`, fetches RecordBatches, and returns them as a stream

#### Scenario: ExchangeNode mapped to ExchangeExec
- **WHEN** ExecutionContext encounters a `LogicalPlan::ExchangeNode` with a remote source address
- **THEN** it creates an ExchangeExec operator instead of a local scan
