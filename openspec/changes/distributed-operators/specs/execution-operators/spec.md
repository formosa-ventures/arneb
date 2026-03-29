## MODIFIED Requirements

### Requirement: ShuffleWriteOperator added to execution operators
The system SHALL add `ShuffleWriteOperator` as a new `ExecutionPlan` implementation in the execution crate. It SHALL hash-partition input rows by specified columns and write them to an `OutputBuffer` with the corresponding partition index. It returns empty output from `execute()`.

#### Scenario: ShuffleWriteOperator display name
- **WHEN** `display_name()` is called on a `ShuffleWriteOperator`
- **THEN** it returns `"ShuffleWrite"`

#### Scenario: ShuffleWriteOperator schema
- **WHEN** `schema()` is called on a `ShuffleWriteOperator`
- **THEN** it returns an empty schema (no output columns)

### Requirement: BroadcastOperator added to execution operators
The system SHALL add `BroadcastOperator` as a new `ExecutionPlan` implementation in the execution crate. It SHALL replicate all input batches to every output buffer partition. It returns empty output from `execute()`.

#### Scenario: BroadcastOperator display name
- **WHEN** `display_name()` is called on a `BroadcastOperator`
- **THEN** it returns `"Broadcast"`

### Requirement: MergeOperator added to execution operators
The system SHALL add `MergeOperator` as a new `ExecutionPlan` implementation in the execution crate. It SHALL perform K-way sorted merge of multiple input streams. Unlike ShuffleWrite and Broadcast, it produces output batches.

#### Scenario: MergeOperator display name
- **WHEN** `display_name()` is called on a `MergeOperator`
- **THEN** it returns `"Merge"`

#### Scenario: MergeOperator schema
- **WHEN** `schema()` is called on a `MergeOperator`
- **THEN** it returns the schema of the input streams (all streams must have the same schema)

### Requirement: PartialHashAggregateExec added to execution operators
The system SHALL add `PartialHashAggregateExec` as a new `ExecutionPlan` implementation. It performs partial aggregation producing intermediate aggregate states.

#### Scenario: PartialHashAggregateExec display name
- **WHEN** `display_name()` is called on a `PartialHashAggregateExec`
- **THEN** it returns `"PartialHashAggregate"`

### Requirement: FinalHashAggregateExec added to execution operators
The system SHALL add `FinalHashAggregateExec` as a new `ExecutionPlan` implementation. It combines partial aggregate results into final values.

#### Scenario: FinalHashAggregateExec display name
- **WHEN** `display_name()` is called on a `FinalHashAggregateExec`
- **THEN** it returns `"FinalHashAggregate"`
