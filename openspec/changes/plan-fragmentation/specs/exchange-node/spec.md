## ADDED Requirements

### Requirement: PartitioningScheme enum
The system SHALL define a `PartitioningScheme` enum with the following variants:
- `Single` — gather all data to a single node
- `Hash { columns: Vec<usize> }` — hash-distribute by the specified column indices
- `RoundRobin` — distribute rows evenly across nodes in round-robin fashion
- `Broadcast` — send all data to every node

It SHALL derive `Debug`, `Clone`, `PartialEq`, `Eq` and implement `Display`.

#### Scenario: Displaying a Hash partitioning scheme
- **WHEN** `PartitioningScheme::Hash { columns: vec![0, 2] }` is formatted with `Display`
- **THEN** it produces a string containing "Hash" and the column indices (e.g., `"Hash([0, 2])"`)

#### Scenario: Displaying Single partitioning
- **WHEN** `PartitioningScheme::Single` is formatted with `Display`
- **THEN** it produces `"Single"`

### Requirement: ExchangeNode LogicalPlan variant
The system SHALL add an `ExchangeNode` variant to the `LogicalPlan` enum with fields:
- `input: Box<LogicalPlan>` — the child plan producing data
- `partitioning_scheme: PartitioningScheme` — how data is redistributed
- `schema: Vec<ColumnInfo>` — output schema (same as input schema)

The `schema()` method on `LogicalPlan` SHALL return the stored schema for `ExchangeNode`. The `Display` implementation SHALL show the exchange type (e.g., `"Exchange [Hash([0])]"`).

#### Scenario: ExchangeNode schema passthrough
- **WHEN** an `ExchangeNode` wraps an input plan with schema `[id: Int64, name: Utf8]`
- **THEN** `schema()` returns `[id: Int64, name: Utf8]`

#### Scenario: ExchangeNode in plan display
- **WHEN** a plan tree containing `ExchangeNode` with `PartitioningScheme::Single` is formatted
- **THEN** the display output includes a line showing `"Exchange [Single]"`

## MODIFIED Requirements

### Requirement: LogicalPlan enum with relational operators (MODIFIED)
The `LogicalPlan` enum SHALL include the following additional variants beyond the existing ones:
- `ExchangeNode { input: Box<LogicalPlan>, partitioning_scheme: PartitioningScheme, schema: Vec<ColumnInfo> }`
- `PartialAggregate { input: Box<LogicalPlan>, group_by: Vec<PlanExpr>, aggr_exprs: Vec<PlanExpr>, schema: Vec<ColumnInfo> }`
- `FinalAggregate { input: Box<LogicalPlan>, group_by: Vec<PlanExpr>, aggr_exprs: Vec<PlanExpr>, schema: Vec<ColumnInfo> }`

#### Scenario: Schema of PartialAggregate
- **WHEN** `schema()` is called on a `PartialAggregate` node
- **THEN** it returns the stored schema representing the partial aggregation output columns

#### Scenario: Schema of FinalAggregate
- **WHEN** `schema()` is called on a `FinalAggregate` node
- **THEN** it returns the stored schema representing the final aggregation output columns
