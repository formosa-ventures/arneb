## ADDED Requirements

### Requirement: PartialAggregate LogicalPlan variant
The system SHALL add a `PartialAggregate` variant to `LogicalPlan` with fields:
- `input: Box<LogicalPlan>` — the child plan producing rows to aggregate
- `group_by: Vec<PlanExpr>` — group-by expressions
- `aggr_exprs: Vec<PlanExpr>` — aggregate function expressions (e.g., SUM, COUNT)
- `schema: Vec<ColumnInfo>` — output schema of the partial aggregation

`PartialAggregate` represents the first phase of distributed aggregation, running on each worker to produce intermediate aggregate state.

#### Scenario: PartialAggregate schema
- **WHEN** a `PartialAggregate` groups by column "region" and computes `SUM(amount)`
- **THEN** `schema()` returns columns for the group key ("region") and the partial sum

### Requirement: FinalAggregate LogicalPlan variant
The system SHALL add a `FinalAggregate` variant to `LogicalPlan` with fields:
- `input: Box<LogicalPlan>` — the child plan (typically an ExchangeNode collecting partial results)
- `group_by: Vec<PlanExpr>` — group-by expressions
- `aggr_exprs: Vec<PlanExpr>` — aggregate function expressions
- `schema: Vec<ColumnInfo>` — output schema of the final aggregation

`FinalAggregate` represents the second phase of distributed aggregation, combining partial results from all workers.

#### Scenario: FinalAggregate schema
- **WHEN** a `FinalAggregate` merges partial `SUM(amount)` grouped by "region"
- **THEN** `schema()` returns the final schema with "region" and the complete sum

### Requirement: Two-phase aggregation splitting
The `PlanFragmenter` SHALL split every `Aggregate` node into a two-phase structure:
1. `PartialAggregate` — placed in the child fragment (runs on workers alongside the data source)
2. `ExchangeNode` with `PartitioningScheme::Hash { columns }` where columns are the indices of the group-by keys, or `PartitioningScheme::Single` if there are no group-by keys (global aggregation)
3. `FinalAggregate` — placed in the parent fragment (combines partial results)

#### Scenario: Grouped aggregation split
- **WHEN** `Aggregate(TableScan("orders"), group_by=[region], aggr=[SUM(amount)])` is fragmented
- **THEN** the result contains: a SOURCE fragment with `PartialAggregate(TableScan("orders"), group_by=[region], aggr=[SUM(amount)])`, an exchange with `Hash` on the group-by column index, and the parent fragment with `FinalAggregate(ExchangeNode(...), group_by=[region], aggr=[SUM(amount)])`

#### Scenario: Global aggregation split (no group-by)
- **WHEN** `Aggregate(TableScan("orders"), group_by=[], aggr=[COUNT(*)])` is fragmented
- **THEN** the exchange uses `PartitioningScheme::Single` (gather to one node) since there are no group-by keys to hash on

#### Scenario: Display for two-phase aggregation nodes
- **WHEN** a plan tree containing `PartialAggregate` and `FinalAggregate` is formatted with `Display`
- **THEN** the output distinguishes between partial and final phases (e.g., `"PartialAggregate"` and `"FinalAggregate"`)
