## ADDED Requirements

### Requirement: ExecutionPlan trait
The system SHALL define an `ExecutionPlan` trait with `schema() -> Vec<ColumnInfo>`, `execute() -> Result<Vec<RecordBatch>, ExecutionError>`, and `display_name() -> &str`. The trait SHALL require `Send + Sync + Debug` bounds for use as `Arc<dyn ExecutionPlan>`.

### Requirement: ScanExec
The system SHALL implement a `ScanExec` operator that reads all data from an `Arc<dyn DataSource>` by calling `scan()`.

#### Scenario: Scanning a data source
- **WHEN** `ScanExec` wraps a data source with 3 rows
- **THEN** `execute()` returns batches containing those 3 rows

### Requirement: ProjectionExec
The system SHALL implement a `ProjectionExec` operator that evaluates a list of `PlanExpr` against each input batch, producing new batches with the projected columns. It SHALL cast output columns to match the declared output schema types.

#### Scenario: Projecting a single column
- **WHEN** `ProjectionExec` projects column index 1 ("name") from an input with columns (id, name, value)
- **THEN** `execute()` returns batches with a single "name" column

### Requirement: FilterExec
The system SHALL implement a `FilterExec` operator that evaluates a boolean predicate expression against each input batch and filters rows using Arrow's `filter_record_batch`. Empty batches after filtering SHALL be omitted.

#### Scenario: Filtering rows
- **WHEN** `FilterExec` applies predicate `id > 1` to input rows with id `[1, 2, 3]`
- **THEN** `execute()` returns 2 rows (id=2 and id=3)

### Requirement: NestedLoopJoinExec
The system SHALL implement a `NestedLoopJoinExec` operator supporting CROSS, INNER, LEFT, RIGHT, and FULL join types. For CROSS joins, the condition is `JoinCondition::None`. For other joins, the condition is evaluated on a combined single-row batch for each (left_row, right_row) pair. Unmatched rows in outer joins SHALL have null-filled columns for the missing side.

#### Scenario: Cross join
- **WHEN** left has 2 rows and right has 3 rows with CROSS join
- **THEN** `execute()` returns 6 rows (Cartesian product)

#### Scenario: Left outer join with unmatched rows
- **WHEN** a LEFT join is performed and some left rows have no matching right rows
- **THEN** those left rows appear in the output with NULL values for all right columns

### Requirement: HashAggregateExec
The system SHALL implement a `HashAggregateExec` operator that performs hash-based grouping and aggregation. It SHALL support both grouped aggregation (with group-by expressions) and global aggregation (no group-by, producing a single output row). Aggregate expressions SHALL be `PlanExpr::Function` nodes whose names map to accumulators via `create_accumulator()`.

#### Scenario: Global COUNT and SUM
- **WHEN** `HashAggregateExec` with no group-by computes `COUNT(*)` and `SUM(value)` on 3 rows with values `[100, 200, 300]`
- **THEN** `execute()` returns 1 row: count=3, sum=600

#### Scenario: Grouped aggregation
- **WHEN** `HashAggregateExec` groups by a column and computes SUM on another
- **THEN** `execute()` returns one row per distinct group-by value, each with the correct aggregate

### Requirement: SortExec
The system SHALL implement a `SortExec` operator that concatenates all input batches and sorts them using Arrow's `lexsort_to_indices` with the specified sort expressions (direction, nulls ordering).

#### Scenario: Descending sort
- **WHEN** `SortExec` sorts by `id DESC` on rows with id `[1, 2, 3]`
- **THEN** `execute()` returns rows ordered `[3, 2, 1]`

### Requirement: LimitExec
The system SHALL implement a `LimitExec` operator that applies OFFSET (skip) and LIMIT (take) to the input. It SHALL concatenate all input batches, then slice the result. If offset exceeds total rows, it SHALL return an empty batch with the correct schema.

#### Scenario: LIMIT with OFFSET
- **WHEN** `LimitExec` applies limit=1, offset=1 to rows with id `[1, 2, 3]`
- **THEN** `execute()` returns 1 row with id=2

### Requirement: ExplainExec
The system SHALL implement an `ExplainExec` operator that formats the contained `LogicalPlan` as text and returns it as a single-row, single-column Utf8 batch with column name "plan".

#### Scenario: Explaining a plan
- **WHEN** `ExplainExec` wraps a `LogicalPlan::TableScan` for table "test"
- **THEN** `execute()` returns a batch with one row whose "plan" column contains text including "TableScan"
