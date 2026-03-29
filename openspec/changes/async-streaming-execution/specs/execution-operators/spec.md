## MODIFIED Requirements

### Requirement: ExecutionPlan trait
The system SHALL define an `ExecutionPlan` trait with `schema() -> Vec<ColumnInfo>`, `async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError>`, and `display_name() -> &str`. The trait SHALL require `Send + Sync + Debug` bounds for use as `Arc<dyn ExecutionPlan>`. The `execute()` method SHALL be async (via `#[async_trait]`) and return a `SendableRecordBatchStream` instead of `Vec<RecordBatch>`.

#### Scenario: Executing an operator returns a stream
- **WHEN** `execute().await` is called on any `Arc<dyn ExecutionPlan>`
- **THEN** it returns `Result<SendableRecordBatchStream, ExecutionError>`
- **AND** collecting the stream yields the same batches as the previous sync `execute()` would have returned

### Requirement: ScanExec
The system SHALL implement a `ScanExec` operator that calls `DataSource::scan().await` and returns the resulting stream.

#### Scenario: Scanning a data source
- **WHEN** `ScanExec` wraps a data source with 3 rows
- **THEN** `execute().await` returns a stream whose collected batches contain those 3 rows

### Requirement: ProjectionExec
The system SHALL implement a `ProjectionExec` operator that evaluates a list of `PlanExpr` against each input batch from the child stream, producing new batches with the projected columns. It SHALL cast output columns to match the declared output schema types.

#### Scenario: Projecting a single column
- **WHEN** `ProjectionExec` projects column index 1 ("name") from an input with columns (id, name, value)
- **THEN** `execute().await` returns a stream whose collected batches have a single "name" column

### Requirement: FilterExec
The system SHALL implement a `FilterExec` operator that evaluates a boolean predicate expression against each batch from the child stream and filters rows using Arrow's `filter_record_batch`. Empty batches after filtering SHALL be omitted from the output stream.

#### Scenario: Filtering rows
- **WHEN** `FilterExec` applies predicate `id > 1` to input rows with id `[1, 2, 3]`
- **THEN** `execute().await` returns a stream whose collected batches contain 2 rows (id=2 and id=3)

### Requirement: NestedLoopJoinExec
The system SHALL implement a `NestedLoopJoinExec` operator supporting CROSS, INNER, LEFT, RIGHT, and FULL join types. The operator SHALL collect all input from both child streams before performing the join. Unmatched rows in outer joins SHALL have null-filled columns for the missing side.

#### Scenario: Cross join
- **WHEN** left has 2 rows and right has 3 rows with CROSS join
- **THEN** `execute().await` returns a stream whose collected batches contain 6 rows (Cartesian product)

#### Scenario: Left outer join with unmatched rows
- **WHEN** a LEFT join is performed and some left rows have no matching right rows
- **THEN** those left rows appear in the output with NULL values for all right columns

### Requirement: HashAggregateExec
The system SHALL implement a `HashAggregateExec` operator that collects all input from the child stream, performs hash-based grouping and aggregation, and returns the result as a stream. It SHALL support both grouped aggregation and global aggregation.

#### Scenario: Global COUNT and SUM
- **WHEN** `HashAggregateExec` with no group-by computes `COUNT(*)` and `SUM(value)` on 3 rows with values `[100, 200, 300]`
- **THEN** `execute().await` returns a stream whose collected batches contain 1 row: count=3, sum=600

#### Scenario: Grouped aggregation
- **WHEN** `HashAggregateExec` groups by a column and computes SUM on another
- **THEN** `execute().await` returns a stream with one row per distinct group-by value

### Requirement: SortExec
The system SHALL implement a `SortExec` operator that collects all input from the child stream, concatenates and sorts using Arrow's `lexsort_to_indices`, and returns the sorted result as a stream.

#### Scenario: Descending sort
- **WHEN** `SortExec` sorts by `id DESC` on rows with id `[1, 2, 3]`
- **THEN** `execute().await` returns a stream whose collected batches contain rows ordered `[3, 2, 1]`

### Requirement: LimitExec
The system SHALL implement a `LimitExec` operator that applies OFFSET and LIMIT to the child stream. It SHALL track cumulative row counts and terminate the output stream early when the limit is reached.

#### Scenario: LIMIT with OFFSET
- **WHEN** `LimitExec` applies limit=1, offset=1 to rows with id `[1, 2, 3]`
- **THEN** `execute().await` returns a stream whose collected batches contain 1 row with id=2

### Requirement: ExplainExec
The system SHALL implement an `ExplainExec` operator that formats the contained `LogicalPlan` as text and returns it as a single-batch stream with column name "plan".

#### Scenario: Explaining a plan
- **WHEN** `ExplainExec` wraps a `LogicalPlan::TableScan` for table "test"
- **THEN** `execute().await` returns a stream whose single batch has a "plan" column containing text including "TableScan"
