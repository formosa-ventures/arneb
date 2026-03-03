## ADDED Requirements

### Requirement: LogicalPlan enum with relational operators
The system SHALL define a `LogicalPlan` enum with the following variants representing relational algebra operations:
- `TableScan { table: TableReference, schema: Vec<ColumnInfo>, alias: Option<String> }` — reads all rows from a table
- `Projection { input: Box<LogicalPlan>, exprs: Vec<PlanExpr>, schema: Vec<ColumnInfo> }` — selects/computes columns
- `Filter { input: Box<LogicalPlan>, predicate: PlanExpr }` — filters rows by a boolean expression
- `Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, join_type: JoinType, condition: JoinCondition }` — combines two inputs
- `Aggregate { input: Box<LogicalPlan>, group_by: Vec<PlanExpr>, aggr_exprs: Vec<PlanExpr>, schema: Vec<ColumnInfo> }` — groups and aggregates
- `Sort { input: Box<LogicalPlan>, order_by: Vec<SortExpr> }` — orders rows
- `Limit { input: Box<LogicalPlan>, limit: Option<usize>, offset: Option<usize> }` — limits result count
- `Explain { input: Box<LogicalPlan> }` — wraps a plan for EXPLAIN output

#### Scenario: Building a simple SELECT plan
- **WHEN** `SELECT a, b FROM users WHERE a > 1` is planned
- **THEN** the result is `Projection(Filter(TableScan("users"), a > 1), [a, b])`

#### Scenario: Building a JOIN plan
- **WHEN** `SELECT * FROM t1 JOIN t2 ON t1.id = t2.id` is planned
- **THEN** the result is `Projection(Join(TableScan(t1), TableScan(t2), Inner, ON ...), [*])`

### Requirement: Output schema on plan nodes
Every `LogicalPlan` node SHALL provide a `schema() -> Vec<ColumnInfo>` method that returns the output column schema of that node.

#### Scenario: TableScan schema
- **WHEN** `schema()` is called on a `TableScan` for a table with columns (id: Int64, name: Utf8)
- **THEN** it returns those two `ColumnInfo` entries

#### Scenario: Projection schema
- **WHEN** `schema()` is called on a `Projection` selecting column "name"
- **THEN** it returns a single `ColumnInfo` for "name"

### Requirement: SortExpr type
The system SHALL define a `SortExpr` struct with fields: `expr: PlanExpr`, `asc: bool`, `nulls_first: bool`.

#### Scenario: ORDER BY a DESC NULLS LAST
- **WHEN** `ORDER BY a DESC NULLS LAST` is planned
- **THEN** the SortExpr has `asc: false, nulls_first: false`

### Requirement: Debug and Display for LogicalPlan
The `LogicalPlan` enum SHALL implement `Debug` and `Display`. The `Display` output SHALL show a human-readable plan tree for EXPLAIN output.

#### Scenario: EXPLAIN output
- **WHEN** a plan tree is formatted with `Display`
- **THEN** it shows indented operator names with key info (e.g., table name, filter expression)
