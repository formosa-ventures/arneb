## ADDED Requirements

### Requirement: ExecutionContext struct
The system SHALL define an `ExecutionContext` struct with a `HashMap<String, Arc<dyn DataSource>>` for registered data sources. It SHALL provide `register_data_source(name, source)` and `create_physical_plan(logical_plan)` methods.

#### Scenario: Registering a data source
- **WHEN** `ctx.register_data_source("users", source)` is called
- **THEN** the source is available for subsequent `create_physical_plan()` calls that reference table "users"

### Requirement: LogicalPlan to ExecutionPlan conversion
The `create_physical_plan()` method SHALL recursively convert a `LogicalPlan` tree into an `Arc<dyn ExecutionPlan>` tree. Each `LogicalPlan` variant maps to its corresponding operator:

| LogicalPlan | ExecutionPlan |
|-------------|---------------|
| TableScan | ScanExec |
| Projection | ProjectionExec |
| Filter | FilterExec |
| Join | NestedLoopJoinExec |
| Aggregate | HashAggregateExec |
| Sort | SortExec |
| Limit | LimitExec |
| Explain | ExplainExec |

#### Scenario: Planning a table scan
- **WHEN** `create_physical_plan(&LogicalPlan::TableScan { table: "users", .. })` is called and "users" is registered
- **THEN** it returns `Ok(Arc<ScanExec>)` wrapping the registered data source

#### Scenario: Table not found
- **WHEN** `create_physical_plan(&LogicalPlan::TableScan { table: "nonexistent", .. })` is called and "nonexistent" is not registered
- **THEN** it returns `Err(ExecutionError::InvalidOperation("data source not found for table 'nonexistent'"))`

### Requirement: Table name resolution
The physical planner SHALL look up data sources by the full table reference string first (e.g., "catalog.schema.table"), then fall back to the bare table name. This allows registration by either simple name or fully-qualified name.

#### Scenario: Simple name lookup
- **WHEN** a data source is registered as "users" and the plan references `TableReference::table("users")`
- **THEN** the lookup succeeds via the bare table name

### Requirement: End-to-end pipeline
The system SHALL support the full pipeline: construct `ExecutionContext` → register data sources → `create_physical_plan(logical_plan)` → `plan.execute()` → get `Vec<RecordBatch>` results.

#### Scenario: SELECT name FROM users WHERE id > 2 LIMIT 2
- **WHEN** a LogicalPlan representing `SELECT name FROM users WHERE id > 2 LIMIT 2` is planned and executed against a "users" table with 5 rows (id 1-5)
- **THEN** it returns 2 rows: "carol" and "dave"
