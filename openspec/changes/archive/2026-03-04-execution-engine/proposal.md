## Why

After parsing SQL into an AST (Change 2), resolving metadata via the catalog (Change 3), and building a logical plan (Change 4), the system still cannot produce actual query results. The execution engine bridges this gap — it takes a `LogicalPlan` tree and evaluates it against real data, producing Arrow `RecordBatch` results. Without it, queries parse and plan but return nothing.

## What Changes

- Create `crates/execution/` crate (package name: `arneb-execution`)
- Define `DataSource` trait — the abstraction execution uses to read data from connectors
- Implement expression evaluator that turns `PlanExpr` + `RecordBatch` into Arrow arrays using compute kernels
- Implement `Accumulator` trait and five built-in aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Define `ExecutionPlan` trait and eight physical operator implementations (Scan, Filter, Projection, Join, Aggregate, Sort, Limit, Explain)
- Implement `ExecutionContext` that converts a `LogicalPlan` tree into an `Arc<dyn ExecutionPlan>` tree and holds registered data sources

## Capabilities

### New Capabilities

- `datasource`: `DataSource` trait with `schema()` and `scan()` methods. `InMemoryDataSource` implementation for testing. The catalog crate stays metadata-only; execution defines what it needs from data providers.
- `expression-evaluator`: Evaluates `PlanExpr` nodes against a `RecordBatch`, producing `ArrayRef` results. Supports arithmetic, comparison, logical, string (LIKE), null checks (IS NULL/IS NOT NULL), BETWEEN, IN, CAST. Automatic numeric type coercion (Int32+Int64→Int64, int+float→Float64).
- `accumulators`: `Accumulator` trait for aggregate functions. Built-in implementations: CountAccumulator, SumAccumulator, AvgAccumulator, MinAccumulator, MaxAccumulator. Proper null handling.
- `execution-operators`: `ExecutionPlan` trait with `schema()`, `execute()`, `display_name()`. Eight operator structs: ScanExec, ProjectionExec, FilterExec, NestedLoopJoinExec, HashAggregateExec, SortExec, LimitExec, ExplainExec.
- `physical-planner`: `ExecutionContext` struct with data source registry. `create_physical_plan()` recursively converts `LogicalPlan` → `Arc<dyn ExecutionPlan>`, looking up `DataSource` instances for TableScan nodes.

### Modified Capabilities

(No existing capabilities modified)

## Impact

- **New crate**: `crates/execution/`
- **Dependencies**: `arneb-common`, `arneb-planner`, `arneb-sql-parser`
- **Downstream**: The `connectors` crate (Change 6) will implement `DataSource`; the `server` crate (Change 8) will wire everything together for end-to-end query execution
