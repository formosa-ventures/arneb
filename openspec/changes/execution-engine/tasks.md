## 1. Crate Setup

- [x] 1.1 Add `crates/execution` to workspace members in root `Cargo.toml`
- [x] 1.2 Create `crates/execution/Cargo.toml` with package name `trino-execution`, dependencies: `trino-common`, `trino-planner`, `trino-sql-parser`, `arrow`
- [x] 1.3 Create `crates/execution/src/lib.rs` with module declarations and re-exports

## 2. DataSource (`datasource` module)

- [x] 2.1 Define `DataSource` trait with `schema()` and `scan()` methods
- [x] 2.2 Implement `InMemoryDataSource` with constructors: `new()`, `empty()`, `from_batch()`
- [x] 2.3 Implement helper `column_info_to_arrow_schema()`

## 3. Expression Evaluator (`expression` module)

- [x] 3.1 Implement `evaluate()` for Column and Literal variants
- [x] 3.2 Implement BinaryOp evaluation: arithmetic (add, sub, mul, div, rem), comparison (eq, neq, lt, lt_eq, gt, gt_eq), logical (and, or), string (like, nlike)
- [x] 3.3 Implement UnaryOp evaluation: Not, Minus, Plus
- [x] 3.4 Implement IsNull, IsNotNull, Cast evaluation
- [x] 3.5 Implement Between (composed as `>= low AND <= high`) and InList (composed as OR of equality checks)
- [x] 3.6 Implement `scalar_to_array()` helper for broadcasting literals
- [x] 3.7 Implement numeric type coercion (`coerce_numeric_pair`, `wider_numeric_type`)

## 4. Accumulators (`aggregate` module)

- [x] 4.1 Define `Accumulator` trait with `update_batch()`, `evaluate()`, `reset()`
- [x] 4.2 Implement `CountAccumulator` with COUNT(*) and COUNT(expr) modes
- [x] 4.3 Implement `SumAccumulator` for integer and floating-point types
- [x] 4.4 Implement `AvgAccumulator` returning Float64
- [x] 4.5 Implement `MinAccumulator` and `MaxAccumulator` using `OrdScalar` for total ordering
- [x] 4.6 Implement `create_accumulator()` factory function

## 5. Execution Operators (`operator` module)

- [x] 5.1 Define `ExecutionPlan` trait with `schema()`, `execute()`, `display_name()`
- [x] 5.2 Implement `ScanExec` — reads all data from a DataSource
- [x] 5.3 Implement `ProjectionExec` — evaluates expressions on input batches with type coercion
- [x] 5.4 Implement `FilterExec` — evaluates boolean predicate, applies mask via `filter_record_batch`
- [x] 5.5 Implement `NestedLoopJoinExec` — supports Cross, Inner, Left, Right, Full join types with proper null handling for outer joins
- [x] 5.6 Implement `HashAggregateExec` — hash-based group-by with accumulators; supports both grouped and global (no group-by) aggregation
- [x] 5.7 Implement `SortExec` — multi-key lexicographic sort via `lexsort_to_indices`
- [x] 5.8 Implement `LimitExec` — applies OFFSET and LIMIT via batch concatenation and slicing
- [x] 5.9 Implement `ExplainExec` — returns the LogicalPlan text as a single-column Utf8 batch

## 6. Physical Planner (`planner` module)

- [x] 6.1 Define `ExecutionContext` with `HashMap<String, Arc<dyn DataSource>>` registry
- [x] 6.2 Implement `register_data_source()` and `create_physical_plan()`
- [x] 6.3 Implement recursive `convert()` mapping each LogicalPlan variant to its ExecutionPlan operator

## 7. Tests

- [x] 7.1 DataSource tests: empty source, source with data, schema inference from batch
- [x] 7.2 Expression tests: column, literal, add, mixed-type add, comparison, AND/OR, NOT, negate, IS NULL, BETWEEN, IN, LIKE, CAST, column out-of-bounds error
- [x] 7.3 Accumulator tests: COUNT non-null, COUNT(*), SUM int, SUM float, SUM empty, AVG, AVG empty, MIN, MAX string, MIN empty, reset
- [x] 7.4 Operator tests: scan, filter, projection, limit, limit+offset, sort, aggregate (no grouping), cross join, explain
- [x] 7.5 Physical planner tests: table scan, filter, projection, limit+offset, sort, aggregate count+sum, explain, table not found error
- [x] 7.6 End-to-end integration test: `SELECT name FROM users WHERE id > 2 LIMIT 2` through logical plan → physical plan → execute → verify RecordBatch contents

## 8. Quality

- [x] 8.1 `cargo build` compiles without warnings
- [x] 8.2 `cargo test -p trino-execution` — all 47 tests pass
- [x] 8.3 `cargo clippy -- -D warnings` — clean
- [x] 8.4 `cargo fmt -- --check` — clean
