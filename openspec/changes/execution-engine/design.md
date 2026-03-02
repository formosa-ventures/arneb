## Context

trino-alt has `common` (shared types, errors), `sql-parser` (SQL → AST), `catalog` (table metadata), and `planner` (AST → LogicalPlan). The execution engine evaluates LogicalPlan trees against actual data, producing Arrow RecordBatch results.

Project conventions: `Arc<dyn Trait>` for polymorphism, `thiserror` for errors, trait-based extensibility, Arrow columnar format for all intermediate data.

## Goals / Non-Goals

**Goals:**

- Evaluate LogicalPlan trees to produce Arrow RecordBatch results
- Define DataSource trait for data providers (implemented by connectors in Change 6)
- Evaluate PlanExpr expressions using Arrow compute kernels
- Support five built-in aggregate functions: COUNT, SUM, AVG, MIN, MAX
- Implement all eight physical operators: Scan, Filter, Projection, Join, Aggregate, Sort, Limit, Explain
- Support all join types: CROSS, INNER, LEFT, RIGHT, FULL
- Automatic numeric type coercion for mixed-type operations
- Comprehensive unit tests including end-to-end pipeline tests

**Non-Goals:**

- No async/streaming execution (synchronous `Vec<RecordBatch>` return for MVP; Phase 2 adds `Stream<RecordBatch>`)
- No query optimization (separate optimizer crate, Change not yet started)
- No hash join or sort-merge join (nested-loop only for MVP simplicity)
- No spill-to-disk for large sorts or aggregates (in-memory only)
- No parallel execution within a single query
- No DISTINCT aggregate support in execution (planned for later)

## Decisions

### D1: Synchronous execution for MVP

**Choice**: `execute()` returns `Result<Vec<RecordBatch>>`. No async, no streaming.

**Rationale**: Single-node, in-memory data — async/streaming adds complexity without benefit. Phase 2 (distributed) will introduce `Stream<RecordBatch>` when inter-node communication requires it.

**Alternative**: `Stream<Item = Result<RecordBatch>>` from the start. Rejected because it complicates every operator and test for no immediate gain.

### D2: DataSource trait in execution crate, not catalog

**Choice**: The `DataSource` trait (with `scan()` method) lives in `crates/execution/`, not `crates/catalog/`.

**Rationale**: The catalog crate is metadata-only (`TableProvider` returns schema, not data). Execution defines what it needs from data providers — this keeps catalog lightweight and avoids a dependency from catalog on Arrow RecordBatch.

**Alternative**: Put DataSource in catalog. Rejected because it would force catalog to depend on RecordBatch, mixing metadata concerns with data access.

### D3: Nested-loop join for all join types

**Choice**: A single `NestedLoopJoinExec` handles CROSS, INNER, LEFT, RIGHT, and FULL joins.

**Rationale**: O(n×m) is acceptable for MVP with small datasets. It's simple to implement correctly for all join types including outer joins. Hash join can be added later as an optimization without changing the trait interface.

**Alternative**: Hash join for equi-joins. Deferred to optimization phase — the `ExecutionPlan` trait allows swapping implementations transparently.

### D4: Hash-based grouping for aggregation

**Choice**: `HashAggregateExec` uses a `HashMap<String, GroupState>` keyed by stringified group-by values.

**Rationale**: Simple and correct. The string key approach handles any data type without implementing custom hash/eq. Performance is acceptable for MVP data sizes.

**Trade-off**: String serialization is slower than native hashing. Can be optimized later with typed hash keys.

### D5: Expression evaluator using Arrow compute kernels

**Choice**: All expression evaluation delegates to Arrow's compute kernels (`arrow::compute::kernels::*`) for arithmetic, comparison, boolean logic, and string matching.

**Rationale**: Arrow kernels are vectorized, SIMD-optimized, and handle null propagation correctly. No need to reimplement element-wise logic.

### D6: Automatic numeric coercion

**Choice**: When binary operations have mismatched numeric types, both sides are cast to the wider type before the operation (Int32+Int64→Int64, int+float→Float64).

**Rationale**: Matches SQL semantics for implicit type widening. Avoids type errors on common expressions like `int_column > 1.5`.

## Risks / Trade-offs

**[O(n×m) joins]** → Nested-loop join is slow for large tables. **Mitigation**: Acceptable for MVP. Hash join will be added in the optimizer/execution improvement phase.

**[Full materialization]** → `Vec<RecordBatch>` materializes all results in memory. **Mitigation**: Phase 2 streaming will address this. For MVP single-node use, memory is sufficient.

**[String-keyed grouping]** → HashMap with string keys is slower than typed hashing. **Mitigation**: Correctness first. Performance optimization is a separate concern.

**[No spill-to-disk]** → Sort and aggregate hold all data in memory. **Mitigation**: MVP operates on small to moderate datasets. Resource exhaustion returns `ExecutionError::ResourceExhausted`.
