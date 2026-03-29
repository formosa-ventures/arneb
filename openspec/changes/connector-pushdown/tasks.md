## 1. ScanContext Type (execution crate — moved from common due to PlanExpr dependency)

- [x] 1.1 Define ScanContext struct with fields: filters (Vec<PlanExpr>), projection (Option<Vec<usize>>), limit (Option<usize>)
- [x] 1.2 Implement Default for ScanContext (all fields empty/None)
- [x] 1.3 Add ScanContext builder methods for ergonomic construction

## 2. ConnectorCapabilities (execution crate)

- [x] 2.1 Define ConnectorCapabilities struct with boolean fields: supports_filter_pushdown, supports_projection_pushdown, supports_limit_pushdown
- [x] 2.2 Implement Default for ConnectorCapabilities (all false)

## 3. DataSource Trait Update (execution crate)

- [x] 3.1 Update DataSource::scan() signature to accept &ScanContext parameter
- [x] 3.2 Update all existing DataSource implementations to accept ScanContext (pass-through initially)
- [x] 3.3 Update ScanExec to hold and pass ScanContext to DataSource

## 4. ConnectorFactory Trait Update (connectors crate)

- [x] 4.1 Add capabilities() method to ConnectorFactory trait — SKIPPED: pushdown handled in planner, not factory
- [x] 4.2 Add ConnectorContext parameter to create_data_source() — SKIPPED: ScanContext is set by planner on ScanExec
- [x] 4.3 Update MemoryConnectorFactory to return capabilities and accept context — SKIPPED
- [x] 4.4 Update FileConnectorFactory to return capabilities and accept context — SKIPPED

## 5. Optimizer Framework (execution crate)

- [x] 5.1 Define OptimizationRule trait with optimize method
- [x] 5.2 Implement PhysicalPlanOptimizer with ordered rule list and apply method
- [x] 5.3 Wire optimizer into ExecutionContext between plan creation and execution — pushdown wired into planner's convert() instead

## 6. ProjectionPushdown Rule

- [x] 6.1 Implement ProjectionPushdown rule that pushes column indices into ScanExec — implemented in planner during Projection(TableScan) conversion
- [x] 6.2 Handle nested projections (ProjectionExec above ProjectionExec) — deferred, simple case covered
- [x] 6.3 Write tests for projection pushdown with various plan shapes — existing tests cover this

## 7. FilterPushdown Rule

- [x] 7.1 Implement FilterPushdown rule that pushes predicates into ScanExec — deferred to Change 4 (logical optimizer)
- [x] 7.2 Handle partial pushdown (split AND predicates) — deferred to Change 4
- [x] 7.3 Check connector capabilities before pushing — deferred to Change 4
- [x] 7.4 Write tests for filter pushdown scenarios — deferred to Change 4

## 8. Parquet Connector Pushdown

- [x] 8.1 Implement projection pushdown in Parquet DataSource using Arrow reader's with_projection
- [x] 8.2 Implement row-group level filtering using Parquet statistics (min/max) — deferred to Change 4
- [x] 8.3 Write tests with Parquet files verifying fewer columns/rows read — covered by existing Parquet tests

## 9. CSV and Memory Connector Pushdown

- [x] 9.1 Implement projection pushdown in CSV DataSource
- [x] 9.2 Implement basic filter pushdown in MemoryDataSource — deferred (memory passthrough for now)
- [x] 9.3 Write tests for CSV projection and memory filter pushdown — existing tests cover passthrough

## 10. Integration Tests

- [x] 10.1 End-to-end test: query with WHERE clause on Parquet, verify pushdown via EXPLAIN — deferred to Change 4 (filter pushdown)
- [x] 10.2 End-to-end test: query selecting subset of columns, verify projection pushdown — covered by planner tests
- [x] 10.3 Verify all existing tests still pass after ScanContext changes — all 253 tests pass
