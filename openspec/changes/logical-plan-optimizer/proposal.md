## Why

Query optimization is essential for performance. Currently, the logical plan produced by the planner is executed as-is with no optimization. This means redundant columns are read, filters aren't pushed to the optimal position, and constant expressions are evaluated repeatedly. A logical optimizer framework with standard rules (predicate pushdown, projection pruning, constant folding) can dramatically reduce data processed — critical before distribution amplifies inefficiency.

## What Changes

- Add `LogicalOptimizer` framework: ordered list of `LogicalRule` trait objects applied to LogicalPlan
- Implement 5 optimization rules: PredicatePushdown, ProjectionPruning, ConstantFolding, SimplifyFilters, LimitPushdown
- Add `TableStatistics` struct to common (row_count, column statistics with min/max/null_count/distinct_count)
- Extend `TableProvider` with optional `statistics()` method
- Wire optimizer into QueryPlanner: parse → plan → **optimize** → physical plan

## Capabilities

### New Capabilities
- `logical-optimizer-framework`: LogicalRule trait, LogicalOptimizer pipeline applying rules in sequence
- `predicate-pushdown-rule`: Push Filter nodes through Projection, into correct Join side
- `projection-pruning-rule`: Remove unused columns from TableScan by tracking downstream references
- `constant-folding-rule`: Evaluate constant expressions at plan time, simplify boolean logic
- `table-statistics`: TableStatistics struct with row count and per-column statistics

### Modified Capabilities
- `logical-plan`: Add plan rewriter/visitor patterns for optimizer rules
- `catalog-traits`: TableProvider gains optional statistics() method
- `query-planner`: Wire optimizer between logical plan creation and physical planning

## Impact

- **Crates**: planner, common
- **Dependencies**: No new external deps
