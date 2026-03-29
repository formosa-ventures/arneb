## Context

The current query pipeline is: SQL → AST → LogicalPlan → PhysicalPlan → Execute. There is no optimization step on the logical plan. The LogicalPlan enum currently has: TableScan, Projection, Filter, Join, Aggregate, Sort, Limit, Explain.

## Goals / Non-Goals

**Goals:**
- Extensible optimizer framework with pluggable rules
- 5 core rules: SimplifyFilters, ConstantFolding, PredicatePushdown, ProjectionPruning, LimitPushdown
- TableStatistics for future cost-based decisions
- Clean integration into QueryPlanner

**Non-Goals:**
- Cost-based optimization (CBO) — rules are heuristic/deterministic only
- Join reordering — requires CBO with table statistics, deferred
- Subquery optimization (decorrelation) — complex, deferred

## Decisions

1. **LogicalRule trait**: `fn name() -> &str` and `fn optimize(plan: LogicalPlan) -> Result<LogicalPlan>`. Rules are pure functions that transform the plan tree.

2. **Rule ordering**: SimplifyFilters → ConstantFolding → PredicatePushdown → ProjectionPruning → LimitPushdown. Simplification first so pushdown works on cleaner predicates.

3. **PredicatePushdown**: Pushes Filter through Projection (rewrite column refs), into Join sides (only push predicates that reference columns from one side). Does NOT push through Aggregate (would change semantics).

4. **ProjectionPruning**: Bottom-up pass that tracks which columns are referenced by downstream operators. Inserts minimal Projection above TableScan to prune unused columns.

5. **ConstantFolding**: Evaluates BinaryOp(Literal, Literal) at plan time. Also simplifies: `x AND true → x`, `x OR false → x`, `NOT NOT x → x`.

6. **SimplifyFilters**: `WHERE true → remove filter`, `WHERE false → empty result (EmptyRelation)`, tautology/contradiction detection.

7. **LimitPushdown**: Push LIMIT through Projection (pass through), Sort (keep sort but add limit). Does NOT push through Filter or Join.

8. **TableStatistics**: Struct in common crate. `row_count: Option<usize>`, `column_statistics: Vec<ColumnStatistics>` where ColumnStatistics has `min_value`, `max_value`, `null_count`, `distinct_count` (all Optional).

## Risks / Trade-offs

- **Rule interaction**: Rules can enable/inhibit each other. Fixed ordering mitigates but doesn't eliminate all cases. Multiple passes could help but add complexity.
- **Plan rewriting complexity**: Recursive tree transformation in Rust requires ownership management. Using `Box<LogicalPlan>` with take/replace pattern.
