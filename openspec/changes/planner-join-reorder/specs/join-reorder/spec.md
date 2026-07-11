## ADDED Requirements

### Requirement: JoinReorder is an AnalysisPass

The system SHALL provide a `JoinReorder` struct in `crates/planner/src/analyzer/join_reorder.rs` implementing the `AnalysisPass` trait. The pass SHALL rewrite the input `LogicalPlan` so that contiguous inner joins are reordered to minimize total cost.

```rust
pub struct JoinReorder {
    catalog_stats: Arc<CatalogStats>,
    config: ReorderConfig,
}

pub struct ReorderConfig {
    pub dp_max_tables: usize,      // default 8
    pub default_table_size: u64,   // default 10_000
}

impl AnalysisPass for JoinReorder { /* ... */ }
```

#### Scenario: Pass is registered in default pipeline

- **WHEN** `Analyzer::default_pipeline()` is constructed
- **THEN** the pipeline contains `JoinReorder` after `TypeCoercion`

### Requirement: Pass identifies reorderable join groups

The pass SHALL walk the `LogicalPlan` tree and identify maximal contiguous sub-trees of `LogicalPlan::Join { join_type: Inner, .. }` separated by non-inner operators. Each identified group SHALL be reordered independently.

#### Scenario: Single inner-join chain

- **GIVEN** a plan `Inner(Inner(Inner(A, B), C), D)` where every join is inner
- **WHEN** the pass runs
- **THEN** it identifies one reorderable group `{A, B, C, D}`

#### Scenario: Group boundary at outer join

- **GIVEN** a plan `Inner(LeftJoin(A, B), C)`
- **WHEN** the pass runs
- **THEN** it identifies the inner-join group `{LeftJoin(A, B) as one input, C}`
- **AND** does NOT attempt to reorder across the `LeftJoin`

#### Scenario: Group boundary at aggregate

- **GIVEN** a plan `Inner(Aggregate(Inner(A, B)), C)`
- **WHEN** the pass runs
- **THEN** the inner pair `{A, B}` is one group and the outer pair `{Aggregate(...), C}` is another

### Requirement: Dynamic programming for N ≤ dp_max_tables

For a reorderable group of `N` tables where `N <= dp_max_tables` (default 8), the system SHALL run Selinger-style dynamic programming:

```
best_plan(S) = min over (L, R) partitions of S of
               join(best_plan(L), best_plan(R), join_conditions_between(L, R))
```

Implemented via memoization in a `HashMap<BTreeSet<TableId>, (Plan, Cost)>`. The chosen plan SHALL have minimum estimated cardinality across all valid bushy trees (left-deep, right-deep, and bushy alike).

#### Scenario: Smallest table chosen as build side

- **GIVEN** a 3-way inner join `lineitem (6M) ⋈ orders (1.5M) ⋈ customer (150K)` on standard TPC-H keys
- **WHEN** the DP reorderer runs
- **THEN** the chosen plan joins the two smaller relations first (e.g. `customer ⋈ orders ⋈ lineitem`) so the largest table (`lineitem`) appears on the probe side of the outermost join

#### Scenario: DP is deterministic

- **GIVEN** the same input plan and the same `CatalogStats`
- **WHEN** the DP reorderer runs twice
- **THEN** it produces byte-identical plans both times (tie-breaking by table ID)

#### Scenario: DP never produces a Cartesian product when avoidable

- **GIVEN** a join graph that is fully connected
- **WHEN** the DP reorderer runs
- **THEN** the chosen plan only includes joins for which an equi-condition exists between the two sides
- **AND** Cartesian products are skipped during enumeration

### Requirement: Greedy fallback for N > dp_max_tables

For a reorderable group with `N > dp_max_tables` tables, the system SHALL apply a greedy heuristic instead of DP:

1. Pick the single table with the smallest filtered cardinality (apply per-table WHERE predicates first).
2. At each step, pick the next table whose join with the current sub-tree has the smallest estimated output cardinality.
3. Break ties by `min(ndv_l, ndv_r)` on the join key (lower → better build).

The greedy result SHALL still respect connectivity (no Cartesian join if an alternative exists).

#### Scenario: 12-way join falls back to greedy

- **GIVEN** a 12-table inner-join group and `dp_max_tables = 8`
- **WHEN** the pass runs
- **THEN** it applies the greedy heuristic and emits one warning-level trace `tracing::warn!("join-reorder fallback to greedy: N=12 exceeds dp_max_tables=8")`

### Requirement: Subquery sub-trees are skipped

The pass SHALL detect `PlanExpr::ScalarSubquery`, `PlanExpr::InSubquery`, and `PlanExpr::ExistsSubquery` predicates and SHALL NOT reorder the join graph they reference. Plans inside subqueries SHALL be reordered separately (the pass recurses into subquery sub-plans).

#### Scenario: Correlated subquery leaves outer plan untouched

- **GIVEN** a plan whose `Filter` predicate references a correlated `ScalarSubquery(other_plan)`
- **WHEN** the pass runs
- **THEN** the outer plan's join group is reordered, and `other_plan` is reordered independently if it contains inner joins

### Requirement: NO_REORDER hint disables the pass

When the source SQL contains a `/*+ NO_REORDER */` comment immediately after the leading `SELECT`/`WITH` keyword of the top-level statement, the system SHALL skip the join-reorder pass for that statement.

#### Scenario: Hint disables reorder

- **GIVEN** a query `SELECT /*+ NO_REORDER */ * FROM a, b, c WHERE a.k = b.k AND b.k = c.k` planned with stats indicating a different optimal order
- **WHEN** the pass runs
- **THEN** the join order in the output plan matches the SQL textual order (`a ⋈ b ⋈ c`)

#### Scenario: Hint absence applies reorder

- **GIVEN** the same query without the hint
- **WHEN** the pass runs
- **THEN** the output plan may reorder joins as the cost model dictates

### Requirement: Pass is no-op when no inner-join groups exist

A plan with zero inner-join chains (e.g. a single-table scan, or only outer joins) SHALL be returned unchanged.

#### Scenario: Single-table query

- **GIVEN** the plan `Projection(TableScan(t))`
- **WHEN** the pass runs
- **THEN** the output plan is byte-identical to the input

#### Scenario: Only outer joins

- **GIVEN** the plan `LeftJoin(LeftJoin(A, B), C)`
- **WHEN** the pass runs
- **THEN** the output plan is byte-identical to the input

### Requirement: Pass attaches reorder annotation

After running on a reorderable group, the pass SHALL set a metadata flag on the root of that group's resulting plan recording whether reorder happened and what the original SQL order was. The flag is consumed by `EXPLAIN ANALYZE` output formatting.

```rust
pub struct ReorderAnnotation {
    pub applied: bool,
    pub original_order: Vec<TableReference>,
    pub chosen_order: Vec<TableReference>,
}
```

#### Scenario: Annotation present after reorder

- **GIVEN** a reordered plan
- **WHEN** the annotation is queried
- **THEN** `applied = true` and `original_order != chosen_order`

#### Scenario: Annotation present when no-op

- **GIVEN** a plan whose textual order was already optimal
- **WHEN** the annotation is queried
- **THEN** `applied = false` and `original_order == chosen_order`

### Requirement: Pass is property-test correct

A property test SHALL verify that, for any randomly generated inner-join graph of up to 6 tables with random per-table cardinalities and per-edge join-key NDVs, the DP reorderer's chosen plan has estimated cardinality less than or equal to the textual-order plan's estimated cardinality.

#### Scenario: Random join graph

- **GIVEN** a random inner-join graph with N tables, random `row_count`s in `[10, 10_000_000]`, and random NDVs
- **WHEN** `JoinReorder::reorder` and `estimated_cardinality` are computed on both the chosen plan and the textual-order plan
- **THEN** `cost(chosen) <= cost(textual)` for every sample

### Requirement: Pass robustness against missing stats

When every table in a group lacks `row_count` and the cost model uses defaults, the pass SHALL still terminate, never panic, and SHALL emit a debug-level trace explaining the fallback.

#### Scenario: No stats available

- **GIVEN** a 4-way inner join where every `TableProvider::statistics()` returns `None`
- **WHEN** the pass runs
- **THEN** every table is estimated at `default_table_size`, DP runs to completion using only NDV/selectivity defaults, and the pass returns successfully
