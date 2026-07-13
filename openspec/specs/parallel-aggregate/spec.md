## ADDED Requirements

### Requirement: Two-phase partial+final hash aggregate

The system SHALL implement parallel hash aggregation as a two-phase plan:

1. **PartialAggregateExec** runs per input partition, building a partial hash map per partition. Output is `Partitioning::UnknownPartitioning(N)` where `N` is the input partition count.
2. **Hash repartition** by the group-by keys (inserted by the physical planner) brings rows of the same group together.
3. **FinalAggregateExec** runs per partition over the repartitioned input, merging the partial states by group.

```rust
pub struct PartialAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    group_by: Vec<PlanExpr>,
    aggregates: Vec<AggregateExpr>,
}

pub struct FinalAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    group_by: Vec<PlanExpr>,
    aggregates: Vec<AggregateExpr>,
}
```

The physical planner SHALL select between two-phase parallel and single-phase serial aggregation based on the cardinality estimate (see "Cardinality fallback" below).

#### Scenario: Two-phase produces same result as single-phase

- **GIVEN** a query `SELECT col_a, SUM(col_b), COUNT(*), AVG(col_c) FROM t GROUP BY col_a`
- **WHEN** the query is executed with `target_partitions = 4`
- **THEN** the output rows and aggregate values are exactly equal to executing the same query with `target_partitions = 1` (after sorting both by `col_a` to compare)

#### Scenario: SUM is partial-state correct

- **GIVEN** a `SUM(col)` aggregate over rows `[1, 2, 3, 4]` split across 2 partitions as `[1, 2]` and `[3, 4]`
- **WHEN** the two-phase aggregate runs
- **THEN** partial: partition 0 emits `SUM = 3`, partition 1 emits `SUM = 7`; final merges to `SUM = 10`

#### Scenario: AVG is partial-state correct via SUM/COUNT split

- **GIVEN** an `AVG(col)` aggregate over the same input
- **WHEN** the two-phase aggregate runs
- **THEN** the partial state for `AVG` is `(sum, count)`, the final state merges sums and counts then divides

#### Scenario: COUNT(DISTINCT) is correct

- **GIVEN** `COUNT(DISTINCT col)` over rows `[1, 2, 2, 3]` split across 2 partitions as `[1, 2]` and `[2, 3]`
- **WHEN** the two-phase aggregate runs
- **THEN** the final result is `3` (the partial state preserves the distinct set, not just a count)

### Requirement: Cardinality-driven fallback

The system SHALL fall back to a single-partition aggregate when the cost model's estimated number of groups (`estimated_cardinality(Aggregate)`) is below a configured threshold (default 1024). In that case the planner emits a `CoalescePartitionsExec` before a single `HashAggregateExec` instead of the two-phase plan.

The threshold SHALL be configurable via `[execution] parallel_aggregate_min_groups = 1024` in `arneb.toml`.

#### Scenario: Low cardinality falls back to single-phase

- **GIVEN** a query whose `LogicalPlan::Aggregate` estimates `100` groups
- **WHEN** the physical planner runs with `parallel_aggregate_min_groups = 1024`
- **THEN** the plan contains `HashAggregateExec(CoalescePartitionsExec(input))`, not the two-phase plan

#### Scenario: High cardinality uses two-phase

- **GIVEN** a query whose `Aggregate` estimates `1_000_000` groups
- **WHEN** the physical planner runs
- **THEN** the plan contains `FinalAggregateExec(Repartition(Hash, PartialAggregateExec(input)))`

#### Scenario: Fallback is opt-out via config

- **GIVEN** `[execution] parallel_hash_aggregate = false`
- **WHEN** the physical planner runs
- **THEN** every aggregate uses the single-phase plan regardless of cardinality

### Requirement: Per-partition memory bound

The system SHALL guarantee that each `PartialAggregateExec` partition's hash map holds at most `estimated_groups / partition_count + headroom` entries before being flushed. Memory growth is bounded per partition.

#### Scenario: Partial aggregate spills are not implemented in v1

- **WHEN** a partial-aggregate partition's hash map exceeds memory budget
- **THEN** the operator returns `ExecutionError::OutOfMemory { .. }` rather than spilling (spill-to-disk is out of scope for this change)
