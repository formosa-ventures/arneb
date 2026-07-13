## MODIFIED Requirements

### Requirement: LogicalPlan to ExecutionPlan conversion

The `create_physical_plan()` method SHALL recursively convert a `LogicalPlan` tree into an `Arc<dyn ExecutionPlan>` tree. Each `LogicalPlan` variant maps to its corresponding operator. During conversion, the planner SHALL insert `RepartitionExec` and `CoalescePartitionsExec` to satisfy each operator's `required_input_partitioning()`.

| LogicalPlan | ExecutionPlan | Partitioning behavior |
|-------------|---------------|------------------------|
| TableScan | ScanExec | `UnknownPartitioning(source.partition_count())` |
| Projection | ProjectionExec | Passthrough |
| Filter | FilterExec | Passthrough |
| Join (inner equi) | HashJoinExec | Probe passthrough; build coalesced |
| Aggregate (high cardinality) | PartialAggregateExec → Hash repartition → FinalAggregateExec | Two-phase |
| Aggregate (low cardinality) | HashAggregateExec(CoalescePartitionsExec(...)) | Single-phase fallback |
| Sort | SortExec → SortMergeExec | Per-partition sort + merge |
| Limit | LocalLimitExec → CoalescePartitionsExec → GlobalLimitExec | Pre-cap + global cap |
| Distinct | RepartitionExec(Hash) → LocalDistinctExec → CoalescePartitionsExec | Hash + per-partition dedup |
| Window (aligned) | WindowExec | Per-partition |
| Window (unaligned) | WindowExec(CoalescePartitionsExec(...)) | Coalesced |
| Explain | ExplainExec | Single partition |

#### Scenario: Repartition inserted when needed

- **GIVEN** a `Filter(Aggregate(TableScan(t)))` plan where the aggregate needs `Hash(group_keys, N)` input but the scan emits `UnknownPartitioning(8)`
- **WHEN** the physical planner runs with `target_partitions = 14`
- **THEN** the plan is rewritten as `FinalAggregateExec(RepartitionExec(Hash, PartialAggregateExec(...)))` with the repartition bridging the partition shape mismatch

#### Scenario: Coalesce inserted at root when needed

- **GIVEN** a 14-partition plan whose root is `Filter`
- **WHEN** the physical planner runs
- **THEN** the root is wrapped in `CoalescePartitionsExec` so the query runner sees a single sequential stream

#### Scenario: Planning a table scan

- **WHEN** `create_physical_plan(&LogicalPlan::TableScan { table: "users", .. })` is called and "users" is registered
- **THEN** it returns `Ok(Arc<ScanExec>)` wrapping the registered data source
- **AND** `output_partitioning()` of the result reflects the data source's `partition_count()`

## ADDED Requirements

### Requirement: target_partitions controls overall parallelism

The physical planner SHALL read `target_partitions` from the `ExecutionContext` (sourced from `arneb.toml [execution] target_partitions` or CLI override). When inserting `Hash` or `RoundRobinBatch` repartition operators, the planner SHALL use `target_partitions` as the partition count `N`.

#### Scenario: target_partitions = 14 produces 14-way parallelism

- **GIVEN** `target_partitions = 14`
- **WHEN** the planner inserts a hash repartition for an aggregate
- **THEN** the partition count is `14`

#### Scenario: target_partitions = 1 disables parallelism

- **GIVEN** `target_partitions = 1`
- **WHEN** the planner runs
- **THEN** no `RepartitionExec` with `n > 1` is inserted; the entire plan is single-partition

### Requirement: Planner enforces partitioning requirements via insertion

When a child operator's `output_partitioning()` does NOT satisfy its parent's `required_input_partitioning()`, the planner SHALL insert a `RepartitionExec` between them. When the child's count is greater than the parent's requirement of `UnknownPartitioning(1)`, the planner SHALL insert a `CoalescePartitionsExec` instead.

#### Scenario: Insertion is automatic

- **WHEN** the planner walks the plan tree
- **THEN** for every parent-child pair, `child.output_partitioning().satisfies(&parent_required_for_child)` evaluates to `true` after insertion
