## 1. Stage Identifier Types (`common` crate)

- [x] 1.1 Define `StageId(u32)` newtype with `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, `Display` in `crates/common/src/identifiers.rs`
- [x] 1.2 Define `TaskId { stage_id: StageId, partition_id: u32 }` with `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, `Display` (format: `"stage_id.partition_id"`)
- [x] 1.3 Define `SplitId(String)` newtype with `Debug`, `Clone`, `PartialEq`, `Eq`, `Hash`, `Display`
- [x] 1.4 Add `pub mod identifiers;` to `crates/common/src/lib.rs` and re-export types
- [x] 1.5 Unit tests for `StageId` Display, equality, and Hash
- [x] 1.6 Unit tests for `TaskId` Display, equality, and use as HashMap key
- [x] 1.7 Unit tests for `SplitId` Display and equality

## 2. PartitioningScheme (`planner` crate)

- [x] 2.1 Define `PartitioningScheme` enum (`Single`, `Hash { columns: Vec<usize> }`, `RoundRobin`, `Broadcast`) with `Debug`, `Clone`, `PartialEq`, `Eq`, `Display` in `crates/planner/src/fragment.rs`
- [x] 2.2 Unit tests for `PartitioningScheme` Display formatting

## 3. LogicalPlan Extensions (`planner` crate)

- [x] 3.1 Add `ExchangeNode { input: Box<LogicalPlan>, partitioning_scheme: PartitioningScheme, schema: Vec<ColumnInfo> }` variant to `LogicalPlan` enum
- [x] 3.2 Add `PartialAggregate { input: Box<LogicalPlan>, group_by: Vec<PlanExpr>, aggr_exprs: Vec<PlanExpr>, schema: Vec<ColumnInfo> }` variant to `LogicalPlan` enum
- [x] 3.3 Add `FinalAggregate { input: Box<LogicalPlan>, group_by: Vec<PlanExpr>, aggr_exprs: Vec<PlanExpr>, schema: Vec<ColumnInfo> }` variant to `LogicalPlan` enum
- [x] 3.4 Update `LogicalPlan::schema()` to handle `ExchangeNode`, `PartialAggregate`, `FinalAggregate`
- [x] 3.5 Update `Display` impl for `LogicalPlan` to format the three new variants
- [x] 3.6 Unit tests for new variant schema() and Display

## 4. FragmentType and PlanFragment (`planner` crate)

- [x] 4.1 Define `FragmentType` enum (`Source`, `Fixed`, `HashPartitioned`, `RoundRobin`) with `Debug`, `Clone`, `PartialEq`, `Eq`, `Display`
- [x] 4.2 Define `PlanFragment` struct with fields: `id: StageId`, `fragment_type: FragmentType`, `root: LogicalPlan`, `output_partitioning: PartitioningScheme`, `source_fragments: Vec<PlanFragment>`
- [x] 4.3 Implement `Display` for `PlanFragment` showing fragment id, type, partitioning, and source count
- [x] 4.4 Unit tests for `FragmentType` Display and equality
- [x] 4.5 Unit tests for `PlanFragment` construction and Display

## 5. QueryStage (`planner` crate)

- [x] 5.1 Define `QueryStage` struct with fields: `fragment: PlanFragment`, `parallelism: usize`, `output_partitioning: PartitioningScheme`
- [x] 5.2 Implement `Display` for `QueryStage`
- [x] 5.3 Unit tests for `QueryStage` construction and Display

## 6. PlanFragmenter Algorithm (`planner` crate)

- [x] 6.1 Define `PlanFragmenter` struct with `next_stage_id: u32` counter
- [x] 6.2 Implement `PlanFragmenter::new()` constructor
- [x] 6.3 Implement `PlanFragmenter::fragment(plan: LogicalPlan) -> PlanFragment` entry point
- [x] 6.4 Implement recursive `fragment_plan()` that walks the LogicalPlan tree top-down
- [x] 6.5 Implement exchange insertion above `TableScan` nodes (SOURCE fragment boundary with `RoundRobin`)
- [x] 6.6 Implement exchange insertion at `Join` inputs (both sides become separate fragments; Hash for equi-joins, Broadcast right side for cross joins)
- [x] 6.7 Implement two-phase aggregation split: `Aggregate` → `PartialAggregate` + `Exchange(Hash/Single)` + `FinalAggregate`
- [x] 6.8 Implement passthrough for non-fragmentable nodes (Filter, Projection, Sort, Limit, Explain)
- [x] 6.9 Ensure root fragment is always `FragmentType::Fixed` with `PartitioningScheme::Single`
- [x] 6.10 Implement bottom-up `StageId` assignment (leaves get lower IDs)

## 7. Module Integration

- [x] 7.1 Add `pub mod fragment;` to `crates/planner/src/lib.rs` and re-export `PlanFragmenter`, `PlanFragment`, `FragmentType`, `PartitioningScheme`, `QueryStage`
- [x] 7.2 Add `arneb-common` dependency to `crates/planner/Cargo.toml` if not already present (for `StageId`)

## 8. Tests

- [x] 8.1 Test: fragment a simple `SELECT * FROM t` (one SOURCE fragment + one FIXED root fragment)
- [x] 8.2 Test: fragment a `SELECT * FROM t WHERE x > 1` (filter stays in root fragment)
- [x] 8.3 Test: fragment a `SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id` (two SOURCE fragments + join fragment)
- [x] 8.4 Test: fragment a cross join (right side uses Broadcast)
- [x] 8.5 Test: fragment `SELECT region, SUM(amount) FROM orders GROUP BY region` (two-phase aggregation with Hash exchange on group key)
- [x] 8.6 Test: fragment `SELECT COUNT(*) FROM orders` (global aggregation with Single exchange)
- [x] 8.7 Test: fragment a complex query with join + aggregation (multiple stages)
- [x] 8.8 Test: verify StageId ordering is bottom-up
- [x] 8.9 Test: verify root fragment is always Fixed/Single

## 9. Quality

- [x] 9.1 `cargo build` compiles without warnings
- [x] 9.2 `cargo test -p arneb-common` — all tests pass (including new identifier tests)
- [x] 9.3 `cargo test -p arneb-planner` — all tests pass (including new fragmentation tests)
- [x] 9.4 `cargo clippy -- -D warnings` — clean
- [x] 9.5 `cargo fmt -- --check` — clean
