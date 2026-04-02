## Why

After optimizing the logical plan (Change 4), the system still produces a single monolithic plan tree meant for single-node execution. To enable distributed query execution across multiple workers, the optimized plan must be split into fragments separated by exchange boundaries. Each fragment can then be scheduled independently on different nodes, with exchanges handling data redistribution between them. Without plan fragmentation, distributed execution cannot begin.

## What Changes

- Add `StageId`, `TaskId`, `SplitId` identifier types to `crates/common/` for distributed execution addressing
- Add `ExchangeNode` and two-phase aggregation variants (`PartialAggregate`, `FinalAggregate`) to `LogicalPlan` enum in `crates/planner/`
- Define `PartitioningScheme` enum (SINGLE, HASH, ROUND_ROBIN, BROADCAST) in `crates/planner/`
- Define `PlanFragment` struct and `FragmentType` enum representing a distributable unit of work
- Define `QueryStage` struct grouping a fragment with parallelism and scheduling metadata
- Implement `PlanFragmenter` algorithm that walks an optimized `LogicalPlan` and inserts exchange boundaries, producing a tree of `PlanFragment`s
- Implement two-phase aggregation splitting: `Aggregate` → `PartialAggregate` + `Exchange(HASH)` + `FinalAggregate`

## Capabilities

### New Capabilities

- `stage-identifiers`: `StageId(u32)`, `TaskId { stage_id: StageId, partition_id: u32 }`, `SplitId(String)` identifier types in the common crate for addressing stages, tasks, and splits in distributed execution.
- `exchange-node`: `ExchangeNode` as a new `LogicalPlan` variant representing a data redistribution boundary between fragments. `PartitioningScheme` enum (SINGLE, HASH, ROUND_ROBIN, BROADCAST) determines how data flows across the boundary.
- `plan-fragment`: `PlanFragment` struct with `id: StageId`, `fragment_type: FragmentType`, `root: LogicalPlan`, `output_partitioning: PartitioningScheme`, `source_fragments: Vec<PlanFragment>`. `FragmentType` enum: SOURCE, FIXED, HASH_PARTITIONED, ROUND_ROBIN.
- `plan-fragmenter`: `PlanFragmenter` algorithm that walks a `LogicalPlan` top-down, inserts `ExchangeNode`s at distribution boundaries (above table scans, at join inputs, at aggregation boundaries), and produces a tree of `PlanFragment`s.
- `two-phase-aggregation`: Logic to split a single `Aggregate` node into `PartialAggregate` (runs on workers, produces partial results) + `ExchangeNode(HASH on group keys)` + `FinalAggregate` (combines partial results). Enables distributed aggregation.

### Modified Capabilities

- `logical-plan`: Add `ExchangeNode { input, partitioning_scheme, schema }`, `PartialAggregate { input, group_by, aggr_exprs, schema }`, and `FinalAggregate { input, group_by, aggr_exprs, schema }` variants to the `LogicalPlan` enum.

## Impact

- **Modified crate**: `crates/common/` — new identifier types (`StageId`, `TaskId`, `SplitId`)
- **Modified crate**: `crates/planner/` — new plan variants, `PlanFragmenter`, `PlanFragment`, `QueryStage`, `PartitioningScheme`
- **Dependencies**: `arneb-common` (identifiers), `arneb-planner` (plan types)
- **Downstream**: The distributed scheduler (future change) will use `PlanFragment` trees to assign stages to workers; the execution engine will need physical operators for exchange and two-phase aggregation
