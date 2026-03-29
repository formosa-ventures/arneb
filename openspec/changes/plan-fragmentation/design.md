## Context

trino-alt has `common` (shared types, errors), `sql-parser` (SQL → AST), `catalog` (table metadata), `planner` (AST → LogicalPlan), and `execution` (physical operators). The logical-plan-optimizer (Change 4, Phase 2) produces optimized LogicalPlan trees. Plan fragmentation splits these optimized plans into distributable fragments separated by exchange boundaries.

Project conventions: `Arc<dyn Trait>` for polymorphism, `thiserror` for errors, Arrow columnar format, trait-based extensibility.

## Goals / Non-Goals

**Goals:**

- Define identifier types (StageId, TaskId, SplitId) for distributed execution addressing
- Extend LogicalPlan with ExchangeNode, PartialAggregate, and FinalAggregate variants
- Define PartitioningScheme for data redistribution strategies
- Define PlanFragment and FragmentType to represent distributable plan units
- Implement PlanFragmenter that walks a LogicalPlan and produces a fragment tree
- Implement two-phase aggregation splitting for distributed aggregation
- Define QueryStage for scheduling metadata
- Comprehensive unit tests for all fragmentation scenarios

**Non-Goals:**

- No physical exchange operator implementation (separate execution change)
- No actual distributed scheduling or task assignment (separate scheduler change)
- No cost-based fragment optimization (e.g., choosing broadcast vs hash based on table size)
- No support for co-located joins (where both sides are partitioned on join keys)
- No fragment merging or pipeline optimization

## Decisions

### D1: ExchangeNode as a LogicalPlan variant

**Choice**: Add `ExchangeNode` directly as a variant of the existing `LogicalPlan` enum rather than a separate plan type.

**Rationale**: The exchange represents a logical operation (data redistribution) that is part of the plan tree. Keeping it as a LogicalPlan variant means the entire plan tree remains a single recursive type, simplifiable by existing Display/Debug logic and future optimizer rules.

**Alternative**: A separate `DistributedPlan` wrapper type. Rejected because it would require duplicating tree-walking logic and break the uniform plan representation.

### D2: Two-phase aggregation at fragmentation time

**Choice**: Split `Aggregate` into `PartialAggregate` + `Exchange(HASH)` + `FinalAggregate` during plan fragmentation, not during optimization.

**Rationale**: This is a distribution concern, not a logical optimization. The optimizer works on single-node semantics; fragmentation decides how to distribute work. Keeping the split in the fragmenter means the optimizer sees simple Aggregate nodes.

**Alternative**: Split during optimization. Rejected because the optimizer should not need to know about distribution topology.

### D3: Fragment tree structure

**Choice**: `PlanFragment` contains `source_fragments: Vec<PlanFragment>` forming a tree. The root fragment is the final output stage; leaf fragments are SOURCE fragments containing TableScan nodes.

**Rationale**: Mirrors Trino's stage tree. Each fragment's ExchangeNode inputs correspond to its source fragments. The scheduler can walk this tree to determine execution order (leaves first, root last).

### D4: PartitioningScheme as an enum

**Choice**: `PartitioningScheme` is an enum with variants: `Single` (gather to one node), `Hash { columns: Vec<usize> }` (hash-distribute by column indices), `RoundRobin` (distribute evenly), `Broadcast` (send to all nodes).

**Rationale**: These four schemes cover all standard distributed SQL patterns. Hash partitioning needs column indices to determine the hash key. The enum is extensible for future schemes.

### D5: Identifier types in common crate

**Choice**: `StageId(u32)`, `TaskId { stage_id: StageId, partition_id: u32 }`, `SplitId(String)` live in `crates/common/`.

**Rationale**: These identifiers are used across multiple crates (planner for fragmentation, scheduler for assignment, execution for tracking). Placing them in common avoids circular dependencies.

### D6: FragmentType classification

**Choice**: `FragmentType` enum with `Source` (reads from connector), `Fixed` (single-instance, e.g., final aggregation), `HashPartitioned` (distributed by hash), `RoundRobin` (distributed evenly).

**Rationale**: Matches Trino's fragment type classification. The fragment type determines how many parallel instances the scheduler creates and how data is routed.

## Risks / Trade-offs

**[Static fragmentation rules]** → The fragmenter uses fixed rules (exchange above every table scan, at join inputs) rather than cost-based decisions. **Mitigation**: Acceptable for initial implementation. Cost-based decisions (e.g., broadcast join for small tables) can be added later as optimizer rules.

**[Two-phase aggregation always applied]** → Every Aggregate is split into partial+final, even when single-node execution would be faster. **Mitigation**: The scheduler can collapse fragments for single-node mode. The fragmenter produces the distributed plan structure; execution can optimize.

**[No pipeline optimization]** → Each exchange boundary creates a new stage with full materialization between stages. **Mitigation**: Pipeline optimization (fusing compatible fragments) is a future enhancement. Correctness first.
