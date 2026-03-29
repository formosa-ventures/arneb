## Why

The current execution engine only supports nested-loop joins, which are O(n*m) and impractical for large tables. Hash joins provide O(n+m) performance for equi-join conditions (the most common join type). Hash join is also the building block for distributed joins — distributed hash-partitioned joins require a local hash join operator on each worker.

## What Changes

- Implement `HashJoinExec` physical operator supporting INNER, LEFT, RIGHT, and FULL joins with equi-join conditions
- Implement `JoinHashMap` — internal hash table mapping join key values to row indices in the build side
- Add `JoinSelection` optimization rule that chooses HashJoin for equi-joins and NestedLoop for non-equi
- Update physical planner to use join selection

## Capabilities

### New Capabilities
- `hash-join-operator`: HashJoinExec with build/probe phases for INNER, LEFT, RIGHT, FULL equi-joins
- `hash-table`: JoinHashMap for efficient build-side key lookup
- `join-selection-rule`: Optimization rule choosing join strategy based on condition type

### Modified Capabilities
- `execution-operators`: Add HashJoinExec to the operator set
- `physical-planner`: Choose join strategy based on join condition analysis

## Impact

- **Crates**: execution
- **Breaking**: None — additive change, NestedLoopJoin still available for non-equi conditions
- **Dependencies**: No new external deps
