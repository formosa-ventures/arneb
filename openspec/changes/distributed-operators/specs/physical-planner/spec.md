## MODIFIED Requirements

### Requirement: Distributed physical plan generation
The physical planner SHALL generate distributed physical plans from fragmented logical plans. When operating in distributed mode, the planner SHALL insert exchange operators (ShuffleWrite, Broadcast, Merge) between fragments based on the distribution strategy.

#### Scenario: Distributed join plan
- **WHEN** the planner processes a join between two large tables in distributed mode
- **THEN** it generates ShuffleWriteOperator on both sides partitioned by join keys
- **AND** each worker fragment contains a local HashJoinExec

#### Scenario: Distributed aggregation plan
- **WHEN** the planner processes a GROUP BY query in distributed mode
- **THEN** it generates PartialHashAggregateExec on worker fragments
- **AND** ShuffleWriteOperator on group-by keys
- **AND** FinalHashAggregateExec on the receiving fragment

#### Scenario: Distributed sort plan
- **WHEN** the planner processes an ORDER BY query in distributed mode
- **THEN** it generates SortExec on worker fragments
- **AND** MergeOperator on the coordinator fragment

### Requirement: Strategy-aware plan selection
The physical planner SHALL use `DistributionStrategy` to decide between broadcast and hash-partitioned plans for joins. It SHALL query `TableStatistics` from the catalog and compare against `broadcast_join_max_rows`.

#### Scenario: Broadcast join selected
- **WHEN** the smaller join side has fewer rows than the broadcast threshold
- **THEN** the planner generates a BroadcastOperator for the smaller side instead of ShuffleWriteOperator

### Requirement: Standalone mode compatibility
The physical planner SHALL continue to generate single-node plans (without exchange operators) when operating in standalone mode. The existing `LogicalPlan` to `ExecutionPlan` conversion SHALL remain unchanged for non-distributed queries.

#### Scenario: Standalone mode unchanged
- **WHEN** the server is running in standalone mode
- **THEN** the planner generates plans identical to the pre-distributed behavior
- **AND** no exchange operators are inserted
