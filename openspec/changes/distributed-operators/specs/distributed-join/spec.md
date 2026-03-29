## ADDED Requirements

### Requirement: Broadcast join plan generation
The system SHALL generate a broadcast join plan when the distribution strategy selects broadcast. The smaller side of the join SHALL be wrapped in a `BroadcastOperator` and replicated to all workers. Each worker executes a local `HashJoinExec` with the full broadcast side and its partition of the larger side.

#### Scenario: Small dimension table broadcast
- **WHEN** a JOIN query has a left side with 500 rows and a right side with 1,000,000 rows and the broadcast threshold is 10,000
- **THEN** the planner generates a plan where the left side is broadcast to all workers
- **AND** each worker joins its partition of the right side with the complete left side

#### Scenario: Broadcast side selection
- **WHEN** both sides of a join are below the broadcast threshold
- **THEN** the smaller side is chosen for broadcast

### Requirement: Hash-partitioned join plan generation
The system SHALL generate a hash-partitioned join plan when both sides exceed the broadcast threshold. Both sides SHALL be wrapped in `ShuffleWriteOperator` partitioned on the join key columns. Each worker receives matching partitions from both sides and executes a local `HashJoinExec`.

#### Scenario: Large table hash-partitioned join
- **WHEN** a JOIN query has both sides exceeding the broadcast threshold of 10,000 rows
- **THEN** the planner generates a plan with `ShuffleWriteOperator` on both sides using join key columns
- **AND** matching partitions from both sides are co-located on the same worker

#### Scenario: Multi-column join key partitioning
- **WHEN** a JOIN has condition `ON a.region = b.region AND a.year = b.year`
- **THEN** both sides are hash-partitioned on `[region, year]` columns

### Requirement: Join strategy in physical plan
The generated physical plan for distributed joins SHALL include exchange operators (shuffle or broadcast) followed by local `HashJoinExec` operators on each worker fragment. The plan fragment graph SHALL ensure data dependencies are satisfied before join execution.

#### Scenario: Fragment dependency ordering
- **WHEN** a distributed hash-partitioned join is planned
- **THEN** the shuffle fragments for both sides complete before the join fragment begins execution
