## ADDED Requirements

### Requirement: PlanFragmenter struct
The system SHALL define a `PlanFragmenter` struct that takes an optimized `LogicalPlan` and produces a tree of `PlanFragment`s with exchange boundaries inserted at distribution points. The fragmenter SHALL assign monotonically increasing `StageId` values starting from 0.

#### Scenario: Fragmenting a simple table scan query
- **WHEN** `PlanFragmenter` fragments a plan `Projection(Filter(TableScan("orders")))`
- **THEN** it produces two fragments: a SOURCE fragment (StageId 0) containing the TableScan, and a FIXED root fragment (StageId 1) containing `Projection(Filter(ExchangeNode(source_ref)))` with the SOURCE fragment as a source_fragment

### Requirement: Exchange insertion above TableScan
The fragmenter SHALL insert an `ExchangeNode` above every `TableScan` node, creating a SOURCE fragment boundary. The `ExchangeNode`'s partitioning scheme SHALL default to `RoundRobin`.

#### Scenario: Single table scan
- **WHEN** a plan with a single `TableScan("lineitem")` is fragmented
- **THEN** the result has a SOURCE fragment containing the TableScan and an exchange boundary with `PartitioningScheme::RoundRobin` between the source fragment and the parent fragment

### Requirement: Exchange insertion at join boundaries
The fragmenter SHALL insert `ExchangeNode`s at both inputs of a `Join` node. Each join input becomes a separate fragment. For equi-joins, the exchange SHALL use `PartitioningScheme::Hash` on the join key columns. For cross joins, the right side SHALL use `PartitioningScheme::Broadcast`.

#### Scenario: Inner join fragmentation
- **WHEN** a plan `Join(TableScan("orders"), TableScan("lineitem"), Inner, ON orders.id = lineitem.order_id)` is fragmented
- **THEN** it produces at least three fragments: two SOURCE fragments (one per table), and a parent fragment containing the Join with ExchangeNode inputs. The exchanges use `Hash` partitioning on the join key columns.

#### Scenario: Cross join fragmentation
- **WHEN** a plan `Join(TableScan("a"), TableScan("b"), Cross)` is fragmented
- **THEN** the right-side exchange uses `PartitioningScheme::Broadcast`

### Requirement: Fragment ID assignment
The fragmenter SHALL assign `StageId` values in bottom-up order: leaf (SOURCE) fragments get lower IDs, and the root (output) fragment gets the highest ID. IDs SHALL be monotonically increasing starting from 0.

#### Scenario: ID ordering in a join query
- **WHEN** a join query with two table scans is fragmented
- **THEN** the two SOURCE fragments have StageId(0) and StageId(1), and the root fragment has StageId(2)

### Requirement: Root fragment is always FIXED with Single partitioning
The root (output) fragment SHALL always have `FragmentType::Fixed` and `output_partitioning: PartitioningScheme::Single`, since the final result must be gathered to a single coordinator node.

#### Scenario: Root fragment properties
- **WHEN** any plan is fragmented
- **THEN** the root `PlanFragment` has `fragment_type: FragmentType::Fixed` and `output_partitioning: PartitioningScheme::Single`

### Requirement: Passthrough for non-fragmentable nodes
Plan nodes that do not introduce distribution boundaries (Filter, Projection, Sort, Limit) SHALL remain in their parent fragment without inserting additional exchanges.

#### Scenario: Filter and Projection stay in parent fragment
- **WHEN** a plan `Projection(Filter(ExchangeNode(TableScan)))` is fragmented
- **THEN** the Filter and Projection remain in the same fragment as the ExchangeNode consumer, not split into separate fragments
