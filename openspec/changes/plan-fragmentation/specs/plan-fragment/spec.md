## ADDED Requirements

### Requirement: FragmentType enum
The system SHALL define a `FragmentType` enum with the following variants:
- `Source` — a leaf fragment that reads data from a connector (contains TableScan)
- `Fixed` — a single-instance fragment (e.g., final aggregation, final output)
- `HashPartitioned` — distributed by hash partitioning on specified keys
- `RoundRobin` — distributed evenly across workers in round-robin fashion

It SHALL derive `Debug`, `Clone`, `PartialEq`, `Eq` and implement `Display`.

#### Scenario: Displaying FragmentType
- **WHEN** `FragmentType::Source` is formatted with `Display`
- **THEN** it produces `"Source"`

#### Scenario: FragmentType equality
- **WHEN** `FragmentType::Fixed` is compared with `FragmentType::Fixed`
- **THEN** they are equal

### Requirement: PlanFragment struct
The system SHALL define a `PlanFragment` struct with the following fields:
- `id: StageId` — unique identifier for this fragment
- `fragment_type: FragmentType` — classification of this fragment
- `root: LogicalPlan` — the root plan node of this fragment
- `output_partitioning: PartitioningScheme` — how this fragment's output is partitioned
- `source_fragments: Vec<PlanFragment>` — child fragments whose outputs feed into this fragment's ExchangeNode inputs

It SHALL derive `Debug` and `Clone`.

#### Scenario: Creating a source fragment
- **WHEN** a `PlanFragment` is created with `id: StageId(0)`, `fragment_type: FragmentType::Source`, a `TableScan` root, `output_partitioning: PartitioningScheme::RoundRobin`, and empty `source_fragments`
- **THEN** the fragment represents a leaf stage that reads from a table and distributes output via round-robin

#### Scenario: Fragment tree structure
- **WHEN** a root `PlanFragment` has two entries in `source_fragments`
- **THEN** it represents a stage that consumes data from two child stages (e.g., both sides of a join)

#### Scenario: Accessing fragment properties
- **WHEN** `fragment.id` is accessed on a fragment with `StageId(3)`
- **THEN** it returns `StageId(3)`

### Requirement: PlanFragment Display
The `PlanFragment` SHALL implement `Display` showing a summary of the fragment tree: fragment id, type, output partitioning, and the number of source fragments.

#### Scenario: Displaying a fragment
- **WHEN** a `PlanFragment` with `id: StageId(1)`, `fragment_type: Source`, `output_partitioning: RoundRobin` is formatted
- **THEN** the output includes the stage id, fragment type, and partitioning scheme
