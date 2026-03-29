## ADDED Requirements

### Requirement: DistributionStrategy enum
The system SHALL define a `DistributionStrategy` enum with variants: `Broadcast` (replicate small side to all workers) and `HashPartition` (hash-partition both sides on join keys).

#### Scenario: Enum variants
- **WHEN** the distribution strategy is inspected
- **THEN** it is either `DistributionStrategy::Broadcast` or `DistributionStrategy::HashPartition`

### Requirement: Strategy selection for joins
The system SHALL select a distribution strategy for joins based on `TableStatistics`. If the smaller side has `row_count < broadcast_join_max_rows`, the strategy SHALL be `Broadcast`. Otherwise, the strategy SHALL be `HashPartition`.

#### Scenario: Small table selects broadcast
- **WHEN** the left side has 500 rows and the right side has 1,000,000 rows and `broadcast_join_max_rows = 10000`
- **THEN** the strategy is `Broadcast` with the left side as the broadcast side

#### Scenario: Both sides large selects hash partition
- **WHEN** both sides have more than 10,000 rows and `broadcast_join_max_rows = 10000`
- **THEN** the strategy is `HashPartition`

#### Scenario: Both sides small selects broadcast of smaller
- **WHEN** the left side has 100 rows and the right side has 200 rows
- **THEN** the strategy is `Broadcast` with the left side (smaller) as the broadcast side

### Requirement: Configurable broadcast threshold
The system SHALL read `broadcast_join_max_rows` from `ServerConfig`. The default value SHALL be 10,000. This threshold determines the maximum row count for a table side to be eligible for broadcast.

#### Scenario: Default threshold
- **WHEN** no `broadcast_join_max_rows` is configured
- **THEN** the threshold defaults to 10,000

#### Scenario: Custom threshold
- **WHEN** `broadcast_join_max_rows = 50000` is set in configuration
- **THEN** tables with up to 50,000 rows are eligible for broadcast

### Requirement: Unknown statistics fallback
The system SHALL default to `HashPartition` when `TableStatistics` are unavailable for either side of a join. This is the safe default that works for any data size.

#### Scenario: Missing statistics
- **WHEN** neither side of a join has `TableStatistics` available
- **THEN** the strategy defaults to `HashPartition`
