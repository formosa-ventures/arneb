## ADDED Requirements

### Requirement: Timestamp literal arrays
The expression evaluator SHALL convert Timestamp scalar values to Arrow TimestampArray. Currently `crates/execution/src/expression.rs` (line ~200) returns "timestamp literal arrays not yet supported".

#### Scenario: Timestamp literal in expression
- **WHEN** a query contains a Timestamp literal value
- **THEN** the expression evaluator produces a TimestampArray with correct unit and timezone

### Requirement: Timestamp comparisons
The `compare_op` function in `expression.rs` SHALL support Timestamp comparisons (=, !=, <, <=, >, >=) using Arrow compute kernels.

#### Scenario: WHERE on Timestamp column
- **WHEN** `SELECT * FROM events WHERE created_at > TIMESTAMP '2025-01-01 00:00:00'`
- **THEN** the comparison produces correct boolean results

#### Scenario: Timestamp ordering
- **WHEN** two Timestamp values with the same TimeUnit are compared
- **THEN** the comparison follows chronological order

#### Scenario: Timezone-naive comparison
- **WHEN** two timezone-naive Timestamps are compared
- **THEN** comparison is performed on raw values without timezone conversion

### Requirement: Timestamp MIN/MAX aggregates
The MIN and MAX accumulators in `aggregate.rs` SHALL support Timestamp input. A `Timestamp(i64, TimeUnit)` variant SHALL be added to the `OrdScalar` enum.

#### Scenario: MIN of Timestamp column
- **WHEN** `SELECT MIN(created_at) FROM events`
- **THEN** the earliest timestamp is returned

#### Scenario: MAX of Timestamp column
- **WHEN** `SELECT MAX(updated_at) FROM records`
- **THEN** the latest timestamp is returned

### Requirement: Timestamp in ORDER BY
Timestamp columns SHALL be usable in ORDER BY clauses for result sorting.

#### Scenario: ORDER BY Timestamp
- **WHEN** `SELECT * FROM events ORDER BY created_at DESC`
- **THEN** results are sorted by timestamp in descending order
