## ADDED Requirements

### Requirement: Per-query timing
The benchmark runner SHALL record wall clock time for each query execution in milliseconds. Timing SHALL start when the query is sent to the server and end when the last result row is received.

#### Scenario: Single query timing
- **WHEN** query Q1 executes in 150ms
- **THEN** the recorded wall_clock_ms for that run is approximately 150

#### Scenario: Multiple runs timing
- **WHEN** query Q1 is run 5 times with times [180, 155, 150, 152, 148] ms
- **THEN** all 5 timings are recorded in the results

### Requirement: Row count tracking
The benchmark runner SHALL record the number of rows returned by each query execution. This serves as a correctness check — the row count should be consistent across runs.

#### Scenario: Consistent row counts
- **WHEN** query Q1 returns 4 rows on the first run and 4 rows on subsequent runs
- **THEN** all runs record rows_returned = 4

#### Scenario: Row count mismatch warning
- **WHEN** a query returns different row counts across runs
- **THEN** the runner logs a warning about inconsistent results

### Requirement: Warm-up handling
The benchmark runner SHALL discard the first N runs (default 2) as warm-up. Only subsequent runs SHALL be included in timing statistics. Warm-up runs SHALL still be recorded in the raw output but marked as warm-up.

#### Scenario: Warm-up exclusion
- **WHEN** 5 total runs are configured with 2 warm-up runs
- **THEN** runs 1 and 2 are marked as warm-up
- **AND** only runs 3, 4, and 5 are used for statistics

### Requirement: Summary statistics
The benchmark runner SHALL compute summary statistics for each query from non-warm-up runs: minimum, maximum, median, mean, and p95 wall clock time.

#### Scenario: Statistics computation
- **WHEN** query Q1 has 3 measured runs with times [150, 152, 148] ms
- **THEN** the summary shows min=148, max=152, median=150, mean=150
