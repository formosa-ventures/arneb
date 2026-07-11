## ADDED Requirements

### Requirement: Per-query timing for TPC-DS
The benchmark runner SHALL record wall clock time for each TPC-DS query execution in milliseconds, using the same timing infrastructure as TPC-H benchmarks. Timing SHALL start when the query is sent to the server and end when the last result row is received.

#### Scenario: Passing query timing
- **WHEN** TPC-DS query q03 executes in 250ms
- **THEN** the recorded wall_clock_ms for that run is approximately 250

#### Scenario: Skipped query timing
- **WHEN** TPC-DS query q14 has a SKIP marker
- **THEN** no timing is recorded and the query appears with status "skipped"

### Requirement: Coverage metrics
The benchmark results SHALL include aggregate coverage statistics: total queries (99), passing count, skipped count, failed count, and a breakdown of skip reasons with counts.

#### Scenario: Coverage summary
- **WHEN** 18 queries pass, 75 are skipped, and 6 fail
- **THEN** the results include coverage: {total: 99, passing: 18, skipped: 75, failed: 6}

#### Scenario: Skip reason breakdown
- **WHEN** 35 queries are skipped for CTE, 12 for window frames, and 28 for other reasons
- **THEN** the coverage includes skip_reasons: {"CTE": 35, "window frame": 12, ...}

### Requirement: Progress tracking over time
The benchmark results JSON SHALL include a timestamp, enabling comparison across runs to track how many queries pass as SQL features are added. The output filename SHALL include the timestamp for historical tracking.

#### Scenario: Result file naming
- **WHEN** the benchmark completes at 2026-04-15 14:30:00 UTC
- **THEN** the output file is named `arneb_20260415_143000.json` in the output directory

#### Scenario: Progress comparison
- **WHEN** results from two different dates are compared
- **THEN** the number of newly-passing queries can be determined from the status fields
