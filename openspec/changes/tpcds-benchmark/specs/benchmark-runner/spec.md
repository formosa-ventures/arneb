## ADDED Requirements

### Requirement: Runner reuse via queries-dir
The existing benchmark runner at `benchmarks/tpch/src/main.rs` SHALL be used for TPC-DS benchmarks without code modification. The runner is invoked with `--queries-dir benchmarks/tpcds/queries` to discover and execute TPC-DS query files.

#### Scenario: Run TPC-DS against arneb
- **WHEN** `cargo run -p tpch-bench -- --queries-dir benchmarks/tpcds/queries` is executed
- **THEN** the runner discovers all 99 query files and executes each one
- **AND** queries with SKIP markers are recorded as skipped

#### Scenario: Run TPC-DS against Trino
- **WHEN** `cargo run -p tpch-bench -- --engine trino --queries-dir benchmarks/tpcds/queries --catalog tpcds --schema sf1` is executed
- **THEN** the runner executes all 99 queries against Trino via REST API

#### Scenario: Run subset of queries
- **WHEN** `cargo run -p tpch-bench -- --queries-dir benchmarks/tpcds/queries --queries 1,3,7` is executed
- **THEN** only q01, q03, and q07 are executed

### Requirement: SKIP marker detection
The benchmark runner SHALL detect `-- SKIP:` markers at the beginning of query files. When a SKIP marker is detected, the query SHALL be recorded with status "skipped" and the skip reason captured from the marker text.

#### Scenario: Skipped query output
- **WHEN** q14.sql begins with `-- SKIP: requires CTE support`
- **THEN** the runner records query q14 with status "skipped" and reason "requires CTE support"
- **AND** the runner continues to the next query

#### Scenario: Skip marker not present
- **WHEN** q03.sql contains no SKIP marker
- **THEN** the runner executes the query normally and records timing

### Requirement: Convenience wrapper script
The system SHALL provide `benchmarks/tpcds/scripts/run.sh` as a convenience wrapper that invokes the runner with TPC-DS defaults.

#### Scenario: Default arneb run
- **WHEN** `./benchmarks/tpcds/scripts/run.sh` is executed
- **THEN** it runs `cargo run -p tpch-bench -- --queries-dir benchmarks/tpcds/queries --output-dir benchmarks/tpcds/results`

#### Scenario: Trino baseline run
- **WHEN** `./benchmarks/tpcds/scripts/run.sh --engine trino` is executed
- **THEN** it runs the benchmark against Trino with catalog=tpcds and schema=sf1
