## ADDED Requirements

### Requirement: TPC-DS comparison report
The system SHALL provide a script `benchmarks/tpcds/scripts/report.sh` that generates a markdown comparison report from TPC-DS benchmark results. The report SHALL cover all 99 queries showing status and, for passing queries, performance comparison.

#### Scenario: Generate TPC-DS report
- **WHEN** `./benchmarks/tpcds/scripts/report.sh --arneb results/arneb.json --trino results/trino.json`
- **THEN** a markdown report is printed to stdout

#### Scenario: Arneb-only report
- **WHEN** only arneb results are provided
- **THEN** the report shows query status (pass/skip/fail) without performance comparison

### Requirement: Coverage summary section
The report SHALL begin with a coverage summary showing: total queries, passing count and percentage, skipped count with reason breakdown, and failed count.

#### Scenario: Coverage header
- **WHEN** 18/99 queries pass
- **THEN** the report header shows "Coverage: 18/99 (18.2%)"

#### Scenario: Skip reason table
- **WHEN** queries are skipped for various SQL feature reasons
- **THEN** the report includes a table: Skip Reason | Count (e.g., "CTE | 35", "Window frames | 12")

### Requirement: Performance comparison table
The report SHALL include a markdown table for passing queries with columns: Query, Trino (ms), arneb (ms), Speedup. Queries that are skipped or failed in arneb SHALL show their status instead of timing.

#### Scenario: Passing query row
- **WHEN** q03 has Trino median 300ms and arneb median 200ms
- **THEN** the table row shows `| q03 | 300 | 200 | 1.50x |`

#### Scenario: Skipped query row
- **WHEN** q14 is skipped in arneb with reason "requires CTE support"
- **THEN** the table row shows `| q14 | 150 | SKIP (CTE) | - |`

### Requirement: Summary statistics
The report SHALL include summary statistics for passing queries: geometric mean speedup, median speedup, number faster, number slower. Statistics SHALL only include queries that passed on both engines.

#### Scenario: Summary with partial coverage
- **WHEN** 18 queries pass on both engines, 12 are faster in arneb, 6 are slower
- **THEN** the summary shows geometric mean speedup, "12/18 faster", "6/18 slower", "81 skipped/failed"
