## ADDED Requirements

### Requirement: Report generator script
The system SHALL provide a Python script `scripts/report.py` that reads benchmark result JSON files from both trino-alt and Trino, and produces a markdown comparison report.

#### Scenario: Generate comparison report
- **WHEN** `python scripts/report.py --trino-alt results/trino_alt_sf1.json --trino results/trino_sf1.json`
- **THEN** a markdown report is printed to stdout with a comparison table

#### Scenario: Missing Trino baseline
- **WHEN** only the trino-alt results file is provided
- **THEN** the report shows trino-alt results only with no speedup column

### Requirement: Comparison table format
The report SHALL include a markdown table with columns: Query, Trino (ms), trino-alt (ms), Speedup. Speedup SHALL be calculated as `trino_median / trino_alt_median` rounded to 2 decimal places. Values greater than 1.0 indicate trino-alt is faster.

#### Scenario: Table row format
- **WHEN** Q1 has trino median 200ms and trino-alt median 150ms
- **THEN** the table row shows `| Q1 | 200 | 150 | 1.33x |`

#### Scenario: Slower query
- **WHEN** Q5 has trino median 100ms and trino-alt median 300ms
- **THEN** the table row shows `| Q5 | 100 | 300 | 0.33x |`

#### Scenario: Skipped query
- **WHEN** Q15 was skipped in trino-alt
- **THEN** the table row shows `| Q15 | 200 | SKIP | - |`

### Requirement: Summary statistics
The report SHALL include summary statistics at the bottom: geometric mean speedup (across all comparable queries), median speedup, number of queries faster, number of queries slower, and number of queries skipped.

#### Scenario: Summary section
- **WHEN** 18 queries are comparable, 14 are faster, and 4 are slower
- **THEN** the summary shows geometric mean speedup, `14/18 faster`, `4/18 slower`, and the count of skipped queries

### Requirement: Environment documentation
The report SHALL include a metadata section documenting: scale factor, date, engine versions (if available), and a note that results are hardware-dependent.

#### Scenario: Metadata header
- **WHEN** the report is generated for SF1 on 2026-03-25
- **THEN** the report header includes `Scale Factor: SF1`, `Date: 2026-03-25`, and a hardware dependency disclaimer
