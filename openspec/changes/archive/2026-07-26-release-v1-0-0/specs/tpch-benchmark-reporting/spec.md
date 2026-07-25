## MODIFIED Requirements

### Requirement: Three-way comparison table

For runs covering Arneb, Trino, and DataFusion, the report SHALL include a per-query table with one row per query containing, at minimum: query id, p50 (median) wall-clock duration for each engine, an Arneb-vs-Trino speedup, an Arneb-vs-DataFusion speedup, and the query status if not `ok`. Speedups MUST be expressed as a numeric multiplier with two decimals (e.g., `1.42x`), and MUST be omitted (`-`) when either side failed or was skipped.

A speedup of engine A over engine B MUST be computed as B's duration divided by A's, so that a value above `1.00x` always means A is that many times faster. The report MUST state this orientation in the text, next to the table. The reciprocal reads as the exact opposite of what it means — a genuine 2x win rendered as `0.50x` understates the result fourfold and appears to be a loss — so the orientation MUST be covered by an automated test rather than left to review.

The suite-level geomean MUST use the same orientation as the per-query columns, so the summary cannot contradict the table above it.

When fewer than three engines are present, the table MUST gracefully degrade to the engines available and still emit pairwise speedups for any two engines that both ran a given query.

#### Scenario: All three engines ran the same query

- **WHEN** Arneb, Trino, and DataFusion each have a successful `q01` measurement
- **THEN** the report's `q01` row shows three p50 columns and two speedup columns, all populated

#### Scenario: Trino is missing from the inputs

- **WHEN** only Arneb and DataFusion result files are passed
- **THEN** the report renders two p50 columns and a single Arneb-vs-DataFusion speedup column, with no Trino column at all

#### Scenario: The faster engine reads above 1.00x

- **WHEN** engine A completes a query in 100ms and engine B takes 400ms
- **THEN** the `A→B` speedup renders as `4.00x`, not `0.25x`

#### Scenario: Geomean agrees with the per-query table

- **WHEN** the per-query columns show engine A ahead of engine B
- **THEN** the suite-level geomean for that pair is also above `1.00x`

#### Scenario: Orientation is stated to the reader

- **WHEN** a reader looks at the per-query table
- **THEN** accompanying text tells them that a value above `1.00x` means the left-hand engine is that many times faster
