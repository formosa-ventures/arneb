## ADDED Requirements

### Requirement: TPC-H query files
The system SHALL provide SQL files for all 22 TPC-H queries in `benchmarks/tpch/queries/q{01-22}.sql`. Each file SHALL contain a single SQL query adapted from the TPC-H specification to be compatible with the arneb SQL parser.

#### Scenario: Query files exist
- **WHEN** the queries directory is listed
- **THEN** it contains files q01.sql through q22.sql (22 files total)

#### Scenario: Query file format
- **WHEN** any query file is read
- **THEN** it contains a single valid SQL statement ending with a semicolon

### Requirement: SQL dialect adaptation
Each TPC-H query SHALL be adapted from the official specification to use SQL syntax supported by the arneb parser. Adaptations MAY include: replacing unsupported functions with equivalents, restructuring subqueries, and adjusting date arithmetic syntax.

#### Scenario: Q1 pricing summary report
- **WHEN** Q1 is executed against SF1 data
- **THEN** it returns aggregated pricing data grouped by returnflag and linestatus for shipments before a cutoff date

#### Scenario: Q6 forecasting revenue change
- **WHEN** Q6 is executed against SF1 data
- **THEN** it returns a single row with the revenue sum for filtered line items

### Requirement: Unsupported query documentation
Queries that cannot be adapted due to missing SQL features SHALL be documented with a comment at the top of the file listing the unsupported features. The benchmark runner SHALL skip these queries and report them as "skipped".

#### Scenario: Unsupported query file
- **WHEN** a query requires an unsupported SQL feature (e.g., correlated subquery)
- **THEN** the file begins with `-- SKIP: <reason>` and the runner skips it

### Requirement: Query validation
Each adapted query SHALL be validated by running it against SF1 data and verifying it produces a non-empty result set. The expected row count for each query at SF1 SHALL be documented.

#### Scenario: Q1 row count at SF1
- **WHEN** Q1 runs against SF1 data
- **THEN** it returns 4 rows (one per returnflag/linestatus combination)
