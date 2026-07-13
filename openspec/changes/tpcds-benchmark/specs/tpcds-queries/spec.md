## ADDED Requirements

### Requirement: TPC-DS query files
The system SHALL provide SQL files for all 99 TPC-DS queries in `benchmarks/tpcds/queries/q{01-99}.sql`. Each file SHALL contain a single SQL query adapted from the TPC-DS specification to be compatible with the arneb SQL parser where possible.

#### Scenario: Query files exist
- **WHEN** the queries directory is listed
- **THEN** it contains files q01.sql through q99.sql (99 files total)

#### Scenario: Query file format
- **WHEN** any query file is read
- **THEN** it contains either a valid SQL statement ending with a semicolon, or a SKIP marker followed by the query SQL

### Requirement: SKIP markers for unsupported queries
Queries that require SQL features not yet supported by arneb SHALL begin with `-- SKIP: <reason>` on the first line. The reason SHALL name the specific SQL feature required (e.g., "requires CTE support", "requires ROLLUP", "requires LAG window function"). A query MAY list multiple reasons separated by commas.

#### Scenario: CTE-dependent query
- **WHEN** a query requires Common Table Expressions
- **THEN** the file begins with `-- SKIP: requires CTE support`

#### Scenario: Multiple missing features
- **WHEN** a query requires both CTEs and ROLLUP
- **THEN** the file begins with `-- SKIP: requires CTE support, requires ROLLUP`

#### Scenario: Supported query
- **WHEN** a query uses only SQL features supported by arneb
- **THEN** the file contains no SKIP marker and begins directly with the SQL statement

### Requirement: SQL dialect adaptation
Each TPC-DS query SHALL be adapted from the Trino TPC-DS query set to use SQL syntax compatible with the arneb parser. Adaptations MAY include: adjusting date literal syntax, replacing unsupported functions, and restructuring expressions.

#### Scenario: Date literal adaptation
- **WHEN** a query uses Trino-specific date syntax
- **THEN** the query is adapted to use DATE 'YYYY-MM-DD' cast syntax supported by arneb

#### Scenario: Query source
- **WHEN** queries are adapted
- **THEN** they are derived from Trino's TPC-DS query templates to maximize compatibility

### Requirement: Skip reason categories
SKIP markers SHALL use consistent category names to enable tracking by SQL feature. The standard categories SHALL include: CTE, ROLLUP, CUBE, GROUPING SETS, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTILE, window frame (ROWS/RANGE BETWEEN), INTERVAL arithmetic, LATERAL JOIN, and statistical aggregates (STDDEV/VARIANCE).

#### Scenario: Category consistency
- **WHEN** two queries both require CTE support
- **THEN** both use the exact same skip reason text "requires CTE support"

#### Scenario: Skip reason grep
- **WHEN** `grep "SKIP.*CTE" benchmarks/tpcds/queries/*.sql` is run
- **THEN** it returns all queries blocked by CTE support, enabling batch un-skipping when CTEs are implemented
