## ADDED Requirements

### Requirement: Filter expression in ScanContext
The `ScanContext` struct SHALL include an optional filter expression (`Option<PlanExpr>`) that represents the WHERE clause predicate applicable to the scan. The physical planner SHALL populate this field when a FilterOperator directly above a ScanOperator has a pushable predicate.

#### Scenario: Simple filter passed to scan
- **WHEN** the query is `SELECT * FROM lineitem WHERE l_quantity > 10`
- **THEN** ScanContext contains a filter expression representing `l_quantity > 10`

#### Scenario: No filter
- **WHEN** the query is `SELECT * FROM lineitem`
- **THEN** ScanContext filter is None

### Requirement: Predicate pushdown to Parquet RowFilter
The Parquet reader SHALL translate supported filter expressions from ScanContext into `ArrowPredicate` instances and apply them via `ParquetRecordBatchStreamBuilder::with_row_filter()`. This evaluates predicates during Parquet decoding before materializing full Arrow arrays.

#### Scenario: Simple comparison pushdown
- **WHEN** ScanContext contains `l_quantity > 10`
- **THEN** the Parquet reader applies this as a RowFilter during decoding
- **AND** fewer rows are materialized compared to in-memory filtering

#### Scenario: Unsupported expression falls through
- **WHEN** ScanContext contains `UPPER(name) = 'FOO'` (function call)
- **THEN** the predicate is NOT pushed to Parquet
- **AND** in-memory filtering handles it (existing behavior)

### Requirement: Supported pushdown expressions
The following expression types SHALL be pushable to Parquet:

- Column comparisons with literals: `column op literal` where op is =, !=, <, <=, >, >=
- AND conjunctions of pushable expressions
- IS NULL / IS NOT NULL on columns

#### Scenario: AND conjunction
- **WHEN** the filter is `l_shipdate >= '1998-01-01' AND l_quantity < 50`
- **THEN** both conditions are pushed to Parquet as a combined RowFilter

#### Scenario: OR disjunction not pushed
- **WHEN** the filter is `l_shipdate = '1998-01-01' OR l_quantity > 40`
- **THEN** the filter is NOT pushed to Parquet (handled in-memory)

### Requirement: Correctness guarantee
Predicate pushdown SHALL produce identical results to in-memory filtering for all queries. No valid rows SHALL be silently dropped.

#### Scenario: TPC-H Q6 correctness
- **WHEN** TPC-H Q6 is executed with and without predicate pushdown
- **THEN** the result (revenue sum) is identical in both cases

#### Scenario: Null handling
- **WHEN** a filter `column > 5` is pushed down
- **AND** some rows have NULL in the column
- **THEN** NULL rows are excluded (same as in-memory NULL comparison semantics)
