## MODIFIED Requirements

### Requirement: Parquet connector supports projection pushdown
The Parquet file connector SHALL support projection pushdown by reading only the columns specified in ScanContext.projection.

#### Scenario: Reading Parquet with projection
- **WHEN** a Parquet DataSource receives a ScanContext with projection [0, 2]
- **THEN** it SHALL configure the Parquet reader to read only columns at indices 0 and 2

### Requirement: CSV connector supports projection pushdown
The CSV file connector SHALL support projection pushdown by reading all columns but projecting the output to only the specified columns.

#### Scenario: Reading CSV with projection
- **WHEN** a CSV DataSource receives a ScanContext with projection [1, 3]
- **THEN** it SHALL return a stream with only columns at indices 1 and 3
