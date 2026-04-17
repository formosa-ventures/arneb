## ADDED Requirements

### Requirement: Row group pruning via column statistics
The Parquet reader SHALL examine row group column statistics (min/max values) and skip row groups that cannot contain matching rows for the query's filter predicate. This applies to both file connector (`crates/connectors/src/file.rs`) and Hive data source (`crates/hive/src/datasource.rs`).

#### Scenario: Date range filter skips row groups
- **WHEN** a Parquet file has 10 row groups with `l_shipdate` statistics [1992-01-01, 1992-12-31], [1993-01-01, 1993-12-31], ..., [2001-01-01, 2001-12-31]
- **AND** the query filters `WHERE l_shipdate >= DATE '1998-01-01'`
- **THEN** only row groups with max >= 1998-01-01 are read (approximately 4 of 10)

#### Scenario: Equality filter prunes row groups
- **WHEN** a query filters `WHERE nation_key = 5`
- **AND** a row group has statistics min=10, max=20 for nation_key
- **THEN** that row group is skipped

#### Scenario: No statistics available
- **WHEN** a row group does not have statistics for the filtered column
- **THEN** the row group is NOT pruned (conservative: read it)

#### Scenario: Multiple filter conditions (AND)
- **WHEN** a query has `WHERE a > 10 AND b < 20`
- **AND** a row group has stats a=[5,8] (all below 10)
- **THEN** the row group is pruned based on the `a > 10` condition alone

### Requirement: Supported types for statistics pruning
Row group pruning SHALL support statistics comparison for: Int32, Int64, Float32, Float64, Utf8, Date32, Decimal128, Timestamp.

#### Scenario: Int64 column pruning
- **WHEN** filtering on an Int64 column with row group stats
- **THEN** pruning works correctly using integer comparison

#### Scenario: Decimal128 column pruning
- **WHEN** filtering on a Decimal128 column with row group stats
- **THEN** pruning works correctly with scale-aware comparison

### Requirement: Debug logging for pruning
The system SHALL log at DEBUG level how many row groups were pruned vs. read for each scan operation.

#### Scenario: Pruning log output
- **WHEN** a scan reads 3 of 10 row groups
- **THEN** a debug log entry shows "pruned 7/10 row groups for filter: l_shipdate >= '1998-01-01'"
