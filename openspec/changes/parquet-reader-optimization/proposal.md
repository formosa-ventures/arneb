## Why

Arneb reads entire Parquet files into memory regardless of query filters. For a query like `SELECT * FROM lineitem WHERE l_shipdate = '1998-01-01'`, all 6 million rows are read and then filtered in memory. Parquet's row group statistics (min/max per column per row group) and predicate pushdown capabilities allow skipping entire row groups and pages that cannot match the filter, dramatically reducing I/O and memory usage.

Column projection pushdown is already implemented, but row group pruning and predicate pushdown are not. This is the highest-impact optimization for scan-heavy analytical queries.

## What Changes

- Implement row group pruning using Parquet row group statistics (min/max)
- Implement predicate pushdown from WHERE clauses to the Parquet reader
- Add configurable batch size for Parquet record batch streaming
- Apply to both file connector and Hive data source

## Capabilities

### New Capabilities

- `row-group-pruning`: Skip row groups whose statistics prove no rows can match the filter
- `predicate-pushdown`: Push WHERE conditions to Parquet's `RowFilter` or `ArrowPredicate`
- `batch-configuration`: Configurable batch size for Parquet scanning

### Modified Capabilities

- `DataSource::scan()` gains filter context (`ScanContext` extended with filter expressions)
- Existing connector-pushdown infrastructure extended to Parquet level

## Impact

- **crates/execution/src/**: ScanContext extended with filter expressions
- **crates/connectors/src/file.rs**: ParquetDataSource gains row group pruning and predicate pushdown
- **crates/hive/src/datasource.rs**: HiveDataSource gains same capabilities
- **Performance**: Significant reduction in I/O for filtered queries on large Parquet files
