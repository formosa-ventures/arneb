## ADDED Requirements

### Requirement: Configurable batch size in ScanContext
The `ScanContext` struct SHALL include an optional `batch_size: Option<usize>` field. When set, the Parquet reader SHALL call `builder.with_batch_size(n)` to control the number of rows per RecordBatch.

#### Scenario: Default batch size
- **WHEN** ScanContext has batch_size = None
- **THEN** the Parquet reader uses its default batch size (8192 rows)

#### Scenario: Custom batch size
- **WHEN** ScanContext has batch_size = Some(1024)
- **THEN** each RecordBatch from the Parquet reader contains at most 1024 rows

#### Scenario: Large batch size
- **WHEN** ScanContext has batch_size = Some(65536)
- **THEN** each RecordBatch contains at most 65536 rows
- **AND** memory usage per batch increases accordingly

### Requirement: Batch size applied to all Parquet readers
The batch size configuration SHALL apply to all Parquet reading paths:

- File connector `ParquetDataSource::scan()` in `crates/connectors/src/file.rs`
- File connector `PreResolvedParquetDataSource::scan()` in `crates/connectors/src/file.rs`
- Hive data source `HiveDataSource::scan()` in `crates/hive/src/datasource.rs`

#### Scenario: File connector respects batch size
- **WHEN** a Parquet file is read via file connector with batch_size = 1024
- **THEN** all returned RecordBatches have at most 1024 rows

#### Scenario: Hive data source respects batch size
- **WHEN** a Hive table is scanned with batch_size = 2048
- **THEN** all returned RecordBatches have at most 2048 rows

### Requirement: Results unchanged by batch size
Changing the batch size SHALL NOT affect query results — only the chunking of intermediate RecordBatches.

#### Scenario: Same results with different batch sizes
- **WHEN** the same query is executed with batch_size = 1024 and batch_size = 65536
- **THEN** the final result set is identical
