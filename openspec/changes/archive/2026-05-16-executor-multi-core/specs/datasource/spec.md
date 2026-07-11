## MODIFIED Requirements

### Requirement: DataSource trait

The system SHALL define a `DataSource` trait with `schema()`, `partition_count()`, and `scan(ctx, partition)` methods. The trait SHALL require `Send + Sync + Debug` bounds so that data sources can be shared across operators via `Arc<dyn DataSource>`.

```rust
pub trait DataSource: Send + Sync + Debug {
    fn schema(&self) -> Vec<ColumnInfo>;

    /// Number of independent input partitions this data source exposes.
    /// File connectors typically return file count or row-group group count.
    /// Memory connectors return 1.
    fn partition_count(&self) -> usize { 1 }

    /// Stream the rows of a single partition.
    fn scan(
        &self,
        ctx: &ScanContext,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError>;
}
```

The default `partition_count()` returns 1 so existing single-partition connectors compile unchanged. Connectors that can read in parallel (multi-file Parquet, HMS S3, etc.) override this method.

#### Scenario: Single-partition data source

- **GIVEN** an `InMemoryDataSource` with one batch of 3 rows
- **WHEN** `source.partition_count()` is called
- **THEN** it returns `1`
- **AND** `source.scan(&ctx, 0)` returns a stream containing the 3 rows

#### Scenario: Multi-partition data source

- **GIVEN** a `HiveDataSource` wrapping a Hive table with 8 Parquet files
- **WHEN** `source.partition_count()` is called
- **THEN** it returns `8` (one partition per file)
- **AND** `source.scan(&ctx, 5)` returns a stream over file index 5 only

#### Scenario: Out-of-range partition

- **WHEN** `source.scan(&ctx, N)` is called with `N >= source.partition_count()`
- **THEN** it returns `Err(ExecutionError::InvalidPartition { requested: N, max: count - 1 })`

### Requirement: ScanContext carries shared planner state

The `ScanContext` (existing) SHALL be passed unchanged to `scan(ctx, partition)`. Any per-partition configuration (e.g. push-down filters) SHALL apply uniformly across all partitions of the same scan.

#### Scenario: Pushdown filter applies to every partition

- **GIVEN** a `HiveDataSource` with a pushdown filter `l_shipdate <= '1998-12-01'` set in `ScanContext`
- **WHEN** `scan(&ctx, 0)` and `scan(&ctx, 1)` are called
- **THEN** both partitions apply the filter to their respective files independently

## ADDED Requirements

### Requirement: HiveDataSource per-file partitioning

The `HiveDataSource` SHALL list all data files under the table's storage prefix at scan-context-build time and assign one partition per file. `partition_count()` SHALL return the file count. `scan(ctx, i)` SHALL read file index `i` only and return a stream over its rows.

The file list SHALL be computed once per query (not per partition) and cached for the lifetime of the `HiveDataSource` instance.

#### Scenario: SF1 lineitem reads multiple files in parallel

- **GIVEN** the SF1 `lineitem` table stored as 4 Parquet files in S3 (MinIO)
- **WHEN** the query runner executes `SELECT COUNT(*) FROM lineitem`
- **THEN** `HiveDataSource::partition_count()` returns `4` and 4 file reads happen concurrently across 4 spawned tasks

### Requirement: File connector per-file partitioning

The local file connector (`crates/connectors/src/file.rs`) SHALL similarly expose `partition_count()` reflecting the file count for glob-style table registrations. Single-file registrations SHALL report `partition_count() = 1`.

#### Scenario: Single-file local table

- **GIVEN** a table registered as `path = "/data/nation.parquet"` (single file)
- **WHEN** `source.partition_count()` is called
- **THEN** it returns `1`

#### Scenario: Glob-style multi-file local table

- **GIVEN** a table registered as `path = "/data/lineitem/*.parquet"` resolving to 8 files
- **WHEN** `source.partition_count()` is called
- **THEN** it returns `8`
