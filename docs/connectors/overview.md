# Connectors Overview

Arneb uses a trait-based connector system to abstract data access. All data sources — local files, cloud object stores, and external catalogs — implement the same interface.

## The DataSource Trait

Every connector implements the `DataSource` trait:

```rust
#[async_trait]
pub trait DataSource: Send + Sync + Debug {
    fn schema(&self) -> Vec<ColumnInfo>;
    async fn scan(&self, ctx: &ScanContext) -> Result<SendableRecordBatchStream, ExecutionError>;
}
```

- `schema()` returns the column definitions for the data source
- `scan()` produces an async stream of Arrow `RecordBatch` values

## Connector Registration

Connectors are registered through the `ConnectorFactory` and `ConnectorRegistry` traits. The server automatically wires up connectors based on your configuration:

- **File connector**: Activated for `[[tables]]` entries with local paths
- **Object store connector**: Activated for `[[tables]]` entries with `s3://`, `gs://`, or `az://` paths
- **Hive connector**: Activated for `[[catalogs]]` entries with `type = "hive"`

## Pushdown Optimization

Arneb pushes operations down into connectors when supported:

| Pushdown | Description | Benefit |
|----------|-------------|---------|
| **Filter pushdown** | WHERE conditions pushed to the scan level | Reduces data read from disk/network |
| **Projection pushdown** | Only requested columns are read | Reduces I/O for wide tables |
| **Limit pushdown** | LIMIT applied at scan level | Short-circuits reading early |

Connectors apply as many pushdown hints as they support. The query engine always applies any remaining operations above the scan, so correctness is guaranteed regardless of connector support.

## Available Connectors

- [File Connector](/connectors/file) — CSV and Parquet from local filesystem
- [Object Store](/connectors/object-store) — S3, GCS, and Azure Blob Storage
- [Hive](/connectors/hive) — Hive Metastore catalog with automatic table discovery
