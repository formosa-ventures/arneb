## Why

Changes 1–5 built the full query pipeline (parse → plan → execute), but the execution engine can only read from `InMemoryDataSource` instances constructed in code. There is no way to query persistent data — CSV files on disk, Parquet files, or tables registered dynamically. The connectors crate bridges catalog metadata and execution data access, providing concrete `DataSource` implementations that read real data from external sources.

## What Changes

- Create `crates/connectors/` crate (package name: `arneb-connectors`)
- Implement a `ConnectorFactory` trait that creates `DataSource` instances from catalog table metadata, and a `ConnectorRegistry` that maps catalog names to factories
- Implement an in-memory connector that wraps pre-built RecordBatches (replacing ad-hoc `InMemoryDataSource` usage with a catalog-integrated connector)
- Implement a file connector that reads CSV and Parquet files from the local filesystem, producing Arrow RecordBatches
- Provide a `TableProviderWithData` trait (or extended `TableProvider`) that unifies catalog metadata with data source creation, so the server can wire catalogs to the execution engine without manual DataSource registration

## Capabilities

### New Capabilities

- `connector-traits`: `ConnectorFactory` trait that creates `Arc<dyn DataSource>` from table metadata (table reference, schema, connector-specific options). `ConnectorRegistry` that maps connector names to factories for dynamic dispatch.
- `memory-connector`: In-memory connector implementing `ConnectorFactory`. Creates `InMemoryDataSource` instances from pre-registered RecordBatches. Integrates with the catalog system by implementing `CatalogProvider`/`SchemaProvider`/`TableProvider` with data access.
- `file-connector`: File-based connector that reads CSV and Parquet files from the local filesystem. CSV reader infers or uses provided schema. Parquet reader uses Arrow's native Parquet support. Both produce `Vec<RecordBatch>` via the `DataSource` trait.

### Modified Capabilities

(No existing capabilities modified)

## Impact

- **New crate**: `crates/connectors/`
- **Dependencies**: `arneb-common`, `arneb-catalog`, `arneb-execution` (for `DataSource` trait), `arrow` (with `csv` and `parquet` features), `parquet` crate
- **Downstream**: The `server-integration` crate (Change 8) will use `ConnectorRegistry` to wire catalogs to the execution engine, enabling end-to-end SQL queries against files
