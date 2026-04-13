## Why

Arneb currently only reads data files from the local filesystem via `std::fs::File`. To support production data lake workloads, the engine must read Parquet and CSV files from cloud object stores (AWS S3, GCP GCS, Azure Blob Storage). This is the foundational layer for future Hive Metastore and table format (Iceberg/Delta) integration.

## What Changes

- Add `object_store` crate as a workspace dependency for unified S3/GCS/Azure/local access
- Refactor `ParquetDataSource` and `CsvDataSource` to use `ObjectStore` trait instead of `std::fs::File`
- Add async Parquet reading via `ParquetObjectReader` for streaming remote files
- Support URI scheme detection (`s3://`, `gs://`, `abfss://`, `file://`, plain path → local)
- Extend `arneb.toml` table config with optional storage backend and credential settings
- Add an object-store-aware file connector that transparently handles local and remote paths

## Capabilities

### New Capabilities
- `object-store-io`: Unified storage abstraction over local filesystem and cloud object stores (S3, GCS, Azure). Handles URI parsing, credential configuration, and async object reads.

### Modified Capabilities
- `file-connector`: File connector gains the ability to read from remote object stores, not just local paths. `ParquetDataSource` and `CsvDataSource` use `ObjectStore` trait internally.
- `server-config`: Table configuration supports remote URIs and per-table storage credentials.

## Impact

- **Crates modified**: `connectors` (file module), `server` (config), `common` (possibly URI types)
- **New dependencies**: `object_store` with feature flags for `aws`, `gcp`, `azure`
- **Config format**: `arneb.toml` gains optional `storage`, `s3_region`, `s3_endpoint`, `gcs_service_account_path`, `azure_storage_account` fields per table or global
- **No breaking changes**: Existing local file paths continue to work as-is (default to local filesystem)
