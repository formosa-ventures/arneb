## 1. Dependencies & Storage Registry

- [x] 1.1 Add `object_store` to workspace `Cargo.toml` with feature flags (`aws`, `gcp`, `azure`) behind optional Cargo features
- [x] 1.2 Create `StorageRegistry` in `crates/connectors/` that manages `Arc<dyn ObjectStore>` instances, caches by scheme+bucket
- [x] 1.3 Implement URI parsing: extract scheme, bucket, and object path from `s3://`, `gs://`, `abfss://`, `file://`, and plain paths
- [x] 1.4 Write unit tests for URI parsing and StorageRegistry instance caching

## 2. Configuration

- [x] 2.1 Extend `AppConfig` in `crates/server/src/config.rs` with `[storage.s3]`, `[storage.gcs]`, `[storage.azure]` sections (region, endpoint, service_account_path, etc.)
- [x] 2.2 Wire storage config into `StorageRegistry` construction at server startup in `main.rs`
- [x] 2.3 Update `TableConfig.path` to accept remote URIs (backward compatible — existing local paths unchanged)
- [x] 2.4 Write tests for config parsing with remote URIs and storage sections

## 3. Parquet Data Source Refactor

- [x] 3.1 Refactor `ParquetDataSource` to accept `Arc<dyn ObjectStore>` + `object_store::path::Path` instead of `PathBuf`
- [x] 3.2 Replace sync `ParquetRecordBatchReader` with async `ParquetObjectReader` + `ParquetRecordBatchStreamBuilder`
- [x] 3.3 Ensure projection pushdown continues to work via `ProjectionMask` on the async builder
- [x] 3.4 Implement async schema inference from remote Parquet footer
- [x] 3.5 Write integration tests: read local Parquet via `LocalFileSystem` ObjectStore

## 4. CSV Data Source Refactor

- [x] 4.1 Refactor `CsvDataSource` to accept `Arc<dyn ObjectStore>` + `Path` instead of `PathBuf`
- [x] 4.2 For remote CSV: use `ObjectStore::get()` to buffer content, then parse with `arrow-csv`
- [x] 4.3 Write integration tests: read local CSV via `LocalFileSystem` ObjectStore

## 5. FileConnectorFactory Wiring

- [x] 5.1 Update `FileConnectorFactory` to accept a `StorageRegistry` reference
- [x] 5.2 Update `register_table()` to use URI parsing → StorageRegistry → ObjectStore for each table
- [x] 5.3 Update `create_data_source()` to pass the correct ObjectStore to ParquetDataSource/CsvDataSource
- [x] 5.4 Update server startup in `main.rs` to construct `StorageRegistry` and pass to `FileConnectorFactory`

## 6. End-to-End Validation

- [x] 6.1 Write integration test: configure a table with a local `file://` URI and query it
- [x] 6.2 Write integration test: configure a table with a plain local path (backward compat) and query it
- [x] 6.3 Add documentation in CLAUDE.md for remote table configuration examples

## 7. Cloud Store Wiring

- [x] 7.1 Add `aws` feature to `object_store` in workspace `Cargo.toml` (no feature gate — object store is a core capability)
- [x] 7.2 Add `StorageRegistry::with_config(StorageConfig)` constructor that accepts storage config and lazy-creates cloud ObjectStore instances on cache miss via `AmazonS3Builder`
- [x] 7.3 Update `main.rs` to create per-catalog `StorageRegistry` with merged config (per-catalog storage overrides global `[storage]`); add `StorageConfig::merge()` helper
- [x] 7.4 Unit tests for `StorageRegistry` lazy creation with `S3Config` (verify builder construction, endpoint + allow_http for MinIO scenario)
- [x] 7.5 Integration test: S3 read via MinIO using `docker-compose.yml` (not required for CI — manual/nightly only)
