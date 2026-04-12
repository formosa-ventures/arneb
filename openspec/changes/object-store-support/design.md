## Context

Arneb's file connector reads Parquet and CSV files exclusively through `std::fs::File`. The `ParquetDataSource` and `CsvDataSource` in `crates/connectors/src/file.rs` open local file paths at both schema-inference time and scan time. The `DataSource` trait itself is async and storage-agnostic, so the abstraction layer is clean — only the concrete implementations need to change.

The Apache Arrow ecosystem provides the `object_store` crate, which offers a unified `ObjectStore` trait with implementations for local filesystem, S3, GCS, and Azure. The `parquet` crate already has `ParquetObjectReader` for async reading from any `ObjectStore`. This is the same foundation used by DataFusion, Polars, and DuckDB.

## Goals / Non-Goals

**Goals:**
- Read Parquet and CSV from S3, GCS, Azure Blob Storage, and local filesystem through a unified interface
- Detect storage backend automatically from URI scheme (`s3://`, `gs://`, `abfss://`, plain path)
- Support per-table and global storage credential configuration in `arneb.toml`
- Maintain backward compatibility — existing local path configs work without changes
- Use async I/O for remote reads (no blocking the tokio runtime)

**Non-Goals:**
- Hive Metastore integration (separate change: `hive-connector`)
- Catalog trait async-ification (separate change: `async-catalog`)
- Partition-aware scanning or partition pruning
- Write support (INSERT INTO remote tables)
- ORC or Avro format support

## Decisions

### 1. Use `object_store` crate as the storage abstraction

**Choice**: Apache `object_store` crate with feature flags `aws`, `gcp`, `azure`.

**Alternatives considered**:
- **opendal**: More general (supports 40+ services) but heavier. `object_store` is better integrated with the Arrow/Parquet ecosystem we already use.
- **Custom trait**: Unnecessary — `object_store` is the de facto standard in the Rust data ecosystem.

**Rationale**: Direct integration with `parquet` crate's `ParquetObjectReader`. Same dependency used by DataFusion, Polars. Well-maintained under the Apache umbrella.

### 2. Async Parquet reading via `ParquetObjectReader`

**Choice**: Replace `std::fs::File` + sync `ParquetRecordBatchReader` with `ParquetObjectReader` + async `ParquetRecordBatchStreamBuilder`.

**Rationale**: Remote object stores require async I/O. `ParquetObjectReader` implements range-based reads, only fetching the bytes needed (footer, row groups). This avoids downloading entire files for projected scans.

### 3. URI scheme detection for storage backend selection

**Choice**: Parse the path/URI at table registration time to determine the `ObjectStore` implementation:

| URI prefix | Store |
|-----------|-------|
| `s3://` or `s3a://` | AmazonS3 |
| `gs://` | GoogleCloudStorage |
| `abfss://` or `az://` | MicrosoftAzure |
| `file://` or plain path | LocalFileSystem |

**Alternative**: Explicit `storage = "s3"` field in config. Rejected as primary mechanism because URI schemes are self-describing, but kept as optional override for edge cases (e.g., S3-compatible endpoints like MinIO).

### 4. Storage configuration in `arneb.toml`

**Choice**: Support both global `[storage]` section and per-table overrides:

```toml
[storage.s3]
region = "us-east-1"
endpoint = "https://s3.amazonaws.com"  # optional, for MinIO/localstack
# credentials: env vars AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or IAM role

[storage.gcs]
service_account_path = "/path/to/sa.json"  # optional, defaults to ADC

[[tables]]
name = "events"
path = "s3://my-bucket/data/events.parquet"
format = "parquet"
```

Per-table credential override is deferred to a later iteration. First version uses global storage config + URI-based backend detection.

### 5. Introduce a `StorageRegistry` for ObjectStore instance management

**Choice**: Create a `StorageRegistry` that caches `Arc<dyn ObjectStore>` instances by scheme+bucket, avoiding recreating clients for every table.

```
StorageRegistry
├── local:// → Arc<LocalFileSystem>
├── s3://my-bucket → Arc<AmazonS3>
├── s3://other-bucket → Arc<AmazonS3>
└── gs://analytics → Arc<GoogleCloudStorage>
```

**Rationale**: Object store clients are expensive to construct (credential resolution, HTTP client setup). Sharing them across tables in the same bucket is both efficient and necessary.

### 6. Lazy ObjectStore creation from config

**Choice**: `StorageRegistry::get_store()` lazy-creates cloud ObjectStore instances on cache miss using stored `StorageConfig`, rather than requiring all stores to be pre-registered at startup.

**Rationale**: HMS table locations (e.g., `s3://bucket/warehouse/db/table`) are discovered at query time via Thrift calls. The server cannot know which buckets to pre-register at startup. Lazy creation with caching gives the best of both worlds: no upfront knowledge needed, but clients are reused across queries to the same bucket.

### 7. Per-catalog StorageRegistry

**Choice**: Each catalog (file, hive) gets its own `StorageRegistry` instance, constructed with merged config: per-catalog `[catalogs.storage]` overrides global `[storage]`.

**Alternatives considered**:
- **Shared registry with scoped cache keys** (e.g., `catalog:s3://bucket`): More complex, risk of config conflicts when different catalogs target the same bucket with different credentials.

**Rationale**: Simpler isolation. Each `HiveConnectorFactory` already receives its own `StorageRegistry` reference. Per-catalog registries allow different S3 endpoints/credentials per catalog (e.g., production HMS vs staging HMS) without cache key conflicts.

## Risks / Trade-offs

- **[Large dependency footprint]** → `object_store` with cloud features adds compile-time dependencies (AWS SDK, etc.). Cloud object stores are a core capability of Arneb, so this is accepted as a necessary cost.
- **[Credential exposure]** → Storing credentials in `arneb.toml` is insecure. Mitigation: Prefer env vars and IAM roles. Config file only stores non-secret settings (region, endpoint). Document this clearly.
- **[CSV async limitation]** → The `arrow-csv` reader requires `std::io::Read`, not async. Mitigation: For remote CSV, download to a temporary buffer first via `object_store::ObjectStore::get()`, then parse. CSV files in data lakes are uncommon enough that this is acceptable.
- **[Breaking internal API]** → `ParquetDataSource::new()` currently takes `PathBuf`. Changing to `ObjectStore` + `Path` changes the internal API. Mitigation: Not a public API. Internal refactor only.
