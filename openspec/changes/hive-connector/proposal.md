## Why

Arneb needs to query data stored in Hive-managed tables on cloud object stores. Hive Metastore (HMS) is the de facto metadata service for data lakes, managing table schemas, partition information, and file locations. Adding a Hive connector allows Arneb to dynamically discover and query tables registered in HMS without static configuration in `arneb.toml`.

## What Changes

- Add `hive_metastore` crate (by Xuanwo) for HMS Thrift client communication
- Create a new `crates/hive/` crate with `HiveCatalogProvider` and `HiveDataSource`
- `HiveCatalogProvider` maps HMS databases to schemas and HMS tables to TableProviders via async catalog traits
- `HiveDataSource` reads Parquet files from object stores based on HMS table metadata (storage location, format)
- Extend `arneb.toml` with `[[catalogs]]` section for configuring Hive Metastore connections
- Support per-catalog credential configuration for object store access
- First version: Parquet format only, no partition pruning

## Capabilities

### New Capabilities
- `hive-catalog`: Hive Metastore integration — dynamic table discovery, schema resolution, and file location mapping via HMS Thrift API
- `hive-data-source`: Reading Hive table data (Parquet) from object stores using HMS metadata for schema and storage location

### Modified Capabilities
- `server-config`: Configuration gains `[[catalogs]]` section for defining Hive Metastore connections with URI, warehouse location, and storage credentials

## Impact

- **New crate**: `crates/hive/` (workspace member)
- **New dependencies**: `hive_metastore` (Thrift client), `volo-thrift` (transitive)
- **Crates modified**: `server` (config, catalog wiring), `connectors` (StorageRegistry shared)
- **Depends on**: `object-store-support` (for reading files from S3/GCS/Azure) and `async-catalog` (for async CatalogProvider)
- **No breaking changes**: Hive connector is additive. Existing file-based tables unaffected.
