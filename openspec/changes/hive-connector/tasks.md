## 1. Crate Setup

- [x] 1.1 Create `crates/hive/` workspace crate with `Cargo.toml` (deps: `hive_metastore`, `arneb-common`, `arneb-catalog`, `arneb-connectors`, `object_store`, `arrow`, `parquet`, `async-trait`, `tokio`, `tracing`)
- [x] 1.2 Add `arneb-hive` to workspace members in root `Cargo.toml`
- [x] 1.3 Add `arneb-hive` dependency to `crates/server/Cargo.toml`

## 2. HMS Type Mapping

- [x] 2.1 Implement Hive type string → Arrow DataType mapping (INT, BIGINT, STRING, DOUBLE, BOOLEAN, DATE, TIMESTAMP, FLOAT, SMALLINT, TINYINT, DECIMAL)
- [x] 2.2 Handle unsupported types (MAP, ARRAY, STRUCT) with descriptive errors
- [x] 2.3 Write unit tests for type mapping

## 3. HiveCatalogProvider

- [x] 3.1 Implement HMS Thrift client wrapper with connection management
- [x] 3.2 Implement `HiveCatalogProvider` — async `CatalogProvider` that calls `get_all_databases()` and `get_database()`
- [x] 3.3 Implement `HiveSchemaProvider` — async `SchemaProvider` that calls `get_all_tables()` and `get_table()`
- [x] 3.4 Implement `HiveTableProvider` — converts HMS table columns to `Vec<ColumnInfo>` via type mapping
- [x] 3.5 Write unit tests with mocked HMS responses

## 4. HiveDataSource

- [x] 4.1 Implement `HiveDataSource` that reads `sd.location` from HMS table metadata
- [x] 4.2 Implement file listing: list `.parquet` files at table location via ObjectStore
- [x] 4.3 Implement multi-file reading: scan all Parquet files and combine into a single RecordBatchStream
- [x] 4.4 Support projection pushdown through to underlying Parquet reads
- [x] 4.5 Add schema validation: Parquet file schema vs HMS table schema
- [x] 4.6 Write integration tests with local Parquet files and mocked HMS metadata

## 5. HiveConnectorFactory

- [x] 5.1 Implement `HiveConnectorFactory` that creates `HiveDataSource` instances using StorageRegistry + HMS metadata
- [x] 5.2 Register Hive connector in `ConnectorRegistry`

## 6. Configuration & Server Wiring

- [x] 6.1 Extend `AppConfig` with `[[catalogs]]` section parsing (name, type, metastore_uri, default_schema, storage)
- [x] 6.2 Wire Hive catalog registration into server startup in `main.rs`
- [x] 6.3 Support per-catalog storage config falling back to global `[storage]`
- [x] 6.4 Write config parsing tests for Hive catalog entries

## 7. End-to-End Validation

- [x] 7.1 Write integration test: register a Hive catalog with mocked HMS, query a table, verify results
- [x] 7.2 Update CLAUDE.md with Hive connector configuration examples

## 8. Integration Testing

- [x] 8.1 Server-level integration test: manually inject Hive metadata (bypass HMS Thrift) + InMemory ObjectStore + pgwire query verification (no Docker required, runs in CI)
- [x] 8.2 E2E showcase: `docker-compose.yml` at repo root (HMS standalone + MinIO), test script (`scripts/hive-e2e-test.sh`), update CLAUDE.md with E2E instructions

## 9. Query Path Wiring (discovered gap)

- [x] 9.1 Fix `register_data_sources()` in `crates/protocol/src/handler.rs`: pass `HiveTableProvider` location to `HiveConnectorFactory::register_table_location()` during query execution so the production path works end-to-end (currently location info is discarded after planning)
