## 1. Crate Setup and Dependency Choice

- [x] 1.1 Check `iceberg` (apache/iceberg-rust) 0.10.1 compatibility: it pins arrow/parquet 58 (the workspace is on 59), does I/O through OpenDAL, and needs MSRV 1.94. Rejected; decision recorded in design.md
- [x] 1.2 Create the `crates/iceberg/` workspace crate (`arneb-iceberg`) with `apache-avro` 0.22 and `flate2`. Confirm with `cargo tree -d` that there's no duplicate arrow, parquet, or object_store
- [x] 1.3 Add `arneb-iceberg` to the root workspace members and to the `hive` crate dependencies

## 2. Table Metadata

- [x] 2.1 Parse metadata JSON v1/v2 with serde (`schemas` + `current-schema-id` or legacy `schema`; snapshots; properties). Transparently gunzip gzip-compressed metadata
- [x] 2.2 Treat `current-snapshot-id` of `-1` or `null` as an empty table
- [x] 2.3 Map Iceberg types to Arneb/Arrow types. Hide `time` and nested columns with a warning
- [x] 2.4 Parse the name mapping (`schema.name-mapping.default`)
- [x] 2.5 Unit tests: v2 with a renamed and an added column, v1 without a snapshot, gzip, unknown format version, primitive types, name mapping

## 3. Manifests

- [x] 3.1 Decode manifest lists (v1 and v2 field names, `content`, file counts, provably-empty manifests)
- [x] 3.2 Decode manifest entries (status, content, path, format)
- [x] 3.3 Unit tests with Avro fixtures written by `apache-avro` (deflate codec), plus a corrupt-file error test

## 4. Shared Parquet Scan

- [x] 4.1 Move the Hive split scan into `arneb_connectors::parquet_scan` verbatim: `splits_per_file`, `split_index`, `open_parquet_builder`, `build_split_stream` (row-group pruning ∩ split slice, `RowFilter`, projection roots, batch size), `ParquetBatchStream` (with an optional per-batch adapter)
- [x] 4.2 `HiveDataSource::scan` calls the shared scan; drop the dead legacy `read_one_file`

## 5. Scan Planning and Data Source

- [x] 5.1 `plan_files`: manifest list → manifests (parallel fetch) → live data files. Skip tombstones
- [x] 5.2 Reject live position/equality delete files with a clear `UnsupportedOperation` error that suggests compaction
- [x] 5.3 Reject non-Parquet data files and legacy v1 snapshots without a manifest list
- [x] 5.4 `IcebergDataSource`: one partition per (file × split) through the shared scan
- [x] 5.5 Resolve columns by field ID: file-root projection, output rebuilt in requested order, null-fill for missing columns, cast for promoted types (per-batch adapter)
- [x] 5.6 Push filters into files by field ID: remap to file leaf indices only when physical and literal types match exactly
- [x] 5.7 `create_data_source`: load metadata once, read the pinned snapshot from table properties
- [x] 5.8 End-to-end tests over an in-memory object store: field-ID rename, int→long promotion, reordered physical columns, added null column, projection order, null-only projection, row-filter index remap, mismatched literal type, delete-file rejection, empty table

## 6. Hive Catalog Redirection

- [x] 6.1 Expose `HiveTableMeta.parameters`
- [x] 6.2 `HiveSchemaProvider::table()` returns `IcebergTableProvider::resolve(...)` for `table_type=ICEBERG` (one `is_iceberg_table` helper, one `TABLE_TYPE_PARAM` constant)
- [x] 6.3 `HiveConnectorFactory::create_data_source` delegates `table_type=ICEBERG` tables to `arneb_iceberg::create_data_source`, never listing their location
- [x] 6.4 `HiveCatalogProvider::new` takes the catalog's `StorageRegistry`
- [x] 6.5 Unit test: the factory redirects instead of listing the location

## 7. Engine and Server Wiring

- [x] 7.1 Pass the merged per-catalog storage registry to `HiveCatalogProvider` in `crates/server/src/main.rs`
- [x] 7.2 Propagate `create_data_source` errors in `protocol::handler::register_data_sources` and `server::task_manager::register_task_data_sources`

## 8. Local Environment

- [x] 8.1 `docker/trino/catalog/iceberg.properties` (Trino Iceberg catalog on the same HMS and MinIO, used to write the seed tables)
- [x] 8.2 `iceberg-seed` compose service plus `docker/iceberg-seed/seed.sh`, covering plain, identity-partitioned, schema-evolved, and delete-file tables

## 9. Validation

- [x] 9.1 Tier 1: `cargo fmt -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`
- [x] 9.2 Tier 2 (standalone): diff Trino and Arneb results on the seeded tables (SF1 orders) through a `hive` catalog
- [x] 9.3 Tier 2 (coordinator plus 2 workers): same diff
- [x] 9.4 Error path: a delete-file table fails with the unsupported-operation error
- [x] 9.5 Hive TPC-H SF1 22 queries vs Trino (standalone) to confirm the shared scan didn't change Hive results

## 10. Docs

- [x] 10.1 `docs/connectors/iceberg.md` and the VitePress sidebar entry
- [x] 10.2 Connector overview, configuration guide, Hive connector page, README, and CLAUDE.md mentions

## 11. Follow-ups (not in this change)

- [ ] 11.1 Manifest-level file pruning (column bounds, identity partitions) in `plan_files`, with a benchmark
- [ ] 11.2 Position/equality delete application (merge-on-read)
- [ ] 11.3 Time travel; REST/Glue/Nessie catalogs; ORC/Avro data files
