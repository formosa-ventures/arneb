## 1. Crate Setup and Dependency Choice

- [x] 1.1 Check `iceberg` (apache/iceberg-rust) 0.10.1 compatibility: it pins arrow/parquet 58 (the workspace is on 59), does I/O through OpenDAL, and needs MSRV 1.94. Rejected; decision recorded in design.md
- [x] 1.2 Create the `crates/iceberg/` workspace crate (`arneb-iceberg`) with `apache-avro` 0.22 and `flate2`. Confirm with `cargo tree -d` that there's no duplicate arrow, parquet, or object_store
- [x] 1.3 Add `arneb-iceberg` to the root workspace members and to the `server` crate dependencies

## 2. Table Metadata

- [x] 2.1 Parse metadata JSON v1/v2 (`schemas` + `current-schema-id` or legacy `schema`; `partition-specs` or legacy `partition-spec`; snapshots; properties). Transparently gunzip gzip-compressed metadata
- [x] 2.2 Treat `current-snapshot-id` of `-1` or `null` as an empty table
- [x] 2.3 Map Iceberg types to Arneb/Arrow types. Hide `time` and nested columns with a warning
- [x] 2.4 Parse the name mapping (`schema.name-mapping.default`)
- [x] 2.5 Unit tests: v2 with a renamed and an added column, v1 without a snapshot, gzip, unknown format version, primitive types, name mapping

## 3. Manifests

- [x] 3.1 Decode manifest lists (v1 and v2 field names, `content`, file counts, provably-empty manifests)
- [x] 3.2 Decode manifest entries (status, content, path, format, record count, partition tuple, lower/upper bounds in the `array<{key,value}>` and map encodings)
- [x] 3.3 Unit tests with Avro fixtures written by `apache-avro` (deflate codec), plus a corrupt-file error test

## 4. Scan Planning and Data Source

- [x] 4.1 `plan_files`: manifest list → manifests (parallel fetch) → live data files. Skip tombstones. Support inline manifests from legacy v1 snapshots
- [x] 4.2 Reject live position/equality delete files with a clear `UnsupportedOperation` error that suggests compaction
- [x] 4.3 Reject non-Parquet data files
- [x] 4.4 `IcebergDataSource`: one partition per (file × split), reusing the Hive split heuristic and helpers
- [x] 4.5 Resolve columns by field ID: projection mask over file roots, output rebuilt in requested order, null-fill for missing columns, cast for promoted types
- [x] 4.6 Push filters into files by field ID: remap to file leaf indices only when physical and literal types match exactly
- [x] 4.7 File pruning from manifest column bounds and identity-partition values (`=`, `<`, `<=`, `>`, `>=`, `AND`, `IN`, `BETWEEN`)
- [x] 4.8 `IcebergConnectorFactory`: read the pinned snapshot from table properties, cache parsed metadata by location
- [x] 4.9 End-to-end tests over an in-memory object store: field-ID rename, int→long promotion, reordered physical columns, added null column, projection order, null-only projection, bounds pruning, partition pruning, row-filter index remap, mismatched literal type, delete-file rejection, empty table, non-Iceberg table

## 5. Catalog

- [x] 5.1 `IcebergCatalogProvider` / `IcebergSchemaProvider` over `HmsClient`
- [x] 5.2 `IcebergTableProvider::resolve`: `metadata_location` → current schema, pinned `snapshot_id`, statistics from the snapshot summary
- [x] 5.3 Non-Iceberg HMS tables resolve to a provider that fails at scan time with "not an Iceberg table"
- [x] 5.4 Expose `HiveTableMeta.parameters`

## 6. Hive Catalog Safety

- [x] 6.1 `HiveTableProvider` forwards the HMS `table_type` as a property
- [x] 6.2 `HiveConnectorFactory` rejects `table_type=ICEBERG` with an error that names the `iceberg` catalog type
- [x] 6.3 Unit test for the rejection

## 7. Engine and Server Wiring

- [x] 7.1 Add `type = "iceberg"` to `[[catalogs]]` in `crates/server/src/main.rs` and document it in `config.rs`
- [x] 7.2 Propagate `create_data_source` errors in `protocol::handler::register_data_sources` and `server::task_manager::register_task_data_sources`

## 8. Local Environment

- [x] 8.1 `docker/trino/catalog/iceberg.properties` (Trino Iceberg catalog on the same HMS and MinIO)
- [x] 8.2 `iceberg-seed` compose service plus `docker/iceberg-seed/seed.sh`, covering plain, identity-partitioned, month-partitioned, schema-evolved, and delete-file tables

## 9. Validation

- [x] 9.1 Tier 1: `cargo fmt -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`
- [x] 9.2 Tier 2 (standalone): diff Trino and Arneb results on the seeded tables (SF1 orders)
- [x] 9.3 Tier 2 (coordinator plus 2 workers): same diff. Confirm the workers plan the pinned snapshot and prune files (`arneb_iceberg=debug`)
- [x] 9.4 Error paths: a delete-file table, an Iceberg table via the `hive` catalog, a Hive table via the `iceberg` catalog. Confirm that compaction (`EXECUTE optimize`) makes the delete-file table readable

## 10. Docs

- [x] 10.1 `docs/connectors/iceberg.md` and the VitePress sidebar entry
- [x] 10.2 Connector overview, configuration guide, README, and CLAUDE.md mentions
