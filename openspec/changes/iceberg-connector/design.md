## Context

Arneb already has most of what an Iceberg reader needs:

- An HMS 4.x Thrift client (`crates/hive-metastore` plus `arneb_hive::catalog::HmsClient`).
- `StorageRegistry` over `object_store` 0.13 (S3, GCS, Azure, and local).
- A tuned Parquet scan: row-group min/max pruning, `RowFilter` predicate pushdown, projection pushdown, row-range intra-file splits, and a streaming batch adapter.

The Iceberg-specific work is:

1. Find the table's metadata file.
2. Turn the current snapshot into a list of live data files.
3. Read those files with Iceberg column semantics (field IDs, not names).
4. Refuse table states that would produce wrong answers if handled naively.

Trino tables in HMS carry `table_type=ICEBERG` and `metadata_location=s3://.../metadata/NNNNN-<uuid>.metadata.json`. The storage descriptor still has a `location` and a list of columns. That's why the Hive connector would "work" on these tables and return wrong data if it treated them as plain Hive tables.

## Goals / Non-Goals

**Goals**

- Read HMS-backed Iceberg tables (format v1 and v2), current snapshot, Parquet data files.
- Get schema evolution right: renamed, added, dropped, and reordered columns, plus the type promotions `int→long`, `float→double`, and wider decimals.
- Push filters and projections down into each data file.
- Stay consistent in distributed mode: every node reads the same snapshot.
- Never return wrong results for table states we don't support.
- Keep the Hive scan (the TPC-H hot path) byte-identical: share it, don't fork it.

**Non-Goals (follow-ups)**

- Applying position or equality delete files (merge-on-read). These tables are rejected with a clear error.
- Manifest-level file pruning (column bounds, partition values). See decision 6.
- Writes: `INSERT`, `CTAS`, `DELETE`, `MERGE`, `OPTIMIZE`.
- Time travel (`FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`).
- REST, Glue, Nessie, and JDBC catalogs.
- ORC and Avro data files; legacy v1 snapshots that list manifests inline instead of through a manifest list.
- Nested types (`struct`, `list`, `map`) and `time`. These columns are hidden with a warning, which matches the Hive connector.
- Metadata or manifest caching across queries.

## How a query reads an Iceberg table

1. **Resolve** – the Hive catalog gets the table from HMS. When its parameters say `table_type=ICEBERG`, it redirects to `IcebergTableProvider::resolve`, which reads the `metadata_location` JSON (format v1 or v2, gzip or plain) and exposes the table's **current schema**. The HMS storage-descriptor columns are ignored because they carry no field IDs and can be stale.
2. **Pin a snapshot** – the current snapshot ID becomes a table property. It travels with the plan, so the coordinator and every worker read the same snapshot, even if a writer commits while the query runs.
3. **Plan files** – `HiveConnectorFactory::create_data_source` sees `table_type=ICEBERG` in the properties and delegates to `arneb_iceberg::create_data_source`, which loads the metadata and reads the snapshot's manifest list and manifests (Avro) to get the live data files. Tombstoned entries are skipped; manifests are fetched in parallel.
4. **Scan** – each data file goes through the shared split-based Parquet scan (`arneb_connectors::parquet_scan`): row-group pruning, `RowFilter` pushdown, projection pushdown, and intra-file splits. Iceberg only supplies the file-root projection, the filters remapped by field ID, and a per-batch adapter that casts promoted columns and null-fills added ones.

## Decisions

### 1. A native reader, not the `iceberg` crate

**Choice**: Implement a minimal reader: metadata JSON, manifest-list and manifest Avro decoding, then Arneb's existing Parquet scan.

**Alternative considered: `iceberg` (apache/iceberg-rust)**. The latest release, 0.10.1, doesn't fit:

- It depends on `arrow-*` **58** and `parquet` **58**. Arneb is on arrow/parquet **59**, so pulling it in would put two copies of Arrow in the tree. That means duplicate compile cost and binary size, and Arrow arrays that can't cross the boundary without conversion.
- Its file I/O goes through OpenDAL, not `object_store`, so it wouldn't share `StorageRegistry`'s per-catalog credentials and endpoints.
- Its MSRV is 1.94, while the workspace declares 1.89.
- Its `ArrowReader` would replace Arneb's tuned Parquet path (splits, `RowFilter` pushdown, and the batch-size policy). We'd lose that work or have to re-plumb it.

The subset we need is small and stable across spec versions: metadata JSON and two Avro record shapes. Reading Avro with the *writer* schema into generic values and extracting fields by name handles the v1/v2 differences (such as `added_data_files_count` versus `added_files_count`, and the missing `content` field in v1) without code generation. When iceberg-rust catches up to arrow 59 and `object_store`, we can revisit this, especially for REST catalogs and delete-file application.

### 2. Table redirection inside the Hive catalog

**Choice**: There is no separate catalog type. A `type = "hive"` catalog serves both kinds of table from the same metastore: `HiveSchemaProvider::table()` returns an `IcebergTableProvider` for `table_type=ICEBERG` tables, and `HiveConnectorFactory::create_data_source` delegates those tables to the Iceberg reader. This is Trino's table redirection (`hive.iceberg-catalog-name`), without the second catalog.

**Alternative considered: a separate `type = "iceberg"` catalog** with strict cross-rejection (Hive rejects Iceberg tables, Iceberg rejects Hive tables). It needed a duplicate catalog/schema provider over the same `HmsClient`, an error-marker table property (the catalog API can't return an error from `table()`), two rejection paths, and a config branch — all to make users configure the same metastore twice. Redirection removes that code and the configuration step, and it closes the same correctness hole: an Iceberg table is never read by listing its location.

**Crate layout**: `arneb-hive` depends on `arneb-iceberg` (the redirect target). The shared Parquet scan therefore lives in `arneb-connectors` (`parquet_scan`), below both. It was moved there verbatim from `crates/hive`, so the Hive scan behaves exactly as before.

**Errors**: `create_data_source` failures (delete files, non-Parquet files) now propagate in `protocol::handler::register_data_sources` and `server::task_manager::register_task_data_sources`. Before, those errors were swallowed and the query later failed with a generic "data source not found" error.

### 3. Pin the snapshot at planning time and carry it in the plan

`IcebergTableProvider::properties()` returns `table_type=ICEBERG`, `metadata_location`, and `snapshot_id`. `LogicalPlan::TableScan` carries these properties to workers. Each node loads the metadata once in `create_data_source`, then plans files for **that** snapshot ID. A commit that lands mid-query can't make the coordinator and workers disagree. Metadata files are immutable, so a cache keyed by location would be safe, but it isn't needed yet (one small JSON read per node per scan).

### 4. Scan planning

- Manifest list: skip manifests whose counts prove they're empty (`added == existing == 0`). Fetch the rest with `buffered(8)`, which keeps the order deterministic.
- Entries with `status = DELETED` are tombstones and are skipped.
- Delete manifests (`content = 1`), or data files with `content ≠ 0`, that have any live entry fail the query with `UnsupportedOperation`. The message names the kind (position or equality), the snapshot, an example file, and how to fix it (compact with `optimize` / `rewrite_data_files`). Checking entries instead of snapshot-summary counters means a table whose delete files were all compacted away reads normally.
- A `file_format` other than `PARQUET` fails the query the same way.
- A snapshot without a manifest list (legacy v1 inline `manifests`) fails with `UnsupportedOperation`; no current writer produces those.

### 5. Resolving columns by field ID

For each data file, `FileColumnMap` reads the Parquet schema's root fields and their `field_id`s, and builds a map of `field_id → root index` and `root index → leaf index` (for primitive roots). Then:

- **Projection** becomes the set of file roots that are present, passed to the shared scan. The output batch is rebuilt in the requested order by the per-batch adapter. Columns missing from the file (added later) become `new_null_array`. Columns whose physical type differs from the table type (`int` stored where the table now has `long`, or a narrower decimal) are cast with `arrow::compute::cast`.
- **Filters** use table column indices, the same as the Hive connector. They're rewritten to file leaf indices (`remap_filter`). A filter is pushed into a file only when every column it references is present, is primitive, and has a physical type equal to the table type, and every literal has exactly that type (`literal_types_match`). The shared Parquet predicate kernels compare arrays directly and would otherwise fail on mismatched types, such as Int64 against Int32. Unsupported expression shapes aren't pushed. `FilterExec` always re-applies the predicate above the scan, so this only affects performance, never correctness.
- **No field IDs in the file** (for example, a table migrated from Hive): the reader falls back to the `schema.name-mapping.default` property, and then to current column names.

### 6. No manifest-level file pruning (yet)

Parquet row-group pruning and `RowFilter` pushdown already guarantee correctness and skip most of the I/O inside each file; what manifest pruning adds is skipping the footer read of files that can't match. That matters for tables with many small files, and it's a real follow-up, but it should be done **once in `plan_files`** (drop files from the planned list, so they don't become scan partitions at all), not per split at scan time. It needs single-value bound decoding for every type plus conservative handling of casts, floats, and truncated bounds, so it's worth its own change with its own benchmark.

### 7. Type mapping

Iceberg types map to the Arrow types the Parquet reader already produces, so the common path doesn't cast: `timestamptz → Timestamp(µs, "+00:00")`, `decimal(P,S) → Decimal128(P,S)`, and `uuid`/`fixed`/`binary → Binary`. `time` and nested types have no `arneb_common::DataType` equivalent yet. Those columns are hidden with a warning.

### 8. Statistics

`TableStatistics` comes from the snapshot summary (`total-records`, `total-files-size`), so the cost-based planner sees real sizes. An empty table (no snapshot) reports exactly 0 rows.

## Risks / Trade-offs

- **Filter pushdown reach (pre-existing engine behavior).** The physical planner pushes filters into `ScanContext` only when the `Filter` sits directly on the `TableScan`. This affects the Hive connector's row-group pruning the same way. Fixing it is a separate, perf-validated change.
- **Planning cost.** Every scan reads the metadata JSON, the manifest list, and the manifests on each node, with no cache across queries. That's fine for tables with tens to hundreds of manifests. Caching by path (all of them are immutable) is a follow-up if it shows up in profiles.
- **Delete files.** Tables with merge-on-read deletes are unreadable until they're compacted. This is deliberate. Wrong answers are worse than an error.
- **Resolution errors surface as "table not found".** `SchemaProvider::table()` returns `Option`, so a broken metadata file is logged as a warning and the table resolves to nothing. Same as any HMS lookup failure today.
