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

Trino tables in HMS carry `table_type=ICEBERG` and `metadata_location=s3://.../metadata/NNNNN-<uuid>.metadata.json`. The storage descriptor still has a `location` and a list of columns. That's why the existing Hive connector would "work" on these tables and return wrong data.

## Goals / Non-Goals

**Goals**

- Read HMS-backed Iceberg tables (format v1 and v2), current snapshot, Parquet data files.
- Get schema evolution right: renamed, added, dropped, and reordered columns, plus the type promotions `int→long`, `float→double`, and wider decimals.
- Push filters and projections down, and prune files from manifest metadata.
- Stay consistent in distributed mode: every node reads the same snapshot.
- Never return wrong results for table states we don't support.

**Non-Goals (follow-ups)**

- Applying position or equality delete files (merge-on-read). These tables are rejected with a clear error.
- Writes: `INSERT`, `CTAS`, `DELETE`, `MERGE`, `OPTIMIZE`.
- Time travel (`FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`).
- REST, Glue, Nessie, and JDBC catalogs.
- ORC and Avro data files.
- Nested types (`struct`, `list`, `map`) and `time`. These columns are hidden with a warning, which matches the Hive connector.
- Manifest and metadata caching across queries (only parsed metadata is cached, keyed by its immutable location).

## Decisions

### 1. A native reader, not the `iceberg` crate (Option B)

**Choice**: Implement a minimal reader: metadata JSON, manifest-list and manifest Avro decoding, then Arneb's existing Parquet scan.

**Alternative considered: `iceberg` (apache/iceberg-rust)**. The latest release, 0.10.1, doesn't fit:

- It depends on `arrow-*` **58** and `parquet` **58**. Arneb is on arrow/parquet **59**, so pulling it in would put two copies of Arrow in the tree. That means duplicate compile cost and binary size, and Arrow arrays that can't cross the boundary without conversion.
- Its file I/O goes through OpenDAL, not `object_store`, so it wouldn't share `StorageRegistry`'s per-catalog credentials and endpoints.
- Its MSRV is 1.94, while the workspace declares 1.89.
- Its `ArrowReader` would replace Arneb's tuned Parquet path (splits, `RowFilter` pushdown, and the batch-size policy). We'd lose that work or have to re-plumb it.

The subset we need is small and stable across spec versions: metadata JSON, two Avro record shapes, and single-value bound serialization. Reading Avro with the *writer* schema into generic values and extracting fields by name handles the v1/v2 differences (such as `added_data_files_count` versus `added_files_count`, and the missing `content` field in v1) without code generation. When iceberg-rust catches up to arrow 59 and `object_store`, we can revisit this, especially for REST catalogs and delete-file application.

### 2. A separate `type = "iceberg"` catalog, and strict rejection across types

**Choice**: Iceberg tables are served by their own catalog type, pointed at the same HMS. The Hive catalog rejects `table_type=ICEBERG` tables, and the Iceberg catalog rejects everything else. Both errors name the right catalog type.

**Alternative**: transparent redirection, where the Hive catalog serves Iceberg tables too. That's more convenient, but it couples the crates in both directions (`arneb-iceberg` already depends on `arneb-hive`). Trino's default is also separate catalogs with redirection off, which is what migrating users already have configured.

The catalog API (`SchemaProvider::table() -> Option<_>`) can't return an error, and the planner turns every resolution failure into "table not found". So the rejection is carried as a table property (`iceberg_error`, or `table_type` for Hive), and the connector factory turns it into a `ConnectorError::UnsupportedOperation`. To make that error reach the client, `create_data_source` failures now propagate in `protocol::handler::register_data_sources` and in `server::task_manager::register_task_data_sources`. Before this change, those errors were swallowed, and the query later failed with a generic "data source not found" error.

### 3. Pin the snapshot at planning time and carry it in the plan

`IcebergTableProvider::properties()` returns `metadata_location`, `snapshot_id`, and `table_type=ICEBERG`. `LogicalPlan::TableScan` carries these properties to workers. Each node's `IcebergConnectorFactory` loads the metadata (cached by location, since metadata files are immutable), then plans files for **that** snapshot ID. A commit that lands mid-query can't make the coordinator and workers disagree.

### 4. Scan planning

- Manifest list: skip manifests whose counts prove they're empty (`added == existing == 0`). Fetch the rest with `buffered(8)`, which keeps the order deterministic.
- Entries with `status = DELETED` are tombstones and are skipped.
- Delete manifests (`content = 1`), or data files with `content ≠ 0`, that have any live entry fail the query with `UnsupportedOperation`. The message names the kind (position or equality), the snapshot, an example file, and how to fix it (compact with `optimize` / `rewrite_data_files`). Checking entries instead of snapshot-summary counters means a table whose delete files were all compacted away reads normally.
- A `file_format` other than `PARQUET` fails the query the same way.
- Legacy v1 snapshots that list `manifests` inline, with no manifest list, are supported.

### 5. Resolving columns by field ID

For each data file, `FileColumnMap` reads the Parquet schema's root fields and their `field_id`s, and builds a map of `field_id → root index` and `root index → leaf index` (for primitive roots). Then:

- **Projection** becomes `ProjectionMask::roots` over the file roots that are present. The output batch is rebuilt in the requested order. Columns missing from the file (added later) become `new_null_array`. Columns whose physical type differs from the table type (`int` stored where the table now has `long`, or a narrower decimal) are cast with `arrow::compute::cast`.
- **Filters** use table column indices, the same as the Hive connector. They're rewritten to file leaf indices. A filter is pushed into a file only when every column it references is present, is primitive, and has a physical type equal to the table type, and every literal has exactly that type. The shared Parquet predicate kernels compare arrays directly and would otherwise fail on mismatched types, such as Int64 against Int32. Unsupported expression shapes aren't pushed. `FilterExec` always re-applies the predicate above the scan, so this only affects performance, never correctness.
- **No field IDs in the file** (for example, a table migrated from Hive): the reader falls back to the `schema.name-mapping.default` property, and then to current column names.

### 6. File pruning from manifests

`pruning::file_can_be_skipped` evaluates the pushed conjuncts against a range per column, built from:

- manifest `lower_bounds` / `upper_bounds`, decoded from single-value serialization. That's little-endian int/long/date/timestamp values (including 4-byte bounds on a column promoted to `long`), big-endian two's-complement decimals, and UTF-8 strings.
- **identity** partition values, which give an exact `[v, v]` range.

It handles `=`, `<`, `<=`, `>`, `>=` (with the literal on either side), `AND`, `IN (literals)`, and `BETWEEN`. It deliberately does *not* prune on `OR`, `NOT`, casts, float and double columns (NaN ordering), a naive timestamp literal against a zoned column (or the reverse), or literals that can't be converted exactly into the column's unit or scale. Truncated string bounds stay sound: Iceberg truncates the lower bound (smaller) and truncates-and-increments the upper bound (larger). Non-identity transforms aren't evaluated directly. For `day`/`month`/`year`-partitioned tables, the column bounds on the source column already give the same pruning.

### 7. Type mapping

Iceberg types map to the Arrow types the Parquet reader already produces, so the common path doesn't cast: `timestamptz → Timestamp(µs, "+00:00")`, `decimal(P,S) → Decimal128(P,S)`, and `uuid`/`fixed`/`binary → Binary`. `time` and nested types have no `arneb_common::DataType` equivalent yet. Those columns are hidden with a warning.

### 8. Statistics

`TableStatistics` comes from the snapshot summary (`total-records`, `total-files-size`), so the cost-based planner sees real sizes. An empty table (no snapshot) reports exactly 0 rows.

## Risks / Trade-offs

- **Filter pushdown reach (pre-existing engine behavior).** The physical planner pushes filters into `ScanContext` only when the `Filter` sits directly on the `TableScan`, and only the innermost conjunct. It doesn't push them when projection pushdown puts a `Projection` in between, or on the aggregate column-pruning path. So manifest-based file pruning applies to fewer query shapes than it could. This affects the Hive connector's row-group pruning the same way. Fixing it is a separate, perf-validated change.
- **Planning cost.** Every query reads the manifest list and manifests, with no cache across queries. That's fine for tables with tens to hundreds of manifests. Caching them by path (they're immutable) is a follow-up.
- **Delete files.** Tables with merge-on-read deletes are unreadable until they're compacted. This is deliberate. Wrong answers are worse than an error.
