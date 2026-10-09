## Why

Trino's Hive connector writes ORC by default (`hive.storage-format=ORC`), so a
table created with a plain `CREATE TABLE ... AS SELECT` in Trino is ORC. Arneb's
Hive connector only reads Parquet and fails on these tables with
`Parquet reader error ...: Corrupt footer`. Reading ORC unlocks the most common
Trino-written Hive tables. This is the ORC slice of the broader
`multi-format-hive-connector` proposal, done without its reader abstraction.

## What Changes

- Detect the file format from the HMS storage descriptor: `OrcInputFormat` /
  `OrcSerde` → ORC, Parquet classes (or none) → Parquet, anything else → a clear
  "unsupported Hive storage format" error instead of a misread.
- Read ORC files with the `orc-rust` crate through the existing Hive data source:
  same file listing, one scan partition per file split, projection, filters left
  to the `FilterExec` above the scan. A small `AsyncChunkReader` adapter does
  ranged GETs on the object store.
- Map ORC columns to HMS columns by name, case-insensitively; old Hive
  `_col0, _col1, ...` files by position. Missing columns read as NULL; Hive
  widenings (int→bigint, float→double, int→decimal/double, decimal
  precision/scale) are cast; any other type mismatch is an error naming the column.
- Hive partitioned tables (both formats): partition key columns are appended to
  the table schema and filled from the `key=value` directories (Hive path
  unescaping, `__HIVE_DEFAULT_PARTITION__` → NULL). Hidden directories
  (`.trino-staging`, `_temporary`) are skipped.
- Hive ACID tables are rejected with a clear error: `transactional=true` in HMS,
  `base_N` / `delta_N_M` directories, or ACID-layout ORC files.
- The table-schema batch adapter (`ColumnSource`, `adapt_batch`, `remap_filter`)
  moves from the Iceberg crate to `arneb_connectors::scan_adapter` and is shared
  by Iceberg, Hive ORC and partitioned Hive Parquet.

## Capabilities

### Modified Capabilities

- `hive-data-source`: reads ORC as well as Parquet, reads partitioned tables, and
  rejects ACID tables and unsupported formats.

## Impact

- **Code**: `crates/hive/src/{orc.rs (new), datasource.rs, catalog.rs}`,
  `crates/connectors/src/scan_adapter.rs` (new, moved from
  `crates/iceberg/src/datasource.rs`).
- **Dependencies**: `orc-rust`, temporarily from a git fork pinned to a commit
  (`jackypan1989/orc-rust@ba7af4c`) because the released 0.9.0 is on arrow 59;
  upstream PR datafusion-contrib/orc-rust#91 bumps it to arrow 60. Switch to the
  crates.io release once published and remove the `deny.toml` `allow-git` entry.
  New duplicate (warn-only) crates: `lz4_flex`, `zstd`, `prost`.
- **Non-goals**: ORC predicate pushdown (row-index pruning), partition pruning,
  ORC writes, Hive ACID reads, complex types (already unsupported at the HMS
  type-mapping layer), other formats (text, Avro, RCFile).
