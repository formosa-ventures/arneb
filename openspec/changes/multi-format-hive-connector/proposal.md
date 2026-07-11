## Why

Arneb's Hive connector only reads Parquet today, but real-world Hive tables commonly use multiple storage formats (CSV, JSON, ORC, Avro). HMS already tells us each table's `InputFormat`, and PR #30 exposes it through `HiveTableMeta.input_format`, yet `HiveDataSource` hard-codes Parquet and ignores that field. Any Hive table stored in another format fails to load. Adding a format dispatcher unlocks the bulk of production Hive deployments and makes arneb a usable Trino alternative on existing data lakes.

## What Changes

- Introduce a `FileFormat` value on every `HiveTableMeta`, parsed from the HMS `InputFormat` className (TextInputFormat, MapredParquetInputFormat, OrcInputFormat, AvroInputFormat, JSON SerDe). Unknown or unsupported formats surface a clear error rather than a silent Parquet read.
- Make `HiveDataSource` format-aware: it selects the appropriate reader per table (Parquet today, plus CSV and JSON reusing `crates/connectors/src/file.rs`; ORC and Avro via new `OrcDataSource` / `AvroDataSource` built on the existing `arrow` crate readers).
- Forward SerDe parameters from HMS (`SerDeInfo.parameters` — e.g. CSV `field.delim`, `skip.header.line.count`, JSON serde column mappings) into the reader so that Hive-written files parse correctly.
- Extend the TPC-H benchmark seed to optionally materialise CSV / ORC / Avro variants so correctness against Trino can be verified across formats.
- Documentation update in `docs/connectors/hive.md` listing supported formats and SerDe options.

## Capabilities

### New Capabilities

- None. This change extends an existing capability rather than adding a new one. New reader implementations (`OrcDataSource`, `AvroDataSource`) live under the existing `file-connector` capability.

### Modified Capabilities

- `hive-data-source`: today the requirement is "read Parquet files from object store for a Hive table"; the requirement becomes "read files in the format declared by the table's InputFormat, dispatching across Parquet / CSV / JSON / ORC / Avro". Error contract for unsupported formats is added.
- `file-connector`: today `FileFormat` covers Parquet + CSV with two corresponding readers. The requirement expands to include JSON, ORC, and Avro reader variants selectable through `ConnectorFactory`.

## Impact

- **Code**
  - `crates/hive/src/catalog.rs` — add `HiveTableProvider::format() -> Result<FileFormat>`, replacing `is_parquet()`.
  - `crates/hive/src/datasource.rs` — dispatcher selects reader by `FileFormat`; SerDe params forwarded.
  - `crates/connectors/src/file.rs` — `FileFormat::{Orc, Avro, Json}` variants + corresponding `DataSource` impls.
  - `crates/connectors/Cargo.toml` — add arrow-orc / arrow-avro / arrow-json feature flags (or equivalent crates).
- **APIs**
  - `HiveTableProvider::is_parquet()` removed. Callers within the arneb workspace move to `format()`. Not public outside the workspace, so no breaking external API.
- **Benchmarks / infra**
  - `docker/tpch-seed/seed.sh` gains an optional `TPCH_FORMAT` env var (default `PARQUET`) for multi-format seeding.
- **Tests**
  - Unit: InputFormat className → FileFormat mapping table.
  - Integration: seed each format, scan each, compare row counts with Trino.
- **Dependencies**
  - Add `arrow-orc`, `arrow-avro`, `arrow-json` (or equivalent readers already in the `arrow` crate feature set).
- **Out of scope / non-goals**
  - Iceberg / Delta / Hudi table formats (handled through different metadata layers; separate change).
  - Write path (INSERT INTO Hive). Current connector is read-only.
