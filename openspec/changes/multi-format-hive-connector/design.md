## Context

Hive tables in production warehouses are rarely uniform. Textfile/CSV remains the on-boarding default, ORC is the Hive-native columnar format, Parquet is the Spark-native columnar format, Avro is common for log ingestion, and JSON is frequent for schemaless sources. HMS already tracks this via each table's `StorageDescriptor.inputFormat` className. PR #30 added `HiveTableMeta.input_format: String` (`crates/hive/src/catalog.rs:142`) — but the consumer (`HiveDataSource` at `crates/hive/src/datasource.rs`) ignores it and always instantiates a Parquet reader. Any non-Parquet table fails with parquet footer errors.

The connector crate (`crates/connectors/src/file.rs`) already models format diversity through the `FileFormat` enum and paired `CsvDataSource` / `ParquetDataSource` implementations. The gap is (a) mapping Hive InputFormat strings to `FileFormat`, (b) teaching `HiveDataSource` to dispatch on it, and (c) adding ORC / Avro / JSON reader variants (all available through the `arrow` crate).

Constraints:
- Read-only: arneb doesn't write Hive tables; no INSERT/UPDATE path to touch.
- Format selection is per-table, derived entirely from HMS metadata. Users don't configure it in arneb.toml.
- SerDe parameters (e.g. CSV `field.delim`, `skip.header.line.count`) live in HMS `SerDeInfo.parameters` and must flow to the reader.
- Arneb already uses `arrow` v54 and `parquet` v58; ORC / Avro / JSON support lives in separate arrow-family crates (`arrow-json` is already transitively available; `arrow-avro` and `arrow-orc` need opt-in).

## Goals / Non-Goals

**Goals:**
- Dispatch Hive table reads to the correct reader based on HMS InputFormat.
- Reuse `file.rs` readers wherever possible; new readers share the same `DataSource` trait.
- Forward SerDe params into readers so Hive-written files (with real delimiters, header rows, etc.) parse identically to Trino.
- Unsupported formats produce a clear error naming the InputFormat className, not a silent misread.
- TPC-H benchmarks runnable against at least TEXTFILE + PARQUET variants and match Trino byte-for-byte.

**Non-Goals:**
- Table formats (Iceberg, Delta, Hudi). Those are metadata-layer protocols living on top of a file format; they require their own catalog connector, not just a dispatcher.
- Write path. Hive INSERT/CTAS/DELETE are out of scope until arneb grows a write planner.
- Custom SerDe implementations. We honour built-in Hive SerDes (LazySimpleSerDe, OpenCSVSerde, ParquetHiveSerDe, OrcSerde, AvroSerDe, JsonSerDe); user-defined SerDes fail fast.
- Partition-format heterogeneity (one table with mixed Parquet + CSV partitions). HMS allows it per-partition via `Partition.sd.inputFormat`, but we read the table-level format uniformly in v1 and note it as future work.

## Decisions

### Decision 1: Map by InputFormat className, not SerDe className

HMS carries both `sd.inputFormat` and `sd.serdeInfo.serializationLib`. We key on InputFormat because:
- Trino does the same (`HiveStorageFormat` enum in Trino).
- InputFormat uniquely determines the on-disk byte layout; SerDe governs row interpretation but multiple SerDes can share an InputFormat (e.g. both `LazySimpleSerDe` and `OpenCSVSerde` use `TextInputFormat`).

Known mapping (subset; full table lives in code as a const `InputFormat → FileFormat` lookup):

| InputFormat className                                                    | FileFormat |
|--------------------------------------------------------------------------|------------|
| `org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat`          | Parquet    |
| `org.apache.hadoop.mapred.TextInputFormat`                                | Csv / Json (tie-breaker below) |
| `org.apache.hadoop.hive.ql.io.orc.OrcInputFormat`                         | Orc        |
| `org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat`             | Avro       |

Tie-breaker for `TextInputFormat`: inspect `sd.serdeInfo.serializationLib`. `JsonSerDe` (`org.apache.hive.hcatalog.data.JsonSerDe` or `org.openx.data.jsonserde.JsonSerDe`) → JSON; otherwise → CSV with SerDe params controlling delimiter.

**Alternative considered:** Key on SerDe className. Rejected because SerDe-driven dispatch needs InputFormat anyway to know the byte layout, doubling the lookup surface.

### Decision 2: Add new `FileFormat` variants instead of a separate `HiveFileFormat`

`crates/connectors/src/file.rs` already owns `FileFormat::{Csv, Parquet}` and the corresponding `DataSource`s. Adding `Orc / Avro / Json` variants there (vs. creating a parallel `HiveFileFormat` enum inside the hive crate) keeps readers reusable from any future connector and avoids translating between two enums.

**Trade-off:** `file.rs` becomes longer, and the `FileConnectorFactory` (used for local file tables) inherits format variants it doesn't necessarily advertise. Mitigation: `FileConnectorFactory` only exposes formats the user explicitly registers; the enum is format-agnostic.

### Decision 3: Pass SerDe params via `HashMap<String, String>`

`ConnectorFactory::create_data_source()` already accepts `properties: HashMap<String, String>` (`crates/connectors/src/traits.rs:50-55`). Use the same shape for Hive: `HiveTableProvider::create_data_source()` passes `sd.serdeInfo.parameters` (merged with table properties) downstream. New readers read the keys they care about (e.g. `CsvDataSource` reads `field.delim`, `skip.header.line.count`, `escape.delim`; JSON reader reads column mapping).

**Alternative considered:** A strongly-typed config struct per format. Rejected as premature typing — Hive's SerDe param space is open (custom SerDes add their own keys), and a string map matches HMS and Trino's representation.

### Decision 4: Fail loudly on unsupported InputFormat

When the InputFormat className is not in the lookup (or is a recognised-but-custom SerDe), `HiveDataSource::scan()` returns `ConnectorError::UnsupportedFormat { input_format, table }` rather than attempting Parquet. This prevents silent corruption and gives operators an actionable error.

**Trade-off:** Loses the current "Parquet-or-bust" behaviour. Tables that happened to work by accident (Parquet written under a different InputFormat string) will start erroring. Mitigation: include the observed className in the error so users can raise issues or work around via `ALTER TABLE ... SET FILEFORMAT PARQUET` in Hive.

### Decision 5: Add `arrow-orc` / `arrow-avro` as opt-in deps

`arrow-json` is available through the existing `arrow` crate feature set. ORC and Avro require new deps. Gate them behind `connectors` crate features `orc` and `avro`, both enabled by default in the workspace (so `cargo build` Just Works), but downstream users can disable them for a lean build.

**Alternative considered:** Always include. Rejected — ORC pulls in protobuf-gen code and Avro pulls in an Apache-Avro runtime, both noticeable in compile time. Feature-gating costs almost nothing and keeps binaries small.

### Decision 6: Partition-level format override is a non-goal v1

HMS allows each `Partition` to override `sd.inputFormat`. Trino handles this. v1 uses the table-level `sd.inputFormat` and logs a warning if a partition disagrees. Full partition-level dispatch is a follow-up change.

## Risks / Trade-offs

- **Risk**: HMS-reported InputFormat does not match actual on-disk bytes (e.g. table declared Parquet, files are CSV). → Mitigation: readers already surface parse errors; we include file path and InputFormat in error context.
- **Risk**: ORC / Avro reader performance in the `arrow` crate is less mature than Parquet. → Mitigation: accept baseline correctness first; perf work in a follow-up change. TPC-H benchmarks against Trino quantify the gap.
- **Risk**: SerDe param semantics drift between Hive versions (e.g. CSV escape handling). → Mitigation: match Trino's `HiveSerDe` interpretation where documented; integration test matrix includes Trino-written files so we validate against the same source.
- **Risk**: `FileConnectorFactory` user-facing config surface grows. → Mitigation: factory rejects formats the installation didn't opt into; error clearly names the missing feature.
- **Risk**: Mixed-format partitions (unsupported in v1) silently produce wrong rows. → Mitigation: on partition enumeration, compare each partition's `sd.inputFormat` to table-level; return `UnsupportedFormat` if they diverge.
