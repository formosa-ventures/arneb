## 1. Foundation: FileFormat + readers

- [ ] 1.1 Extend `FileFormat` enum in `crates/connectors/src/file.rs` with `Json`, `Orc` (feature `orc`), `Avro` (feature `avro`) variants, plus `Display`/`FromStr` coverage
- [ ] 1.2 Add `orc` and `avro` cargo features to `crates/connectors/Cargo.toml` (default-on); pull in `arrow-orc` and the `arrow/json` + `arrow-avro` deps
- [ ] 1.3 Implement `JsonDataSource` against `arrow-json` (NDJSON, schema-provided at construction) with NULL-projection for missing fields and file+line-offset errors on malformed rows — reuse existing `CsvDataSource` test scaffolding for harness shape
- [ ] 1.4 Implement `OrcDataSource` under `#[cfg(feature = "orc")]` using `arrow-orc` reader; derive schema from ORC metadata
- [ ] 1.5 Implement `AvroDataSource` under `#[cfg(feature = "avro")]` using `arrow-avro` reader; derive schema from Avro writer schema; apply reader-schema resolution when HMS schema differs
- [ ] 1.6 Teach `FileConnectorFactory::register_table` / `create_data_source` to dispatch the new variants, returning `ConnectorError::UnsupportedFormat` when a requested format's feature is disabled

## 2. Hive InputFormat mapping

- [ ] 2.1 Add `hive_input_format_to_file_format(&str, serde_lib: &str) -> Result<FileFormat, ConnectorError>` in a new `crates/hive/src/file_format.rs` module, with the mapping table from design.md Decision 1 (Parquet / ORC / Avro / JSON-via-SerDe / CSV-via-SerDe default)
- [ ] 2.2 Unit-test the mapping for every supported className (positive) and several unknown classNames (negative), including the `TextInputFormat` + SerDe tie-breaker
- [ ] 2.3 Replace `HiveTableProvider::is_parquet()` with `fn format(&self) -> Result<FileFormat, ConnectorError>` in `crates/hive/src/catalog.rs` using the new mapping; update all workspace callers (`grep -r is_parquet`)

## 3. HiveDataSource dispatcher

- [ ] 3.1 In `crates/hive/src/datasource.rs`, replace the hard-coded Parquet path with a `match table.format()?` dispatch that instantiates the right `DataSource` from `crates/connectors/src/file.rs`
- [ ] 3.2 Merge `sd.serdeInfo.parameters` with table-level `parameters` and pass downstream as the connector `properties` map
- [ ] 3.3 Validate that every partition's `sd.inputFormat` equals the table-level `sd.inputFormat`; return `ConnectorError::UnsupportedFormat` on mismatch (design Decision 6)
- [ ] 3.4 Add unit tests for each supported format using `InMemory` ObjectStore + pre-baked fixture bytes for Parquet/CSV/JSON (skip ORC/Avro if fixture generation is heavy; integration-only)

## 4. Error surface

- [ ] 4.1 Extend `ConnectorError` with `UnsupportedFormat { input_format: String, table: TableReference }` (or equivalent)
- [ ] 4.2 Thread the error through `HiveDataSource::scan()` and through `FileConnectorFactory::register_table` for disabled-feature paths
- [ ] 4.3 Unit-test the error surface — unknown className, disabled feature, partition mismatch

## 5. Benchmarks / docker infra

- [ ] 5.1 Add an optional `TPCH_FORMAT` env var (default `PARQUET`) to `docker/tpch-seed/seed.sh` that switches the CTAS `WITH (format = '...')` clause between `PARQUET`, `TEXTFILE`, `ORC`, `AVRO`, `JSON`
- [ ] 5.2 Document the new env var in `benchmarks/tpch/README.md`
- [ ] 5.3 Smoke-run the 16-green TPC-H query subset against each seeded format and confirm row counts match Trino (local only — not part of CI)

## 6. Documentation

- [ ] 6.1 Update `docs/connectors/hive.md` with a "Supported file formats" section listing className mappings, recognised SerDes, and SerDe param keys honoured
- [ ] 6.2 Note the `orc` / `avro` feature flags and the downstream opt-out implications
- [ ] 6.3 Cross-reference this change from `CLAUDE.md` under the hive connector section

## 7. Verification

- [ ] 7.1 `cargo fmt -- --check` clean
- [ ] 7.2 `cargo clippy --workspace --all-targets -- -D warnings` clean (both with and without `orc` / `avro` features)
- [ ] 7.3 `cargo test --workspace --lib` passes, including new mapping and dispatcher tests
- [ ] 7.4 End-to-end: docker compose up, seed with `TPCH_FORMAT=TEXTFILE`, run arneb against Hive catalog, verify `SELECT COUNT(*) FROM datalake.tpch.lineitem` returns the TPC-H constant
- [ ] 7.5 Repeat end-to-end for ORC (and Avro if feature is part of this change)
- [ ] 7.6 Open PR targeting main, reference this change: `openspec: multi-format-hive-connector`
