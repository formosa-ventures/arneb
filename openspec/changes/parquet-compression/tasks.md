## 1. Enable Compression Features

- [x] 1.1 Add `zstd` feature to parquet dependency in workspace Cargo.toml
- [x] 1.2 Add `lz4` feature to parquet dependency in workspace Cargo.toml
- [x] 1.3 Add `brotli` feature to parquet dependency in workspace Cargo.toml
- [x] 1.4 Resolve flate2 backend: add `flate2` feature to parquet and ensure rust_backend is selected (may require direct flate2 dependency)
- [x] 1.5 Verify `cargo build` succeeds with all compression features enabled

## 2. Integration Tests

- [x] 2.1 Create test Parquet files with each compression codec (Snappy, Gzip, Zstd, LZ4, Brotli, uncompressed)
- [x] 2.2 Add integration test in `crates/connectors/` reading each compressed file via file connector
- [x] 2.3 Add integration test in `crates/hive/` reading each compressed file via Hive data source
- [x] 2.4 Verify all tests pass

## 3. Documentation

- [x] 3.1 Document supported compression formats in CLAUDE.md (Key Dependencies section)
- [x] 3.2 Remove the `hive.compression-codec=SNAPPY` workaround from docker/trino/catalog/hive.properties if no longer needed
