## ADDED Requirements

### Requirement: Gzip/DEFLATE compression support via flate2
The system SHALL read Parquet files compressed with GZIP (DEFLATE) codec. This requires the `flate2` feature on the `parquet` crate and a compatible flate2 backend.

#### Scenario: Read Gzip-compressed Parquet
- **WHEN** a Parquet file with GZIP compression is scanned
- **THEN** the data is decompressed and returned correctly

#### Scenario: Trino default compression
- **WHEN** Trino writes Parquet files without explicit compression config (defaulting to GZIP)
- **THEN** Arneb can read those files without the `hive.compression-codec=SNAPPY` workaround

### Requirement: flate2 backend selection
The `flate2` crate (v1.1.9+) requires explicitly selecting a compression backend. The system SHALL use `rust_backend` (miniz_oxide) to avoid C library dependencies.

#### Scenario: Build with flate2 succeeds
- **WHEN** `cargo build` is run with the `flate2` feature enabled on the parquet crate
- **THEN** compilation succeeds without "You need to choose a zlib backend" errors

#### Scenario: Cross-platform compatibility
- **WHEN** the project is built on macOS, Linux, or in CI
- **THEN** the flate2 backend compiles without requiring system zlib installation

### Requirement: Remove Snappy workaround
After Gzip support is enabled, the `hive.compression-codec=SNAPPY` line in `docker/trino/catalog/hive.properties` SHALL be removed, allowing Trino to use its default compression.

#### Scenario: Trino default config works
- **WHEN** `hive.compression-codec=SNAPPY` is removed from hive.properties
- **AND** Trino writes Parquet with its default compression
- **THEN** Arneb can read the resulting files
