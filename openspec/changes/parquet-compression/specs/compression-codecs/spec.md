## ADDED Requirements

### Requirement: Zstandard (Zstd) compression support
The system SHALL read Parquet files compressed with Zstandard (ZSTD) codec. This requires the `zstd` feature flag on the `parquet` crate dependency in workspace `Cargo.toml`.

#### Scenario: Read Zstd-compressed Parquet
- **WHEN** a Parquet file with ZSTD compression is scanned via the file connector or Hive data source
- **THEN** the data is decompressed and returned as Arrow RecordBatches
- **AND** results are identical to reading the same data with Snappy compression

#### Scenario: Zstd from Spark/DuckDB
- **WHEN** a Parquet file produced by Spark or DuckDB (which default to ZSTD) is loaded
- **THEN** it is read successfully without errors

### Requirement: LZ4 compression support
The system SHALL read Parquet files compressed with LZ4 codec. This requires the `lz4` feature flag on the `parquet` crate.

#### Scenario: Read LZ4-compressed Parquet
- **WHEN** a Parquet file with LZ4 compression is scanned
- **THEN** the data is decompressed and returned correctly

### Requirement: Brotli compression support
The system SHALL read Parquet files compressed with Brotli codec. This requires the `brotli` feature flag on the `parquet` crate.

#### Scenario: Read Brotli-compressed Parquet
- **WHEN** a Parquet file with Brotli compression is scanned
- **THEN** the data is decompressed and returned correctly

### Requirement: Uncompressed Parquet support
The system SHALL read Parquet files with no compression (UNCOMPRESSED codec).

#### Scenario: Read uncompressed Parquet
- **WHEN** a Parquet file with UNCOMPRESSED codec is scanned
- **THEN** the data is returned correctly

### Requirement: All codecs work through both connectors
All compression codecs SHALL work through both the file connector (`crates/connectors/src/file.rs`) and the Hive data source (`crates/hive/src/datasource.rs`).

#### Scenario: File connector reads Zstd
- **WHEN** a Zstd-compressed Parquet file is configured as a `[[tables]]` entry
- **THEN** it is read successfully

#### Scenario: Hive data source reads Zstd
- **WHEN** a Zstd-compressed Parquet file is stored in MinIO and registered in HMS
- **THEN** it is read successfully via the Hive catalog
