## Why

Arneb only supports Snappy-compressed Parquet files. When reading Parquet data produced by Trino (default: Gzip), Spark (default: Snappy/Zstd), DuckDB (default: Zstd), or other engines, files using unsupported compression codecs fail with cryptic errors like "Disabled feature at compile time: flate2". A query engine that claims to read Parquet must support all standard compression codecs.

## What Changes

- Enable all standard Parquet compression features in the `parquet` crate dependency: `zstd`, `lz4`, `brotli`, and `flate2`
- Resolve the `flate2` backend selection issue (requires `rust_backend` or similar feature)
- Add integration tests verifying each compression codec can be read
- Document supported compression formats

## Capabilities

### New Capabilities

- `compression-codecs`: Support for Zstd, LZ4, Brotli compression in Parquet files
- `flate2-backend`: Gzip/DEFLATE support via flate2 with proper backend configuration

### Modified Capabilities

- Existing Parquet reading in both file connector and Hive connector automatically gains all codecs

## Impact

- **Cargo.toml**: Add features to workspace parquet dependency
- **Binary size**: Slight increase from compression libraries (~1-2MB)
- **Dependencies**: `zstd-sys`, `lz4-sys`, `brotli` (transitive via parquet features)
- **No code changes**: Pure dependency configuration
