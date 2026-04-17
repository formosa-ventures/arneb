## Context

The `parquet` crate (v58) supports multiple compression codecs behind feature flags. Arneb currently only enables `snap` (Snappy). During the benchmark-infra-hive implementation, we discovered that Trino writes Gzip-compressed Parquet by default, causing Arneb to fail with "Disabled feature at compile time: flate2". We worked around this by forcing Snappy in the Trino Hive connector config, but this is fragile — any external Parquet source may use different compression.

## Goals / Non-Goals

**Goals:**

- Support reading Parquet files compressed with any standard codec (Snappy, Gzip, Zstd, LZ4, Brotli, uncompressed)
- Resolve the flate2 backend compilation issue encountered during benchmark-infra-hive
- Verify each codec works with integration tests

**Non-Goals:**

- Write-side compression selection (Arneb primarily reads Parquet, writing is limited to CTAS)
- Custom compression parameters (levels, dictionary size)
- Non-standard or deprecated codecs (LZO)

## Decisions

### D1: Enable all standard compression features

**Choice**: Add `zstd`, `lz4`, `brotli`, and `flate2` features to the workspace `parquet` dependency in `Cargo.toml`.

**Rationale**: Each feature flag maps to a single compression codec. The binary size increase is minimal (~1-2MB total) and well worth universal Parquet compatibility. All major query engines (Trino, Spark, DuckDB, DataFusion) can produce files with any of these codecs.

### D2: Use rust_backend for flate2

**Choice**: Add `flate2` with the `rust_backend` feature to avoid requiring system zlib.

**Rationale**: flate2 v1.1.9+ requires explicitly selecting a backend. `rust_backend` (miniz_oxide) is pure Rust, cross-platform, and avoids C dependency issues. Performance is comparable to zlib for decompression. If `rust_backend` is not directly selectable through parquet features, add `flate2` as a direct dependency with `rust_backend` enabled.

## Risks / Trade-offs

**[Binary size]** -> Adding all codecs increases binary size by ~1-2MB. **Mitigation**: Acceptable for a query engine binary.

**[Compilation time]** -> `zstd-sys` and `lz4-sys` require C compilation. **Mitigation**: One-time cost, cached by cargo.

**[flate2 backend complexity]** -> May need to add flate2 as direct dependency to control backend selection. **Mitigation**: Well-documented issue, standard approach in Rust ecosystem.
