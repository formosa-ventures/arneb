## 1. Dependency

- [x] 1.1 Add `orc-rust` (git fork pinned to `ba7af4c`, arrow 60) to the workspace; allow the git source in `deny.toml` with a pointer to orc-rust#91
- [x] 1.2 Confirm `cargo tree -d` has no duplicate `arrow`/`parquet` and `cargo deny --all-features check` passes
- [ ] 1.3 Switch to the crates.io release once orc-rust publishes arrow 60 support; drop the `allow-git` entry

## 2. Metadata

- [x] 2.1 Carry the SerDe class and partition keys from HMS (`HiveTableMeta`)
- [x] 2.2 `HiveTableProvider`: schema = data columns + partition columns; properties carry `serde_lib`, `partition_columns`, `transactional`

## 3. Scan

- [x] 3.1 `HiveFileFormat::detect` from input format + SerDe; unsupported formats error
- [x] 3.2 Move `ColumnSource` / `adapt_batch` / `remap_filter` to `arneb_connectors::scan_adapter` (shared with Iceberg); add `Constant`
- [x] 3.3 ORC reader (`crates/hive/src/orc.rs`): object-store chunk reader, name/positional column mapping, widening checks, stripe/row-slice splits
- [x] 3.4 Partition values from `key=value` paths for both formats; skip hidden directories
- [x] 3.5 Reject ACID tables (HMS flag, directory layout, ORC envelope)
- [x] 3.6 Read partitions from HMS (`get_partitions_req`) at each partition's own location, carried in the scan properties; unregistered directories and stray files are ignored (review on #113)

## 4. Validation

- [x] 4.1 Tier 1: unit/integration tests (orc-rust-written files, a Trino-written all-types fixture, partitioned ORC + Parquet, projection, filters, NULLs, case-insensitive names, splits, ACID and format errors, SQL end-to-end)
- [x] 4.2 Tier 2: TPC-H SF1 ORC (DOUBLE + DECIMAL) 22/22 cell-diff vs Trino, standalone and coordinator + 2 workers; Parquet re-run unchanged; partitioned / escaped / timestamp ORC tables vs Trino
- [x] 4.3 Tier 2 (HMS partitions): unregistered `9-GHOST` directory, stray root file, partition registered in another directory, and escaped varchar values (`a/b`, `x=y%z`, spaces, NULL) cell-diffed vs Trino, standalone and coordinator + 2 workers

## 5. Docs

- [x] 5.1 `docs/connectors/hive.md`: storage formats, partitioning, ACID, limitations
