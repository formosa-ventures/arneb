## 1. New Workspace Crate

- [x] 1.1 Scaffold `crates/hive-metastore/` with `Cargo.toml` (`name = "hive-metastore"`, `publish = false`, deps: `volo`, `volo-thrift`, `pilota`, `anyhow`)
- [x] 1.2 Create `src/lib.rs` with crate-level `#![allow(clippy::all, warnings, unused_imports, dead_code)]`
- [x] 1.3 Add `crates/hive-metastore` and `crates/hive-metastore/thrift_build` to workspace `members` in root `Cargo.toml`
- [x] 1.4 Create `.gitattributes` marking `src/hms.rs linguist-generated=true`
- [x] 1.5 Create `README.md` explaining regeneration workflow and HMS version support matrix

## 2. Thrift IDL

- [x] 2.1 Download `thrift_idl/hive_metastore.thrift` from Hive 4.2.0 release tag
- [x] 2.2 Download `thrift_idl/fb303.thrift` from Apache Thrift 0.16.0 (referenced by hive_metastore.thrift)
- [x] 2.3 Patch `hive_metastore.thrift` line 25 include path (`share/fb303/if/fb303.thrift` → `fb303.thrift`)
- [x] 2.4 Create `thrift_idl/SOURCES.md` recording upstream URLs and git SHAs
- [x] 2.5 Create `thrift_idl/PATCHES.md` recording the include-path fixup

## 3. Code Generation

- [x] 3.1 Scaffold `crates/hive-metastore/thrift_build/` workspace member (binary, dep on `volo-build = "0.10"`)
- [x] 3.2 Write `thrift_build/src/main.rs` using `volo_build::Builder::thrift()` pattern
- [x] 3.3 Run `cargo run -p hive-metastore-thrift-build` to generate `src/hms.rs` (19 MB, 399k lines)
- [x] 3.4 Handle any volo-build parser errors — **none encountered**; Hive 4.2.0 IDL parsed cleanly
- [x] 3.5 Wire up `pub mod hms;` in `lib.rs` with `pub use hms::hms::hive_metastore::*;` re-exports
- [x] 3.6 Verify `cargo check -p hive-metastore` passes
- [x] 3.7 Commit generated `src/hms.rs`

## 4. Production Call Site Migration

- [x] 4.1 Replace `hive_metastore = "0.2"` with `hive-metastore = { path = "../hive-metastore" }` in `crates/hive/Cargo.toml`
- [x] 4.2 Grep generated `hms.rs` for available `_req` methods matching our surface area
- [x] 4.3 Rewrite `HmsClient::get_table` to use `get_table_req(GetTableRequest)` (required — legacy removed in 4.0)
- [x] 4.4 `get_all_databases` and `get_all_tables` retained (still available in 4.2.0; `_req` variants not present for these)
- [x] 4.5 Update imports in `crates/hive/src/catalog.rs`
- [x] 4.6 Verify transport fix (`DefaultMakeCodec::buffered()`) is preserved in the new client construction

## 5. Test / Setup Code Migration

- [x] 5.1 Replace dep in `crates/server/Cargo.toml`
- [x] 5.2 Migrate `crates/server/src/bin/hive_demo_setup.rs` to `_req` variants (`create_database_req`, `create_table_req`, `drop_database_req`)
- [x] 5.3 Migrate `crates/server/tests/integration.rs::test_hive_e2e_hms_s3_parquet` to `_req` variants
- [x] 5.4 Struct literals use `..Default::default()` for 4.2 IDL forward compatibility

## 6. Verification

- [x] 6.1 `cargo check --workspace` passes
- [x] 6.2 `cargo clippy -p arneb-hive -p arneb-server --all-targets -- -D warnings` passes (hive-metastore crate skipped: `linguist-generated`, internal `allow(clippy::all)`)
- [x] 6.3 `cargo test --workspace` passes (non-ignored tests — no regressions)
- [x] 6.4 Update `docker-compose.yml`: `apache/hive:4.0.1` → `apache/hive:4.2.0`
- [x] 6.5 `docker compose up -d` and wait for HMS 4.2.0 healthy
- [x] 6.6 Run `cargo test -p arneb-hive -- --ignored smoke_test_hms_connection` against 4.2.0 — **passed**
- [x] 6.7 Run `cargo run --bin hive-demo-setup` against 4.2.0 — **succeeded** (all `_req` methods work)
- [x] 6.8 Run `cargo test -p arneb-server -- --ignored test_hive_e2e_hms_s3_parquet` against 4.2.0 — **passed** (3 rows returned through full stack)
- [x] 6.9 Interactive psql verification: HMS 4.2.0 metadata queries work (pg_catalog.pg_namespace returns `default`, `demo`, `arneb_e2e_test` schemas from HMS)
- [x] 6.10 Interactive psql verification: `SELECT * FROM datalake.demo.cities` returns 5 rows, JOIN+GROUP BY returns 4 aggregated rows. Unblocked by building a custom HMS image (`docker/hive-metastore/Dockerfile`) `FROM apache/hive:4.2.0` + `hadoop-aws-3.4.1` + `software.amazon.awssdk:bundle:2.29.52` + S3A config for MinIO. HMS now stores real `s3a://` locations, `hive_demo_setup` and the E2E test use them directly, and Arneb's existing properties pipeline routes the queries through `StorageRegistry` to MinIO. No workarounds remain.
