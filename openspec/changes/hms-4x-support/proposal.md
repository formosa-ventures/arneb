## Why

Arneb's Hive connector uses `hive_metastore = "0.2"` (by Xuanwo), which is generated from **Hive 2.3** Thrift IDL. HMS 4.0 removed the legacy `get_table(db, tbl)` method ([HIVE-26537](https://github.com/apache/hive/pull/3599)); only `get_table_req(GetTableRequest)` exists. Calling `get_table` against HMS 4.x returns `Invalid method name: 'get_table'`. Additional legacy methods (`create_database`, `create_table`, `drop_database`, etc.) are deprecated in 4.x and will be removed in HMS 5.0.

Arneb must support HMS 4.x (tested through 4.2.0) and be forward-compatible with 5.0. The third-party crate cannot deliver this without an IDL regeneration that its upstream has not done ([Xuanwo/hive_metastore_rs#30](https://github.com/Xuanwo/hive_metastore_rs/issues/30)).

Note: the related transport codec issue (volo-thrift's default `TTHeader+Framed` vs HMS's `TBinaryProtocol+buffered`) is **already fixed** with `DefaultMakeCodec::buffered()` in `crates/hive/src/catalog.rs`. This change is IDL-version-independent and works on HMS 3.x, 4.x, and 5.x. The remaining blocker is purely the IDL schema mismatch.

## What Changes

Create an in-workspace Rust crate `crates/hive-metastore/` with Thrift bindings generated from **Hive 4.2.0** IDL using `volo-build`. Replace the `hive_metastore = "0.2"` dependency throughout the workspace. Rewrite all HMS call sites to use `_req` method variants for forward compatibility with HMS 5.0.

- New workspace member: `crates/hive-metastore/` (lib crate, `publish = false`, name `hive-metastore`)
- New workspace member: `crates/hive-metastore/thrift_build/` (binary, Xuanwo's pattern — generate code manually, commit the output)
- Thrift IDL files committed under `crates/hive-metastore/thrift_idl/` with upstream SHAs recorded
- Generated code (`src/hms.rs`, ~2.8MB) committed and marked `linguist-generated`
- Production `crates/hive/src/catalog.rs` rewritten to use `get_table_req` (required) and other `_req` variants where available
- Test/setup code (`hive_demo_setup.rs`, `integration.rs` E2E test) migrated to `_req` variants
- `docker-compose.yml` upgraded from `apache/hive:3.1.3` to `apache/hive:4.2.0`

## Impact

- **Crates modified**: `crates/hive-metastore/` (new), `crates/hive/`, `crates/server/`
- **Removed dependencies**: `hive_metastore = "0.2"` (from `crates/hive/Cargo.toml` and `crates/server/Cargo.toml`)
- **Added dependencies**: `hive-metastore = { path = "../hive-metastore" }` in `crates/hive/`; `volo-build = "0.10"` in `crates/hive-metastore/thrift_build/`
- **Config**: No changes required — HMS address format unchanged
- **HMS support matrix**: primary target HMS 4.x (tested 4.0.1 and 4.2.0); HMS 3.x best-effort (methods exist but not actively tested); HMS 5.x compatible by design (no removed methods used)

## Non-Goals

- HMS 2.x support (EOL)
- HMS 3.x active testing / CI coverage
- Partition pruning (tracked separately)
- Write operations beyond what the demo setup requires
