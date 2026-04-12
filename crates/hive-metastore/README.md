# hive-metastore

Hive Metastore Thrift client bindings for Arneb, generated from Hive 4.2.0 IDL.

## Supported HMS Versions

| Version | Status | Notes |
|---------|--------|-------|
| HMS 4.2.x | ✅ Primary target | E2E tested |
| HMS 4.0.x | ✅ Supported | E2E tested |
| HMS 5.x (future) | ✅ Compatible by design | All `_req` variants, no removed methods |
| HMS 3.x | ⚠️ Best-effort | Legacy methods exist in IDL but not actively tested |
| HMS 2.x | ❌ Unsupported | EOL |

## Regenerating from IDL

The generated Rust code (`src/hms.rs`) is **committed to the repo**. `cargo build`
does not regenerate it automatically. After modifying `thrift_idl/hive_metastore.thrift`,
regenerate with:

```bash
cargo run -p hive-metastore-thrift-build
```

This runs the `volo-build` driver in `thrift_build/src/main.rs` and writes fresh
bindings to `src/hms.rs`.

## Layout

```
crates/hive-metastore/
├── Cargo.toml              # lib crate, publish = false
├── README.md               # this file
├── .gitattributes          # marks hms.rs as linguist-generated
├── src/
│   ├── lib.rs              # re-exports from hms.rs
│   └── hms.rs              # GENERATED — do not edit by hand
├── thrift_idl/
│   ├── hive_metastore.thrift   # Hive 4.2.0 IDL (verbatim + 1 include path fix)
│   ├── fb303.thrift            # fb303 IDL (dependency of hive_metastore.thrift)
│   ├── SOURCES.md              # upstream URLs + git SHAs
│   └── PATCHES.md              # record of any local modifications
└── thrift_build/           # separate workspace member (binary)
    ├── Cargo.toml
    └── src/main.rs         # volo-build driver
```

## Why an in-workspace crate?

- Third-party `hive_metastore = "0.2"` on crates.io is generated from Hive 2.3 IDL,
  missing `get_table_req` and other `_req` methods required by HMS 4.0+.
- Upstream `hive_metastore_rs` has not updated to 4.x IDL
  ([upstream issue](https://github.com/Xuanwo/hive_metastore_rs/issues/30)).
- Owning the IDL + generator gives Arneb full control over HMS version support
  without waiting for upstream.
