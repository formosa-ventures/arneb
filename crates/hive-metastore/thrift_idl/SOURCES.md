# Thrift IDL Sources

This directory contains Thrift IDL files used to generate the Rust bindings
in `../src/hms.rs`. Files here are copies from upstream sources, pinned to
specific release tags for reproducibility.

## Files

### `hive_metastore.thrift`

- **Upstream**: https://raw.githubusercontent.com/apache/hive/rel/release-4.2.0/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift
- **Release**: Apache Hive 4.2.0
- **Tag**: `rel/release-4.2.0`
- **Size**: 3383 lines
- **Patches**: see `PATCHES.md`

### `fb303.thrift`

- **Upstream**: https://raw.githubusercontent.com/apache/thrift/v0.16.0/contrib/fb303/if/fb303.thrift
- **Release**: Apache Thrift 0.16.0
- **Tag**: `v0.16.0`
- **Size**: 113 lines
- **Patches**: none

`hive_metastore.thrift` inherits from `fb303.FacebookService`; `fb303.thrift`
provides that base service definition.

## Regenerating

After modifying any file in this directory, regenerate the Rust bindings:

```bash
cargo run -p hive-metastore-thrift-build
```
