# Local Patches to Upstream Thrift IDL

This file records every local modification to the upstream Thrift IDL files
in this directory. Keep it up to date — reproducibility depends on knowing
what has been changed from upstream.

## `hive_metastore.thrift`

### Patch 1: Include path fixup

- **Line**: 25
- **Before**: `include "share/fb303/if/fb303.thrift"`
- **After**: `include "fb303.thrift"`
- **Reason**: Upstream references `fb303.thrift` via a multi-level path that
  matches Apache Hive's source tree layout. In this directory, `fb303.thrift`
  sits as a sibling, so the path must be relative to the local directory.
- **Side effects**: None. `volo-build` is invoked with
  `include_dirs(vec!["../thrift_idl".into()])`, so the sibling lookup works.
