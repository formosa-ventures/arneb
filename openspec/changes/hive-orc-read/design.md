## Context

`HiveDataSource` lists the files under the table location and exposes
`files × splits_per_file` scan partitions, reading each with the shared
`connectors::parquet_scan` helpers. HMS metadata already carries the
storage descriptor's input format; the SerDe and partition keys were dropped.
Table metadata reaches workers only through `TableScan.properties` (serialized
with the plan fragment), so anything the scan needs must be a property.

## Decisions

### 1. Branch on format at the file-reader level, not a second data source

`HiveDataSource` gains a `format` field and a `match` in `scan()`: Parquet keeps
its existing code path byte-for-byte for unpartitioned tables; ORC goes to
`crates/hive/src/orc.rs`. Listing, partition values, splits, projection and
output-schema construction stay shared. Rejected: a separate `OrcDataSource`
(duplicates listing/partition/split plumbing — the review feedback this change
was asked to avoid).

### 2. Format detection from input format + SerDe

`HiveFileFormat::detect(input_format, serde_lib)`: `OrcInputFormat`/`OrcSerde`
→ ORC; Parquet classes or both empty (hand-registered test tables) → Parquet;
else `UnsupportedOperation`. The provider adds `serde_lib`,
`partition_columns` (count) and `transactional` to the scan properties.

### 3. ORC reading via orc-rust's async reader

`ObjectStoreChunkReader` implements orc-rust's `AsyncChunkReader` (`len` = cached
HEAD, `get_bytes` = ranged GET; zero-length requests short-circuit because
object stores reject empty ranges). Timestamps are decoded at microsecond
precision to match the Hive `TIMESTAMP` → `Timestamp(µs)` mapping.

### 4. Splits

`splits_per_file` is the same CPU-saturation heuristic as Parquet. With at least
as many stripes as splits, each split reads a contiguous run of whole stripes
(`with_file_byte_range`). Otherwise each split reads a `rows / splits` slice: the
byte range covers the overlapping stripes, a row selection skips to the slice
start, and the stream stops after the slice length. The end is enforced by the
stream, not the selection, because orc-rust keeps decoding a `select` run longer
than one batch until the stripe ends, ignoring a trailing `skip` (found in Tier 2
as 1.5× row counts; regression test
`orc_row_slices_longer_than_a_batch_are_read_exactly`).

### 5. Column mapping and types

By lower-cased name; if every file column is `_colN` and no table column
matches by name, by position (old Hive writers). The adapter
(`scan_adapter::ColumnSource::{File, Null, Constant}`) builds the table-schema
batch: missing → NULL, partition → constant, narrower type → cast with
`safe: false` so overflow errors rather than becoming NULL. Allowed widenings are
checked up front per file (`can_read_as`), so an incompatible file type is a
clear error naming the column, ORC type and table type.

### 6. Timestamp semantics

ORC `timestamp` stores seconds relative to the writer time zone recorded in each
stripe footer; orc-rust converts back to the writer's wall clock, which is the
Hive/Trino semantics for `timestamp` without time zone. Verified against Trino
on a Trino-written file (committed fixture) including a pre-1970 fractional
value, a DST-gap wall-clock value and the 2262 upper range, and on 1.5M
generated timestamps (Tier 2).

### 7. Partitioned tables

Partition keys are appended to the schema (Hive `SELECT *` order). Values are
parsed per file from `key=value` directory names relative to the table
location, `%XX`-unescaped, `__HIVE_DEFAULT_PARTITION__` → NULL, then cast from
text to the column type at construction (invalid values fail the scan). A file
not under a directory for every key is an error. Filters on partition columns
are not pushed into Parquet (they still run in `FilterExec`). This applies to
Parquet too, which previously dropped partition columns entirely.

### 8. ACID

Rejected three ways, so a table is never misread: HMS `transactional=true`;
`base_N` / `delta_N_M` / `delete_delta_N_M` directories during listing; ORC
files whose root columns are the ACID envelope
(`operation, originalTransaction, bucket, rowId, currentTransaction, row`).

## Risks / Trade-offs

- **Temporary git dependency** on a fork commit until orc-rust releases arrow 60
  support (orc-rust#91). Pinned by `rev`; `deny.toml` allows the one git source.
- **Performance**: ORC is ~1.35–1.6× slower than Parquet at SF1 (no row-index
  predicate pushdown; one GET per stream; row-slice splits re-fetch the shared
  stripe). Follow-ups: `with_predicate` row-index pruning, coalesced stream reads.
- Partition values come from the directory listing, not HMS partition metadata:
  partitions registered at custom locations outside the table directory are not
  read.
