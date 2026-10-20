## Context

Arneb plugs connectors in through two seams.

- **Catalog side**: `CatalogProvider` → `SchemaProvider` → `TableProvider`
  (`crates/catalog/src/lib.rs`). Table lookup is async. A `TableProvider`
  returns a schema, optional `TableStatistics`, and a `properties` map that
  is copied into `LogicalPlan::TableScan.properties` and serialized to workers
  with the plan.
- **Execution side**: `ConnectorFactory::create_data_source(table, schema,
  properties)` → `DataSource` with `partition_count()` and
  `scan(&ScanContext, partition)`. `ScanContext` carries `filters`
  (`PlanExpr`s over the **full** table schema), `projection` (the source MUST
  emit exactly these columns in this order), `limit`, `batch_size` and
  `memory_pool`. A `FilterExec` always stays above the scan, so a source may
  apply any subset of the filters.

Distributed scans: the coordinator gives a SOURCE fragment M scan tasks, and
task *i* reads the global partitions `i, i+M, i+2M, …` of its own data source
(`ScanExec`, `crates/execution/src/operator.rs:605-628`). **Each worker
calls `create_data_source` itself and computes `partition_count()`
independently.** Correctness depends on every node arriving at the same N and
the same partition→rows mapping. Hive gets away with re-listing files on each
worker today. A remote database cannot: row-count estimates and `min/max` can
move between the coordinator's planning and a worker's task start.

The catalog wiring lives in `crates/server/src/main.rs` (one loop over
`config.catalogs`, Hive only). It runs identically for every role, so a
catalog registered there exists on workers too.

## Goals / Non-Goals

**Goals**
- A framework crate where a new database means one `Dialect` plus one client,
  with all pushdown logic, its fallback rules and split planning shared.
- A PostgreSQL connector good enough for the M2 federation scenario: read-only,
  TPC-H types plus `timestamp`/`timestamptz`/`uuid`/`text`/`numeric`, parallel
  reads of large tables, results identical to Trino's `postgresql` catalog.
- No change to `DataSource`, `ScanContext` or the catalog traits.

**Non-Goals**
- Dynamic-filter pushdown into PostgreSQL and the mixed TPC-H layout (F4).
- Aggregate / TopN / join pushdown (M4).
- Writes, DDL, transactions.
- TLS to PostgreSQL (`sslmode`). This change connects with `NoTls`. TLS is a
  follow-up recorded in the connector docs as a known limitation. Arneb's
  own client-facing TLS (M1/P3) is unaffected.
- MySQL (F3). The `Dialect` seam is cut with it in mind but not exercised.

## Decisions

### D1. Two crates: `arneb-rdbms` (framework) and `arneb-postgresql`

`arneb-rdbms` depends on `arneb-common`, `arneb-planner` (for `PlanExpr`) and
`arneb-sql-parser` (for `BinaryOp`), and has no database driver. It owns:

- `Dialect` (trait): `quote_identifier`, `render_literal(&ScalarValue,
  &DataType) -> Option<String>`, `like(column, pattern, negated)`, and
  `supports_binary_op(BinaryOp) -> bool`.
- `RemoteTable` / `RemoteColumn`: the remote schema/table names plus, per
  column, the remote type name and the mapped Arneb type (`Result` — an
  unsupported type is kept as an error value, see D5).
- `translate_filters(&[PlanExpr], &RemoteTable, &dyn Dialect) ->
  Translation { sql: Vec<String>, complete: bool }`.
- `build_select(&RemoteTable, projection, &Translation, split: Option<&SplitRange>,
  limit, &dyn Dialect) -> String`.
- `SplitPlan` (encoding into and decoding from `TableScan.properties`) and
  `plan_splits(min, max, estimated_rows, split_min_rows, max_splits)`.

`arneb-postgresql` holds `PostgresDialect`, the pool, the catalog provider,
the connector factory and the binary COPY decoder.

*Alternative*: one crate with a `postgresql` feature. Rejected, because a
separate crate keeps the framework honest about having no PostgreSQL leak,
which is what F3 will test.

### D2. Split bounds are decided on the coordinator and travel in properties

The `TableProvider` built by `PostgresSchema::table()` runs on the coordinator
during planning. It computes the split plan there:
1. Read `reltuples` and the primary key from `pg_class`/`pg_index`/`pg_attribute`.
2. If the key is a single `smallint`/`integer`/`bigint` column and
   `reltuples ≥ split_min_rows`, run `SELECT min(k), max(k)` (an index-only
   probe on a primary key) and cut `[min, max]` into
   `n = min(max_splits, ceil(reltuples / split_min_rows))` ranges of equal width.
3. Encode as properties: `rdbms.split_column = "<name>"`,
   `rdbms.split_bounds = "b1,b2,…,b(n-1)"` (inner boundaries, i64).

The data source on every node decodes the same properties. Partition *k*
reads `k < b1` for k = 0, `b_k ≤ k < b(k+1)` in the middle, and `k ≥ b(n-1)`
for the last. The first and last ranges are open, so rows inserted after
planning are still read exactly once. Every node therefore agrees on N and on
the ranges by construction. Nothing is recomputed on workers.

Equal-width ranges assume roughly uniform keys, which is true for TPC-H keys
and typical serial/identity keys. Skewed keys still return correct results
with uneven partitions, and histogram-based bounds are a later refinement.

*Alternative*: compute the bounds in `create_data_source`. Rejected, because
it is the bug class the Hive `splits_per_file` issue has (see Risks).

### D3. Filter translation: per-conjunct, all-or-nothing within a conjunct

Each filter in `ScanContext.filters` is flattened into top-level `AND`
conjuncts, and each conjunct either translates fully or is dropped from the
remote `WHERE`. The supported grammar:

```
conj   := cmp | IS [NOT] NULL col | col [NOT] IN (lit, …) | col BETWEEN lit AND lit
        | col [NOT] LIKE 'pattern' | conj AND conj | conj OR conj | NOT conj
cmp    := col op lit | lit op col | col op col (comparable type families)
op     := = <> < <= > >=
```

Literals go through `Dialect::render_literal`, which returns `None` for any
value it cannot render exactly. That covers NULL (never pushed for
comparisons; `IS NULL` is separate), NaN/∞ floats, and types the dialect does
not know. Both sides of a comparison, column or literal, must fall in the same
type family: exact numbers (any integer or decimal against any other, which
PostgreSQL compares exactly), text (equality only), boolean (equality only),
date, or timestamp with the same time zone. Any other pairing stays local. This avoids PostgreSQL's implicit casts producing
different comparison semantics than Arneb's. `complete = true` only if every
conjunct translated, which gates `LIMIT` (D4).

Arneb's `LIKE` and PostgreSQL's `LIKE` agree on `%`/`_`, and both use `\` as
the default escape character. PostgreSQL `LIKE` is case-sensitive like Arneb's,
and `ILIKE` is not used. Collation can make `<`/`>` on text differ from Arneb's
byte order. Text **range** comparisons (`< <= > >=` on VARCHAR) are therefore
not pushed. Text equality and `IN` are pushed, because equality is
collation-independent for deterministic collations, which is PostgreSQL's
default.

Implementation added two rules for comparison semantics that
differ in PostgreSQL. `real`/`double precision` columns are never pushed,
because PostgreSQL orders `NaN` above every number, so `NOT (x > 1)` would drop
NaN rows Arneb keeps. `char(n)` columns are refused through
`Dialect::pushable_column`, because PostgreSQL ignores their trailing spaces,
so `<>` would drop rows. `uuid` columns are refused the same way: Arneb reads
them as text, but PostgreSQL casts the literal to `uuid`, so it compares
case-insensitively and turns a non-uuid literal or a `LIKE` into an error. Lossless widening casts the analyzer puts on columns
(`CAST(int4_col AS BIGINT) IN (…)`) are looked through. Without that, almost
no integer predicate would be pushed.

### D4. Limit

The physical planner does not set `ScanContext.limit` today. This change adds
one narrow case: `Limit { limit: Some(n), offset: None | Some(0) }` directly
over a `TableScan` sets `limit = n` on the scan context. `LimitExec` stays
above it. No existing connector reads `ScanContext.limit`, so they are
unaffected. The PostgreSQL scan appends `LIMIT n` only when
`translation.complete`. With a filter between the limit and the scan, the
planner wiring does not apply, so the question does not come up in this
change. The `complete` gate is still enforced for future callers.

### D5. Types are mapped at discovery, failures deferred to use

`information_schema.columns` gives `data_type`, `numeric_precision`,
`numeric_scale` and `udt_name`. Mapping produces `Result<DataType, String>`.
A column with an unsupported type still appears in the table schema (as VARCHAR,
so `DESCRIBE` and `SELECT` of other columns work), but the scan fails with
`column 'x' has unsupported PostgreSQL type 'jsonb'` as soon as a projection
includes it (or a projection is absent). That is the spec's "fail only if
read" behavior, and it matches Trino's `unsupported-type-handling = FAIL`
for selected columns.

`timestamptz` maps to `Timestamp { unit: Microsecond, timezone: Some("UTC") }`,
because PostgreSQL's binary format sends UTC microseconds. `uuid` maps to
canonical lowercase text. Unconstrained `numeric` maps to `Decimal128(38,
unconstrained_numeric_scale)`, and a value with more fractional digits fails
(D6).

### D6. Binary COPY, hand-decoded

Each partition runs `COPY (<select>) TO STDOUT (FORMAT binary)` through
`tokio_postgres::Client::copy_out`, and decodes the stream with a small
state machine. The stream has an 11-byte signature, flags and an extension
area. Each tuple is `i16` field count, then per field `i32` length (−1 =
NULL) followed by the bytes. The trailer is `-1`. Values are appended straight
to typed Arrow builders, with no `Row`/`FromSql` per value:

| PG type | wire | Arrow |
|---|---|---|
| bool | 1 byte | Boolean |
| int2/int4/int8 | BE ints | Int16/32/64 |
| float4/float8 | BE IEEE | Float32/64 |
| numeric | base-10000 digits, weight, sign, dscale | Decimal128 (exact rescale, error on loss or NaN/∞) |
| text/varchar/bpchar | UTF-8 bytes | Utf8 (bpchar keeps its padding, as PostgreSQL returns it) |
| date | i32 days since 2000-01-01 | Date32 (+10957) |
| timestamp(tz) | i64 µs since 2000-01-01 | Timestamp(µs) (+946684800000000) |
| uuid | 16 bytes | Utf8, canonical form |
| bytea | bytes | Binary |

PostgreSQL `date`/`timestamp` `±infinity` (i32/i64 MIN/MAX) fail with a
column-named error. A batch is flushed every `batch_size` rows. The stream holds a
`MemoryReservation` sized to the decode buffer, which is the last flushed batch's
`get_array_memory_size()`, as the estimate for the builders being refilled. It
is recorded with the infallible `grow`/`shrink` and freed when the stream ends,
fails or is dropped. That matches the Parquet scan's decode-buffer accounting
(#137): a scan cannot spill, so it never fails on its reservation, but
spillable operators see the pressure. The estimate lags: it is taken after the
first full batch and released before the last one, and the decoder's own
`BytesMut` read buffer is not counted. At the default batch size that is small,
but wide rows (large `bytea` or text) can decode well past the reservation.

*Alternative*: `query_raw` with text or binary `Row`s. Rejected, because
per-value `FromSql` plus `Row` allocation is several times slower than COPY,
and there is no `FromSql` for numeric without another crate.

### D7. Pooling and cancellation

`deadpool-postgres` with `max_size = max_connections` per catalog per node.
A scan acquires a client when its stream is first polled and holds it until
the COPY ends. If the stream is dropped (query cancelled or LIMIT satisfied
upstream), the in-flight COPY's connection is **not** returned to the pool. It
is detached (`Object::take`) and dropped, which closes the socket and
makes PostgreSQL abort the COPY. Returning a connection mid-COPY would
poison the pool.

Opening a connection is bounded by `connect_timeout_secs` (default 10, both
the TCP connect and the pool's create step) and waiting for a free one by
`pool_wait_timeout_secs` (default 60). Without them a full pool, including
the planning-time metadata lookups, or a host that drops packets would make a
query wait with no error. (Found in review.)

### D8. Configuration

`CatalogConfig` gains optional fields, ignored by Hive: `connection_url`,
`user`, `password` (a `RedactedString`-style wrapper), `max_connections`,
`split_min_rows`, `max_splits` and `unconstrained_numeric_scale`. The password
env override is `ARNEB_CATALOG_<NAME>_PASSWORD`. Per `configuration.md`,
these are runtime knobs: read, applied and logged (without the password) at
startup. `main.rs`'s loop dispatches on `catalog_type`: `"hive"` keeps the
existing block unchanged, `"postgresql"` calls
`arneb_postgresql::register(...)`, and anything else keeps the
"unsupported, skipping" warning.

`default_catalog`/`default_schema` selection is unchanged. The first
`[[catalogs]]` entry is still the default, so a PostgreSQL catalog listed first
becomes the default catalog. That is documented.

### D9. Read-only

The factory's `ddl_provider()` returns `None`. No server path routes DML to a
connector's `DDLProvider` today. `INSERT … SELECT` fails in the protocol layer
("DDL/DML plans are handled at the protocol level"), and `INSERT … VALUES` fails
in the planner, before any connector is involved. Nothing reaches PostgreSQL,
which Tier 2 checked. A read-only `DDLProvider` would be dead code, so none is
added. The spec requires failure without remote writes, not a specific message.

### D10. Stacked filters (found in Tier 2)

The logical optimizer splits `WHERE a AND b AND c` into stacked `Filter`
nodes. The column-pruning pre-pass then puts a column-only `Projection`
between them and the scan. The physical planner only ever passed a `Filter`
**directly** on a `TableScan` into `ScanContext.filters`, so a remote source saw
at most one conjunct, and with pruning often none. A new
`DataSource::pushdown_stacked_filters()` (default `false`) lets a source opt
in. The planner then collects every stacked predicate, composes the
projections into the scan's projection, maps predicate indices back to the table
schema for the source, and keeps the same `FilterExec`s above the scan. The
default is `false` so file connectors keep their measured behavior. Enabling it
for Parquet is a separate change that needs SF10/SF30 measurement.

### D11. Engine fixes found in Tier 2

- **Scan task owning no partition.** With M scan tasks and a source with
  N < M partitions (a 1-partition PostgreSQL table on 2 workers), task
  `i ≥ N` owns 0 partitions, but it is still executed for partition 0, and
  `ScanExec` asked the source for global partition `i`. `ScanExec` now returns
  an empty stream for that case. Hive never hit it, because its seeded tables
  have 8 files.
- **pgwire timestamp text.** `encode_value` dropped fractional seconds and
  mapped every pre-1970 instant to the epoch (`%` remainder sign plus
  `unwrap_or_default`). It now uses floor division and prints fractions the way
  PostgreSQL does (trailing zeros trimmed). The Trino protocol encoder was
  already correct.

## Risks / Trade-offs

- **Partition agreement across nodes**: handled by D2. Writing this design
  found that Hive's `partition_count = files × splits_per_file`, with
  `splits_per_file` taken from each worker's own `available_parallelism()`
  (`crates/connectors/src/parquet_scan.rs:43-53`), so heterogeneous workers
  can lose or duplicate rows. That is out of scope here and reported as a
  separate issue.
- **Planning-time round trips**: each query that references a PostgreSQL table
  costs a few catalog queries (columns, key, `reltuples`, and min/max when
  split) on the coordinator. That is acceptable for an MVP. A short-TTL
  metadata cache is a follow-up if planning latency shows up.
- **Stale `reltuples`**: a never-analyzed table reports −1 or 0 and is read as
  one split. That is correct, just not parallel. Documented, with `ANALYZE` as
  the remedy.
- **Pushdown semantic drift**: the main correctness risk. Mitigated by D3's
  conservative grammar (no text ranges, no implicit casts, no NULL literals),
  by the always-present `FilterExec`, and by Tier 1 tests that compare pushed
  and unpushed results on the same data.
- **Split snapshots**: each split is its own COPY with its own snapshot, so a
  concurrent `UPDATE` that moves a row's key from one range to another can
  show it twice or not at all. D2's every-row-once holds for a table that is
  not written during the query, and for inserts.
- **Collations**: D3 pushes text `=` / `<>` / `IN` assuming a deterministic
  collation. A column with a nondeterministic (for example ICU
  case-insensitive) collation makes a pushed `<>` or `NOT IN` drop rows Arneb
  would keep. `pg_collation.collisdeterministic` is not checked yet.
- **Pushdown can turn an error into rows**: a pushed filter that excludes
  values Arneb cannot read (`numeric` NaN or ±Infinity, `±infinity`
  timestamps, an unconstrained `numeric` with more fractional digits than
  `unconstrained_numeric_scale`) lets the query succeed where the unpushed
  read fails. The rows returned are still correct.
- **Load errors look like a missing table**: `SchemaProvider::table` returns
  an `Option`, so a failed column lookup (permission, connection) is logged
  and reported as "table not found". A failed split lookup no longer does: the
  table is read as one split.
- **Workers that start before PostgreSQL**: each node connects at startup and
  skips the catalog if it cannot. A worker that started while PostgreSQL was
  unreachable has no factory for it, so the coordinator plans scans whose
  tasks fail on that worker until it restarts. A lazy connect is a follow-up.
- **New dependencies**: `tokio-postgres` and `deadpool-postgres`
  (MIT/Apache-2.0). `cargo deny` must pass.

## Migration Plan

Additive. No config migration. Rollback means removing the `postgresql`
catalog entry.

## Open Questions

None blocking. Histogram-based split bounds and TLS to PostgreSQL are recorded
follow-ups.
