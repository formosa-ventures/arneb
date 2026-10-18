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
cmp    := col op lit | lit op col | col op col (same mapped type)
op     := = <> < <= > >=
```

Literals go through `Dialect::render_literal`, which returns `None` for any
value it cannot render exactly. That covers NULL (never pushed for
comparisons; `IS NULL` is separate), NaN/∞ floats, and types the dialect does
not know. A literal whose `ScalarValue` type differs from the column's mapped
type is rendered only when the dialect knows a safe widening (integer literal
against a decimal column, date literal against a date column). Otherwise the
conjunct stays local. This avoids PostgreSQL's implicit casts producing
different comparison semantics than Arneb's. `complete = true` only if every
conjunct translated, which gates `LIMIT` (D4).

Arneb's `LIKE` and PostgreSQL's `LIKE` agree on `%`/`_`, and both use `\` as
the default escape character. PostgreSQL `LIKE` is case-sensitive like Arneb's,
and `ILIKE` is not used. Collation can make `<`/`>` on text differ from Arneb's
byte order. Text **range** comparisons (`< <= > >=` on VARCHAR) are therefore
not pushed. Text equality and `IN` are pushed, because equality is
collation-independent for deterministic collations, which is PostgreSQL's
default.

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
column-named error. A batch is flushed every `batch_size` rows, and its
`get_array_memory_size()` is reserved from `ctx.memory_pool` before it is
emitted. That uses the same `MemoryReservation` pattern as the Parquet scan
(#137), and keeps the reservation in the stream until the batch is dropped
downstream, consistent with how the Parquet scan accounts decode buffers.

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

The factory's `ddl_provider()` returns `None`. If the existing DML path's
error for a connector without a DDL provider does not say "read-only", the
factory provides a `DDLProvider` whose every method returns
`ConnectorError::UnsupportedOperation("catalog '<name>' is read-only")`, so
the message is explicit.

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
- **New dependencies**: `tokio-postgres` and `deadpool-postgres`
  (MIT/Apache-2.0). `cargo deny` must pass.

## Migration Plan

Additive. No config migration. Rollback means removing the `postgresql`
catalog entry.

## Open Questions

None blocking. Histogram-based split bounds and TLS to PostgreSQL are recorded
follow-ups.
