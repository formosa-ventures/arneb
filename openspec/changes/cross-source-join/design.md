## Context

`rdbms-postgresql-connector` (#158) delivered a read-only PostgreSQL catalog on
the `arneb-rdbms` framework. It translates each scan's `ScanContext::filters`
into a remote `WHERE` clause (`crates/rdbms/src/translate.rs`). It has been
exercised only on the fixture schema in `docker/postgres/init`.

The dynamic-filter plumbing that a federated join depends on already exists,
and none of it is connector-specific:

- `AssignDynamicFilterIds` (`crates/planner/src/analyzer/assign_dynamic_filter_ids.rs`)
  tags any probe-side `TableScan` it can trace a join key to, whatever the
  connector.
- **Same-fragment filters.** `HashJoinExec::ensure_built` builds the hash
  table before the probe side runs, then calls
  `inject_inlist_dynamic_filters`, which pushes `probe_key IN (build keys)`
  into every matching `ScanExec.dynamic_filters`. The per-type cap is
  `dynamic_filter_cap`: 1,000,000 values for `Int64`, 200,000 for
  `Int32`/`Date32`.
- **Cross-fragment filters.** `ScanExec::collect_cross_fragment_filters`
  waits for coordinator-routed `Domain`s. This path is gated by
  `dynamic_filtering_enabled`, which is hard-coded off
  (`crates/execution/src/planner.rs:139`) except for probe-side semi-join
  filters.
- `ScanExec::execute` appends both kinds to a clone of the static
  `scan_context.filters`. It converts `Domain::DistinctValues` and
  `Domain::Range` with `domain_to_filter_expr`. A `Domain::Bloom` has no
  `PlanExpr` form and is never added.

So a PostgreSQL probe scan already receives `IN (...)` conjuncts, and the
translator already renders them. This exposes two gaps:

1. **The `IN` list has no size bound.** One `BIGINT` build side can put up
   to a million literals into the `COPY (SELECT …)` text, repeated once per
   primary-key split.
2. **A runtime conjunct cannot be identified.** `filters` mixes static and
   dynamic conjuncts, so the remote-SQL log cannot say which is which. Since
   the cap should apply only to dynamic conjuncts, the translator also
   cannot tell where to apply it.

The rest of F4 is benchmark infrastructure: a mixed layout, a query set that
runs on both engines, and the correctness and pushdown evidence.

## Goals / Non-Goals

**Goals:**

- Bound the remote SQL produced by dynamic filters. Above a per-catalog
  threshold, compact the `IN` list to a range, and record what was done.
- Make dynamic-filter conjuncts identifiable in the `arneb::rdbms` log.
- Provide a reproducible mixed TPC-H layout: dimensions in PostgreSQL and
  facts in Hive, with the same data and the same SQL for Arneb and Trino.
- Gate on 22/22 cell-identical results against Trino at one scale factor of
  SF1 or larger (validated here at SF10), standalone and
  distributed. Report SF10 latency and peak memory next to Trino.

**Non-Goals:**

- Turning cross-fragment dynamic filtering on by default. That decision
  belongs to its own measured change (see the comment at
  `planner.rs:128`).
- Bloom-filter pushdown into remote SQL.
- Join, aggregate or TopN pushdown (M4).
- Remote SF30 runs. This change rests on Tier 1 and Tier 2, plus a local
  SF10 report.

## Decisions

### D1 — Mark the dynamic-filter boundary in `ScanContext`, not a second list

`ScanContext` gets `dynamic_filters_start: Option<usize>`, the index in
`filters` where runtime conjuncts begin. `ScanExec::execute` sets it to the
static filter count whenever it appends dynamic or cross-fragment filters.
File connectors ignore it and keep using `filters` unchanged.

*Alternative:* a separate `dynamic_filters: Vec<PlanExpr>` field. It would
be cleaner in isolation, but the Parquet row-filter path
(`parquet_scan.rs`), `HiveDataSource` and `FileDataSource` all read
`filters` today, and every one of them would need to concatenate the two
lists. That is churn on measured hot paths for no behavior change. An index
keeps them byte-identical.

### D2 — Compact in the RDBMS translator, at a per-catalog threshold

`translate_filters` gets a `TranslateOptions { dynamic_from, compaction_threshold }`.
For each conjunct at or after `dynamic_from`:

- `col IN (v1..vn)` with `n <= threshold` is translated as it is today.
- With `n > threshold`, every literal must be the same orderable type
  (`Int32`, `Int64`, `Date32`, or `Decimal128` with a single scale). If so,
  the conjunct becomes `col BETWEEN min AND max`. If not, it is not pushed.
- A compacted conjunct marks the translation incomplete (`complete = false`),
  so `LIMIT` is never pushed on a remote query that would return a superset.

Static conjuncts are never compacted. A user-written `IN` list is sent
exactly as written.

The threshold is a catalog property, `domain_compaction_threshold`, the
same name Trino's JDBC connectors use, with default **1000**. It is a
property of the remote database, not of the Arneb process, so it lives in
`[[catalogs]]` next to the pool and split knobs, and it is logged when the
catalog is registered.

PostgreSQL 14+ executes a long `IN` list as a hashed `ScalarArrayOp`, so
execution cost is not what limits it. The limit is SQL text size times the
number of splits, plus parse time. 1000 integer literals are about 8 KB per
split.

*Alternatives:*

- Compact in `ScanExec`. Rejected: the threshold is connector-specific. The
  Parquet row filter handles a million-value set efficiently through its own
  hash set, and capping it there would regress Hive scans.
- Send large sets as a bound array parameter (`= ANY($1)`). Rejected for
  now: the scan uses text `COPY (SELECT …)`, which takes no parameters.
  Revisit if the measured SF10 numbers show the range is too weak.

### D3 — Soundness rests on the join, not on the pushed filter

A dynamic filter is only an optimization: the join that produced it still
compares every probe key against the build side. A weaker remote predicate
(a range), or no remote predicate (Bloom, untranslatable, timed out), can
therefore never change a result. It only changes how many rows PostgreSQL
returns. The tests assert this directly: a scan with a compacted filter
returns a superset of the exact filter's rows, and the query result is
identical.

### D4 — Observability: counts on the existing log event

The existing `remote query` event on `arneb::rdbms` (`postgresql/src/source.rs`)
gains three fields:

- `dynamic_pushed`: dynamic-filter conjuncts pushed as they were;
- `dynamic_compacted`: dynamic `IN` lists compacted to a range;
- `dynamic_skipped`: dynamic conjuncts not pushed.

The SQL text already shows the predicates themselves. The counts let a test
or a grep tell a runtime filter from a static one without parsing SQL.

### D5 — PostgreSQL seed: Trino CTAS from the `tpch` connector

A `pg-tpch-seed` compose profile (`docker/postgres-tpch-seed/seed.sh`) runs
on the Trino image, like `tpch-seed`. It runs `CREATE TABLE
postgresql.<schema>_mixed.<dim> AS SELECT …` for the six dimension tables (the
postgres fixture already owns schema `tpch`, with deliberately `char(25)`
columns, so the TPC-H copy gets its own schema). It uses
the **same column-renaming SELECTs** as the Hive seed, moved into a shared
`docker/tpch-seed/tables.sh` that both scripts source. That keeps the
schema name, the decimal/double choice and the values identical by
construction. The script then calls `postgresql.system.execute` to add the
TPC-H primary keys and to `ANALYZE`. The primary keys enable the
connector's PK-range splits, and `ANALYZE` makes `reltuples` reflect the
loaded rows. Inserts run with `non_transactional_insert=true`, which skips
the staging-table copy.

*Alternatives:* dbgen + `COPY` would be faster at SF10, but it is a second
data generator whose output would have to be proven identical to Trino's
`tpch` connector. An export to CSV followed by `COPY` adds a shared volume
and a second container. At SF10, CTAS through JDBC takes minutes, which is
acceptable for a one-time seed.

### D6 — One query set, catalog-qualified, generated

`benchmarks/tpch/scripts/qualify_queries.py` rewrites `FROM <t>` /
`JOIN <t>` for the eight TPC-H table names into
`postgresql.tpch_mixed.<t>` (dimensions) or `hive.tpch.<t>` (facts). It
writes the result to `benchmarks/tpch/queries-mixed/`, which is checked in
so diffs stay reviewable. All 22 stock queries use explicit `JOIN` syntax,
so a `FROM|JOIN` anchor finds every table reference, and it does not touch
`EXTRACT(YEAR FROM o_orderdate)` or the column alias `AS nation`. The script
also fails if a stock table name survives unqualified in a relation
position.

`benchmarks/tpch/tpch-mixed.toml` names Arneb's catalogs `hive` and
`postgresql`, the same names as the bundled Trino's. Trino is then run with
**no default catalog**, so an unqualified table that slipped through fails
loudly instead of silently reading the all-Hive layout.

### D7 — Correctness gate and pushdown evidence

`blast_radius_oracle.py` gets `--layout mixed`, which:

- reads `queries-mixed/`;
- drops the `search_path` / `--catalog` defaults;
- accepts a host-run Arneb (`--arneb-host`), so the gate also works without
  the dockerized bench stack.

Pushdown evidence comes from two independent sources:

- Arneb's own `arneb::rdbms` events (`RUST_LOG=arneb::rdbms=debug`). This is
  the primary evidence and is unit-tested.
- PostgreSQL's statement log. The compose `postgres` service runs with
  `log_statement=all` and `log_line_prefix='%m [%a] '`. Arneb already sets
  `application_name=arneb`, so its statements are separable from Trino's.
  To avoid `--since` window artifacts, the harness brackets each query with
  a `SELECT 'arneb-mark <q> <begin|end>'` sent through `psql`, and slices
  the log between the markers.

### D8 — Standalone scalar subqueries (found by the gate, fixed here)

The first standalone mixed run failed q11. The cause was pre-existing and has
nothing to do with PostgreSQL: the all-Hive q11 fails the same way on a
standalone server. Every earlier TPC-H gate ran on the distributed bench
cluster, where the coordinator's prepass turns scalar subqueries into literals
before execution. The standalone path had two gaps:

1. `register_data_sources` walked plan nodes but not expressions, so a table
   read only inside a scalar subquery had no data source.
2. `ExecutionContext::resolve_scalar_subqueries` descended only through
   `BinaryOp`, and the local path did not resolve subqueries nested inside a
   subquery's own plan. q11's `SUM(..) / CAST((SELECT COUNT(*) ..) AS ..)`
   hits both.

The fix walks expressions when registering, resolves nested subplans first,
and looks through `Cast` / `UnaryOp`. It is scoped to the shapes the analyzer
produces and is covered by three tests in `crates/protocol/src/handler.rs`.

## Risks / Trade-offs

- **The range compaction is weak on scattered keys.** The q02/q09 `partkey`
  sets are spread across the key space, so min..max prunes little →
  measure it; the SF10 report records per-query rows returned by
  PostgreSQL. If the range turns out useless, the `= ANY($1)` follow-up is
  already scoped (D2).
- **`log_statement=all` volume at SF10.** The statements themselves are
  small (one `COPY` per split per scan), so this is fine. The setting lives
  only in the local compose file, never in a shipped config.
- **The seed takes a long time at SF10** → the scale factor defaults to
  SF1, and SF10 is opt-in via `TPCH_SF=sf10`, the same as the Hive seed.
- **The regex-based qualifier is fragile against future query edits.** The
  generator fails when a table name stays unqualified, and Trino runs with no
  default catalog, so a gap fails loudly.
- **Same-fragment filters fire only where the join and the probe scan share
  a fragment.** In distributed mode that means broadcast joins; a
  partitioned join's probe scan sees only cross-fragment filters, which are
  default-off. The SF10 report labels each query by which dynamic filters
  actually reached PostgreSQL, so we do not overstate coverage.

## Migration Plan

This change is purely additive. A catalog without `domain_compaction_threshold`
gets the default. File connectors ignore `dynamic_filters_start`, and their
behavior is byte-identical. To roll back, revert the change: no data or
config migration is involved.

## Open Questions

- Should the default threshold move after the SF10 measurement? D2's
  default of 1000 is a starting point, and the SF10 report either confirms
  it or proposes a new value.
