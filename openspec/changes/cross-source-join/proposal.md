## Why

M2's goal is the "lakehouse + OLTP" scenario: one SQL statement that joins
Hive/Iceberg tables with PostgreSQL tables, correctly, with the work pushed
into the remote database. F1 and F2 (`rdbms-postgresql-connector`, #158) give
Arneb a PostgreSQL catalog, but it has only been tested on a small fixture
schema and on single-source queries. Nothing yet shows that a federated join
returns the same result as Trino, or that predicates known only at runtime
reach PostgreSQL. F4 is the last item in M2
(`docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`), and it is what
turns the connector into a scenario we can demonstrate.

Most of the plumbing exists, but nobody has checked it end to end. `ScanExec`
already folds resolved cross-fragment dynamic filters into
`ScanContext::filters` as `IN (...)` / range `PlanExpr`s, and the RDBMS
translator already renders `IN` and `BETWEEN`, so a resolved filter is
already translated into the remote `WHERE` today. Nobody has checked that
end to end, and reading the code shows two gaps. First, the same-fragment
hash-join filter (`inject_inlist_dynamic_filters`) emits up to 1M `BIGINT`
values, and nothing caps how many of them become remote SQL text. Second, a
dynamic-filter conjunct cannot be told apart from a static one in the
remote-SQL log. Whether the filters reach PostgreSQL in the mixed TPC-H
queries, and whether they help there, has not been measured. This change
measures it and closes the gaps.

## What Changes

- **Mixed TPC-H layout in the local environment.** A new seed step (a
  `docker compose` profile, like `tpch-seed`) loads the six dimension tables
  (`nation`, `region`, `customer`, `supplier`, `part`, `partsupp`) into
  PostgreSQL under a `tpch_mixed` schema (the connector's test fixture already
  owns `tpch`). It uses the same scale factor and the
  same decimal/double choice as the Hive seed, and it adds the TPC-H primary
  keys so the connector's primary-key range splits apply. The fact tables
  (`orders`, `lineitem`) stay in Hive. Trino's existing `postgresql` and
  `hive` catalogs read the same two stores, so both engines query
  byte-identical data.
- **One query set for both engines.** The 22 TPC-H queries get
  catalog-qualified table names (dimensions in `postgresql.tpch_mixed.*`, facts in
  `hive.tpch.*`). They are generated from the stock queries, not copied by
  hand. A mixed Arneb config (`benchmarks/tpch/tpch-mixed.toml`) names its
  catalogs exactly like Trino's, so the same SQL text runs unchanged on both
  engines.
- **Dynamic-filter pushdown into PostgreSQL.** When a PostgreSQL scan is the
  probe side of a join, the build side's resolved key domain becomes part of
  the remote `WHERE` clause:
  - a set of distinct values up to a configurable cap becomes `col IN (...)`;
  - a set over the cap is compacted to `col BETWEEN min AND max`, a weaker
    filter that is still sound because the join itself checks every key;
  - a `Bloom` domain (no bounds, and off unless `ARNEB_BLOOM_DF=1`), a
    domain that cannot be translated, or one that does not arrive before the
    wait timeout is not pushed. It never fails the query: the scan reads
    unfiltered, and the join keeps the result correct.

  Cross-fragment dynamic filtering stays default-off
  (`dynamic_filtering_enabled`, see `crates/execution/src/planner.rs`).
  Turning it on is not part of this change. A cross-fragment domain that
  does arrive is translated by the same rules.
- **Pushdown evidence.** The `arneb::rdbms` remote-SQL log event records
  which conjuncts came from dynamic filters, so a test or a reviewer can tell
  a static filter from a runtime one. The benchmark harness can capture
  PostgreSQL's own statement log for a run, which is the roadmap's acceptance
  evidence.
- **Correctness gate and SF10 report.** The 22 mixed queries are
  cell-diffed against Trino on the same layout, at SF1 or larger (this
  change runs it at SF10), in
  both standalone and distributed mode (coordinator + 2 workers). We then
  report mixed-layout SF10 latency and peak memory next to Trino. There is
  no win threshold: M2 gates on correctness and pushdown behavior.

No **BREAKING** changes. Single-source queries and configs without a
PostgreSQL catalog behave exactly as before. The new size cap is a runtime
knob (`ARNEB_*`, logged at startup).

## Capabilities

### New Capabilities

- `federated-tpch-benchmark`: the mixed-layout benchmark contract. It covers
  the PostgreSQL seed step and its parity with the Hive seed, the generated
  catalog-qualified query set, the shared catalog naming between Arneb and
  Trino, the 22/22 cell-identical gate (standalone and distributed),
  how PostgreSQL's statement log is captured, and the SF10 latency/memory
  report.

### Modified Capabilities

- `rdbms-connector`: a new requirement for dynamic-filter pushdown. It
  covers `IN`-list rendering up to a size cap, range compaction above the
  cap, `Bloom` domains not being pushed, the soundness guarantee on timeout or
  untranslatable domains, and dynamic-filter conjuncts being identifiable in
  the "Observable remote SQL" log. *Prerequisite:* `rdbms-postgresql-connector`
  (30/30 tasks done) is archived first, so this delta applies to a spec that
  exists in `openspec/specs/`.
- `server-config`: `postgresql` catalog entries accept
  `domain_compaction_threshold` (default 1000), which is logged with the
  other effective settings.

## Impact

- **`crates/rdbms`**: `translate.rs` / `select.rs` gain range fallback and
  the `IN`-list size cap. The remote-SQL log marks which conjuncts came from
  dynamic filters.
- **`crates/postgresql`**: `source.rs` passes the dynamic-filter boundary
  and the cap to the translator.
- **`crates/execution`**: `ScanContext` records where its dynamic-filter
  conjuncts start, so connectors can tell them from static filters. File
  connectors ignore the new field. Consumer assignment
  (`analyzer/assign_dynamic_filter_ids.rs`) is already connector-agnostic,
  so the planner does not change.
- **`crates/protocol` / `crates/execution`**: a pre-existing standalone-mode
  bug in scalar-subquery handling, found by the mixed gate (q11), is fixed;
  see `design.md` D8.
- **Local environment**: `docker-compose.yml` gets a PostgreSQL TPC-H seed
  profile (`docker/postgres-tpch-seed/`). The `postgres` service may need
  `log_statement` enabled when the benchmark asks for it. Seeding SF10
  through Trino's JDBC writer is slow (`partsupp` is 8M rows), and
  `design.md` picks the loading method.
- **Benchmarks**: `benchmarks/tpch/` gets a mixed config, the query-set
  generator, and a `--layout mixed` path in the correctness gate
  (`blast_radius_oracle.py`) and the runner scripts.
- **Docs**: a "Cross-source joins" section in `docs/connectors/postgresql.md`.
  The performance page gets the mixed-layout SF10 table, labeled with its
  tier.
- **Validation tiers**: Tier 1 (`cargo test`) covers domain → SQL
  translation and its fallbacks. Tier 2 (local distributed, SF10 here) covers the
  22/22 gate and the PostgreSQL log capture. The SF10 report runs locally,
  and no remote SF30 run is needed for this change.
