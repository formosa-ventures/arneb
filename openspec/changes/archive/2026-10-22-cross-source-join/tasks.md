## 1. Dynamic-filter boundary in ScanContext

- [x] 1.1 Add `ScanContext::dynamic_filters_start: Option<usize>` (default `None`) with a doc comment
- [x] 1.2 `ScanExec::execute` sets it to the static filter count when it appends same-fragment or cross-fragment dynamic filters
- [x] 1.3 Unit test: a scan with injected dynamic filters hands its source a context whose boundary points at the first dynamic conjunct; a scan without any leaves it `None`

## 2. RDBMS translator: compaction and counts

- [x] 2.1 `TranslateOptions { dynamic_from, compaction_threshold }` and a `translate_filters_with` entry point; `translate_filters` keeps its behavior
- [x] 2.2 Dynamic `IN` over the threshold → `BETWEEN min AND max` for one orderable literal type (`Int32`/`Int64`/`Date32`/single-scale `Decimal128`), else not pushed; a compacted conjunct makes the translation incomplete
- [x] 2.3 `Translation` reports `dynamic_pushed`, `dynamic_compacted` and `dynamic_skipped` counts
- [x] 2.4 Unit tests: small dynamic list exact, large list compacted, static list never compacted, mixed-type large list skipped, compaction disables LIMIT, counts

## 3. PostgreSQL connector wiring

- [x] 3.1 `domain_compaction_threshold` catalog option (default 1000, ≥ 1), parsed in server config, validated, and logged with the effective settings
- [x] 3.2 `PostgresDataSource` passes the boundary and threshold to the translator; the `remote query` event carries the three counts
- [x] 3.3 Live test (behind `ARNEB_TEST_POSTGRES_URL`): a join whose PostgreSQL probe scan receives a compacted dynamic filter returns the same rows as with the filter disabled

## 4. Local environment

- [x] 4.1 Move the TPC-H column-renaming SELECTs into `docker/tpch-seed/tables.sh`, sourced by `tpch-seed`; Hive seed output unchanged
- [x] 4.2 `docker/postgres-tpch-seed/seed.sh` + `pg-tpch-seed` compose profile: CTAS the six dimensions into `postgresql.<schema>`, add primary keys, `ANALYZE`
- [x] 4.3 `postgres` service logs statements with `application_name`
- [x] 4.4 Seed SF1 and confirm row counts and column types match `hive.<schema>` for all six tables

## 5. Benchmark harness

- [x] 5.1 `benchmarks/tpch/scripts/qualify_queries.py` → checked-in `benchmarks/tpch/queries-mixed/` (fails on an unqualified table reference)
- [x] 5.2 `benchmarks/tpch/tpch-mixed.toml` (catalogs `hive` + `postgresql`) and the docker bench overlay `docker/arneb-bench/docker-compose.mixed.yml`
- [x] 5.3 `blast_radius_oracle.py --layout mixed` (mixed queries, no default catalog on Trino, host-run Arneb)
- [x] 5.4 Pushdown-evidence helper: marker-bracketed slice of the PostgreSQL log per query, reporting each Arneb statement's `WHERE`

## 5b. Standalone scalar-subquery fix (found by the mixed gate)

- [x] 5b.1 `register_data_sources` registers tables read only from a scalar subquery inside a `Filter` / `Projection` expression
- [x] 5b.2 Without workers, scalar subqueries nested inside another subquery's plan are resolved first, and both resolvers look through `Cast` / `UnaryOp` (TPC-H Q11's `SUM(..) / CAST((SELECT COUNT(*) ..) AS ..)`)
- [x] 5b.3 Regression tests in `crates/protocol/src/handler.rs`: subquery-only table, nested subquery, full Q11 shape

## 6. Validation

- [x] 6.1 Tier 1: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test` for the touched crates
- [x] 6.2 Tier 2: mixed (SF10) 22/22 cell-identical vs Trino, standalone
- [x] 6.3 Tier 2: mixed (SF10) 22/22 cell-identical vs Trino, distributed (coordinator + 2 workers)
- [x] 6.4 Pushdown evidence captured: q05 static filter + projection visible in the PostgreSQL log; at least one query shows a dynamic filter in the PostgreSQL log
- [x] 6.5 SF10 mixed run, Arneb vs Trino: latency, peak memory, per-query "dynamic filter reached PostgreSQL" column → `measurements/`

## 7. Docs

- [x] 7.1 `docs/connectors/postgresql.md`: cross-source joins, dynamic filters, `domain_compaction_threshold`
- [x] 7.2 `docs/guide/configuration.md`: the new catalog option
- [x] 7.3 Update the Phase 3 roadmap progress note (F4) and the CLAUDE.md local-environment commands
