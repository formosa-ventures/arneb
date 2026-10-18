## 1. Workspace setup

- [x] 1.1 Create `crates/rdbms` (`arneb-rdbms`) and `crates/postgresql` (`arneb-postgresql`), add them to the workspace members, and add `tokio-postgres` + `deadpool-postgres` to `[workspace.dependencies]`
- [x] 1.2 `cargo deny --all-features check` passes with the new dependencies (licenses, advisories, duplicates)

## 2. Framework (`arneb-rdbms`)

- [x] 2.1 `Dialect` trait (quoting, literal rendering, LIKE, supported binary ops) + `RemoteTable` / `RemoteColumn` (remote type name, `Result<DataType, String>` mapping)
- [x] 2.2 Filter translation per D3: flatten AND conjuncts, all-or-nothing per conjunct, no NULL literals, no text range comparisons, no implicit casts; `Translation { sql, complete }`
- [x] 2.3 `build_select`: projection (exact order, empty projection), WHERE from translation + split range, LIMIT only when `complete`
- [x] 2.4 `SplitPlan`: `plan_splits(min, max, estimated_rows, split_min_rows, max_splits)` with open-ended first/last ranges; encode/decode via `rdbms.split_column` / `rdbms.split_bounds` properties
- [x] 2.5 Unit tests: quoting/escaping, every supported and rejected filter shape (spec scenarios), projection order, limit gating, split coverage (disjoint + complete, including min == max and tiny ranges), property round-trip

## 3. PostgreSQL connector (`arneb-postgresql`)

- [x] 3.1 `PostgresDialect` (double-quote identifiers, `''` escaping, literal rendering for int/float/decimal/date/timestamp/bool/text)
- [x] 3.2 Type mapping from `information_schema.columns` per D5, incl. `numeric(p,s)`, unconstrained numeric, `timestamptz` → UTC, `uuid`, `bytea`, unsupported types deferred to scan time
- [x] 3.3 Binary COPY decoder per D6 into Arrow builders (all mapped types, NULLs, exact numeric rescale with error on loss / NaN / ∞, date/timestamp ±infinity errors), batch flush at `batch_size`
- [x] 3.4 Decoder unit tests from hand-built COPY byte streams (each type, NULLs, numeric edge cases, multi-chunk input split across `Bytes` boundaries)
- [x] 3.5 Catalog: `PostgresCatalog` / `PostgresSchema` / `PostgresTable` — schema + table discovery (excluding system schemas), column order, `reltuples` statistics, PK detection, coordinator-side split plan into properties (D2)
- [x] 3.6 `PostgresConnectorFactory` + `PostgresDataSource`: partitions from properties, per-partition SELECT + COPY stream, decode-buffer memory reservation (Parquet pattern), unsupported-column error when read, `arneb::rdbms` debug log of each remote SQL
- [x] 3.7 Pooling + cancellation per D7 (`deadpool-postgres`, connection detached on mid-COPY drop)
- [x] 3.8 Read-only behavior per D9: DML/DDL against the catalog fails with an explicit read-only error and sends nothing remote
- [x] 3.9 Password redaction in `Debug` / logs / errors

## 4. Server + planner wiring

- [x] 4.1 `CatalogConfig` fields (`connection_url`, `user`, `password`, `max_connections`, `split_min_rows`, `max_splits`, `unconstrained_numeric_scale`) with defaults; `ARNEB_CATALOG_<NAME>_PASSWORD` override; config tests
- [x] 4.2 `main.rs` catalog loop dispatches `postgresql` → register catalog + connector (all roles); unreachable DB / missing `connection_url` logged and skipped; effective settings logged without password
- [x] 4.3 Physical planner: `Limit { limit: Some(n), offset: None | Some(0) }` directly over `TableScan` sets `ScanContext.limit` (D4); unit test that other connectors' results are unchanged

## 5. Local environment + docs

- [x] 5.1 `docker-compose.yml`: `postgres` service (host port 5433) with an init fixture (TPC-H `nation`/`region` + a type-coverage table + a split-able keyed table); Trino `postgresql` catalog file over the same DB
- [x] 5.2 `docs/connectors/postgresql.md` (config, type mapping, pushdown rules, splits, read-only, no TLS-to-PG yet, `ANALYZE` note), connectors overview entry, `configuration.md` catalog options, sample config

## 6. Found during validation

- [x] 6.0a Stacked-filter pushdown opt-in (`DataSource::pushdown_stacked_filters`, D10), incl. through pruning projections; planner tests
- [x] 6.0b Translator looks through lossless widening casts; float and `char(n)` columns never pushed (D3); tests
- [x] 6.0c `ScanExec` returns an empty stream for a scan task that owns no partition (D11); unit test
- [x] 6.0d pgwire timestamp text keeps fractional seconds and pre-1970 instants (D11); unit test
- [x] 6.0e PostgreSQL catalogs default to schema `public` when `default_schema` is unset

## 7. Validation

- [x] 7.1 Tier 1: integration tests against a real PostgreSQL, skipped unless `ARNEB_TEST_POSTGRES_URL` is set — discovery, every mapped type, unsupported-type error, pushed-vs-local result equality, split scan returns every row exactly once, LIMIT, pool bound, memory reservation released after the scan
- [x] 7.2 Tier 1: `cargo fmt -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` green
- [x] 7.3 Tier 2: local coordinator + 2 workers with the PostgreSQL catalog — split table scanned across both workers; distributed results == PostgreSQL's own results for the fixture queries (only diff: unconstrained `numeric` printed at its mapped scale 10); Hive × PostgreSQL join == Trino (hive × tpch connector); full remote WHERE visible under `RUST_LOG=arneb::rdbms=debug`. Trino's `postgresql` catalog was not used: the shared Trino container was not restarted to load it
- [x] 7.4 File the Hive `splits_per_file` partition-agreement issue found in design (Risks) — #157
