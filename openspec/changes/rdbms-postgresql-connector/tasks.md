## 1. Workspace setup

- [ ] 1.1 Create `crates/rdbms` (`arneb-rdbms`) and `crates/postgresql` (`arneb-postgresql`), add them to the workspace members, and add `tokio-postgres` + `deadpool-postgres` to `[workspace.dependencies]`
- [ ] 1.2 `cargo deny --all-features check` passes with the new dependencies (licenses, advisories, duplicates)

## 2. Framework (`arneb-rdbms`)

- [ ] 2.1 `Dialect` trait (quoting, literal rendering, LIKE, supported binary ops) + `RemoteTable` / `RemoteColumn` (remote type name, `Result<DataType, String>` mapping)
- [ ] 2.2 Filter translation per D3: flatten AND conjuncts, all-or-nothing per conjunct, no NULL literals, no text range comparisons, no implicit casts; `Translation { sql, complete }`
- [ ] 2.3 `build_select`: projection (exact order, empty projection), WHERE from translation + split range, LIMIT only when `complete`
- [ ] 2.4 `SplitPlan`: `plan_splits(min, max, estimated_rows, split_min_rows, max_splits)` with open-ended first/last ranges; encode/decode via `rdbms.split_column` / `rdbms.split_bounds` properties
- [ ] 2.5 Unit tests: quoting/escaping, every supported and rejected filter shape (spec scenarios), projection order, limit gating, split coverage (disjoint + complete, including min == max and tiny ranges), property round-trip

## 3. PostgreSQL connector (`arneb-postgresql`)

- [ ] 3.1 `PostgresDialect` (double-quote identifiers, `''` escaping, literal rendering for int/float/decimal/date/timestamp/bool/text)
- [ ] 3.2 Type mapping from `information_schema.columns` per D5, incl. `numeric(p,s)`, unconstrained numeric, `timestamptz` → UTC, `uuid`, `bytea`, unsupported types deferred to scan time
- [ ] 3.3 Binary COPY decoder per D6 into Arrow builders (all mapped types, NULLs, exact numeric rescale with error on loss / NaN / ∞, date/timestamp ±infinity errors), batch flush at `batch_size`
- [ ] 3.4 Decoder unit tests from hand-built COPY byte streams (each type, NULLs, numeric edge cases, multi-chunk input split across `Bytes` boundaries)
- [ ] 3.5 Catalog: `PostgresCatalog` / `PostgresSchema` / `PostgresTable` — schema + table discovery (excluding system schemas), column order, `reltuples` statistics, PK detection, coordinator-side split plan into properties (D2)
- [ ] 3.6 `PostgresConnectorFactory` + `PostgresDataSource`: partitions from properties, per-partition SELECT + COPY stream, memory-pool reservation per batch, unsupported-column error when read, `arneb::rdbms` debug log of each remote SQL
- [ ] 3.7 Pooling + cancellation per D7 (`deadpool-postgres`, connection detached on mid-COPY drop)
- [ ] 3.8 Read-only behavior per D9: DML/DDL against the catalog fails with an explicit read-only error and sends nothing remote
- [ ] 3.9 Password redaction in `Debug` / logs / errors

## 4. Server + planner wiring

- [ ] 4.1 `CatalogConfig` fields (`connection_url`, `user`, `password`, `max_connections`, `split_min_rows`, `max_splits`, `unconstrained_numeric_scale`) with defaults; `ARNEB_CATALOG_<NAME>_PASSWORD` override; config tests
- [ ] 4.2 `main.rs` catalog loop dispatches `postgresql` → register catalog + connector (all roles); unreachable DB / missing `connection_url` logged and skipped; effective settings logged without password
- [ ] 4.3 Physical planner: `Limit { limit: Some(n), offset: None | Some(0) }` directly over `TableScan` sets `ScanContext.limit` (D4); unit test that other connectors' results are unchanged

## 5. Local environment + docs

- [ ] 5.1 `docker-compose.yml`: `postgres` service (host port 5433) with an init fixture (TPC-H `nation`/`region` + a type-coverage table + a split-able keyed table); Trino `postgresql` catalog file over the same DB
- [ ] 5.2 `docs/connectors/postgresql.md` (config, type mapping, pushdown rules, splits, read-only, no TLS-to-PG yet, `ANALYZE` note), connectors overview entry, `configuration.md` catalog options, sample config

## 6. Validation

- [ ] 6.1 Tier 1: integration tests against a real PostgreSQL, skipped unless `ARNEB_TEST_POSTGRES_URL` is set — discovery, every mapped type, unsupported-type error, pushed-vs-local result equality, split scan returns every row exactly once, LIMIT, pool bound, memory-limit failure
- [ ] 6.2 Tier 1: `cargo fmt -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` green
- [ ] 6.3 Tier 2: local coordinator + 2 workers with the PostgreSQL catalog — split table scanned across workers, distributed result == standalone result == Trino `postgresql` catalog result; remote SQL visible under `RUST_LOG=arneb::rdbms=debug`
- [ ] 6.4 File the Hive `splits_per_file` partition-agreement issue found in design (Risks)
