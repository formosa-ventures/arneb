## Why

Arneb's stated purpose is federated queries, but today every catalog it can
mount is a lake format (Hive, Iceberg, files). Phase 3 milestone M2
(`docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`) makes the
second half of the "lakehouse + OLTP" scenario real. M1/P3 (TLS, auth) is done,
so it is now safe to point Arneb at an operational database.

The roadmap lists F1 (a shared RDBMS connector framework) and F2 (PostgreSQL)
as two changes designed together. This change lands them as **one** change: a
framework with no concrete dialect cannot be validated, and getting the
`Dialect` seam right needs a real database pushing back on it. F3 (MySQL, M4)
is where the framework proves it generalizes. F4 (cross-source TPC-H layout,
dynamic filters into PostgreSQL) stays a separate change.

## What Changes

- **New crate `crates/rdbms`** (`arneb-rdbms`), a remote-SQL connector framework:
  - A `Dialect` trait that owns identifier quoting, literal rendering, the
    translation of pushed-down `PlanExpr` filters into remote SQL, and remote
    type → Arneb `DataType` mapping. Adding a database means implementing one
    dialect plus a client.
  - Query generation (`SELECT <projected cols> FROM <table> WHERE <translated
    filters> [AND <split range>] [LIMIT n]`). Each filter conjunct is translated
    on its own. A conjunct that cannot be translated stays in Arneb's
    `FilterExec` and never fails the query. `LIMIT` is pushed only when every
    filter conjunct was translated.
  - Split planning: a table with a single-column integer primary key and enough
    estimated rows is read as N primary-key ranges, so several partitions and
    workers read in parallel. Every other table is read as one split.
- **New PostgreSQL connector `crates/postgresql`** (`arneb-postgresql`),
  built on `tokio-postgres` and `deadpool-postgres`:
  - `[[catalogs]] type = "postgresql"` with `connection_url`, `user`,
    `password`, and pool/split knobs. Schemas and tables are discovered from
    `information_schema`, and row-count estimates come from `pg_class.reltuples`.
  - Read-only. `CREATE` / `INSERT` / `DELETE` into a PostgreSQL catalog fail
    with a clear "read-only catalog" error.
  - Types: `smallint`, `integer`, `bigint`, `real`, `double precision`,
    `numeric(p,s)`, unconstrained `numeric`, `boolean`, `char(n)`,
    `varchar(n)`, `text`, `date`, `timestamp`, `timestamptz`, `uuid`, `bytea`.
    A column of any other type makes queries that read it fail with an error
    naming the column and the PostgreSQL type.
  - Data is read with `COPY (SELECT …) TO STDOUT (FORMAT binary)` and decoded
    straight into Arrow builders. The scan accounts its batches in the query's
    memory pool.
- **Server wiring**: the catalog/connector registration in `crates/server`
  accepts `type = "postgresql"`, which is registered on coordinators and workers
  alike. The Hive path is unchanged.
- **Local environment**: `docker-compose.yml` gains a `postgres` service seeded
  with a small fixture schema. The bundled Trino gets a `postgresql` catalog
  over the same database, so `trino-diff`-style comparisons are possible.
- **Docs**: `docs/connectors/postgresql.md`, a connectors overview entry, and
  the catalog options in `docs/guide/configuration.md`.

No **BREAKING** changes. Configs without a `postgresql` catalog behave exactly
as before.

## Capabilities

### New Capabilities

- `rdbms-connector`: the framework contract. Covers dialect responsibilities,
  which filters/projections/limits are pushed and when, the guarantee that
  untranslatable expressions never fail a query, split planning, and the
  generated SQL being observable in logs and `EXPLAIN`.
- `postgresql-connector`: the PostgreSQL catalog. Covers configuration,
  discovery, type mapping and its error cases, read-only behavior, connection
  pooling, credentials redaction, and availability on workers.

### Modified Capabilities

- `server-config`: `[[catalogs]]` accepts `type = "postgresql"` and its
  connection fields, with `ARNEB_*` overrides for the password.

## Impact

- **New crates**: `crates/rdbms`, `crates/postgresql`. Workspace members and
  dependencies gain `tokio-postgres` and `deadpool-postgres`, both
  MIT/Apache-2.0. `deny.toml` licenses need checking.
- **`crates/server`**: `config.rs` (`CatalogConfig` fields), `main.rs` (catalog
  registration loop).
- **`crates/execution`**: no trait changes expected. `ScanContext` already
  carries filters, projection and limit. If the planner needs to set
  `ScanContext.limit` for `Limit → TableScan` (it does not today), that is a
  small, connector-neutral planner change, recorded in `design.md`.
- **`docker-compose.yml`**: a new `postgres` service and a Trino catalog file.
- **Validation tiers**: Tier 1 covers dialect/translation/type unit tests and
  integration tests against a real PostgreSQL. Those tests are skipped unless
  `ARNEB_TEST_POSTGRES_URL` is set, so `cargo test` stays hermetic. Tier 2 is a
  local distributed run (coordinator + 2 workers) reading a PostgreSQL catalog,
  with results compared to Trino's `postgresql` catalog. No Tier 3: this change
  makes no performance claim.
