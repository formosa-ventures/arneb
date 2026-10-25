## Why

G6 in the roadmap's "Path to production": scenario 1 ("replace an internal
Trino cluster") needs evidence that real clients work and that concurrent
queries do not fail, checked automatically. Running the clients against a
local cluster for the first time showed that most did not get far:
psycopg2 and SQLAlchemy failed on `BEGIN`, psycopg 3 parameters arrived as
garbage or NULL, SQLAlchemy's Trino dialect failed on `schema.table.column`
and a quoted `"system"."metadata"` table, dbt-trino's catalog query failed on
`INFORMATION_SCHEMA` in upper case and `JOIN … USING`, and `FROM NATION`
failed on both protocols.

## What Changes

- pgwire: transaction statements (`BEGIN`, `START TRANSACTION`, `COMMIT`,
  `END`, `ROLLBACK`, `ABORT`) are accepted as no-ops; `version()` starts with
  `PostgreSQL 14.0`; startup parameters and `SHOW` agree (`server_version`,
  `server_version_num`, `standard_conforming_strings`, isolation level, …);
  `current_user` is the session user, not the catalog.
- pgwire Bind: parameters are decoded by their declared type and format
  (binary integers, floats, booleans, dates, timestamps); an undecodable
  binary parameter is an error instead of a silent NULL.
- Planner: table names that match nothing fold to lowercase; `[catalog.]
  schema.table.column` references; `INNER` / `LEFT JOIN … USING`;
  Trino's `current_catalog` / `current_schema`.
- Trino protocol: `"system"."metadata"` (quoted) reaches the virtual
  metadata tables.
- `scripts/client-smoke/`: a local cluster launcher, client checks
  (psycopg2, psycopg 3, Trino DB-API, SQLAlchemy Trino dialect), a read-only
  dbt project, and `run.sh`, which also runs the 22 TPC-H queries against
  DuckDB and the concurrency soak. A new CI job runs it on every PR.
- `scripts/k8s-smoke/gen_tpch.py` no longer doubles the data when rerun.
- Docs: `docs/guide/clients.md`.

PostgreSQL catalog introspection (`pg_catalog` as queryable tables,
`regclass`, …), which SQLAlchemy's `postgresql` dialect, dbt-postgres and
Metabase need, is the next change.

## Capabilities

### New Capabilities
- `client-compatibility`: what clients send that Arneb answers, and the CI
  acceptance job.

### Modified Capabilities

## Impact

- `crates/protocol` (metadata.rs, handler.rs, auth.rs, trino/metadata.rs),
  `crates/planner` (planner.rs), `crates/catalog` (lib.rs),
  `crates/sql-parser` (convert.rs).
- `.github/workflows/ci.yml`: the release build uploads the binary; new
  `Cluster smoke` job.
- No configuration change.
