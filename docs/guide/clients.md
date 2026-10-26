# Client Compatibility

Which BI and ELT clients are checked against Arneb, how, and what does not
work yet. The checks run on every pull request in the **Cluster smoke** CI
job (`scripts/client-smoke/run.sh`): a coordinator and two workers on TPC-H
SF1 Parquet, with the client versions pinned in
`scripts/client-smoke/requirements.txt`.

## Checked in CI

| Client | Protocol | What is checked |
|---|---|---|
| psycopg2 2.9 | pgwire | Default (non-autocommit) mode: statements inside `BEGIN … COMMIT`, a bound parameter, `version()` |
| psycopg 3.3 | pgwire | Server-side parameters (integers, floats, booleans, dates and timestamps sent in binary) |
| `trino-python-client` 0.340 (DB-API) | Trino | `SHOW CATALOGS`, `current_catalog` / `current_schema`, an upper-case table name |
| SQLAlchemy 2.1 Trino dialect (Superset's path) | Trino | Inspector: schemas, tables, columns, keys, indexes, table comment; a reflected table queried with `GROUP BY` / `ORDER BY` / `LIMIT` |
| dbt-trino 1.10 | Trino | `dbt debug`, `dbt test` (not-null, unique, relationships, accepted-values tests on sources and an ephemeral model), `dbt show`, `dbt docs generate` |
| dbt-postgres 1.11 | pgwire | `dbt debug` only (see below) |

The same job checks the 22 TPC-H queries against DuckDB and runs 5 rounds of
20 concurrent queries with the admission cap lifted, failing on any error.

Superset itself and Metabase are not started in CI. Superset reaches Trino
through the SQLAlchemy dialect checked above. Metabase's Trino driver uses
the Trino JDBC driver; the `system.jdbc` tables its metadata queries read
are covered by the protocol test suite, but Metabase itself has not been run
against Arneb yet.

## Not supported yet

- **Read-only.** dbt models can be `ephemeral` only: `table`, `view` and
  `incremental` need `CREATE TABLE AS` / `CREATE VIEW` on a writable
  connector. `dbt seed` and snapshots need writes too.
- **PostgreSQL catalog introspection.** Over pgwire, `pg_catalog` is
  answered by matching the statement text, not by queryable tables, so
  catalog queries that join `pg_catalog` tables, reference them without the
  `pg_catalog.` prefix, or cast to `regclass` / `oid` fail. That blocks
  SQLAlchemy's `postgresql` dialect (it probes `pg_type` on connect),
  dbt-postgres beyond `dbt debug`, and Metabase's and Superset's PostgreSQL
  drivers. Use the Trino protocol for these tools for now.
- **Several statements in one simple-query message** (`BEGIN; SELECT 1;
  COMMIT`) are refused; drivers send them one at a time.
- **`JOIN … USING`** works with `INNER` and `LEFT` joins. `RIGHT` / `FULL`
  joins with `USING`, and `NATURAL JOIN`, are refused.
- **Binary parameters** other than integers, floats, booleans, text, dates
  and timestamps (for example `bytea`, `uuid`, `numeric`) are refused with
  SQLSTATE `0A000`; send them in text format.

## Running it locally

```bash
python3 -m venv .venv && .venv/bin/pip install -r scripts/client-smoke/requirements.txt
.venv/bin/python scripts/k8s-smoke/gen_tpch.py /tmp/tpch 1
cargo build --release --bin arneb
PYTHON=.venv/bin/python scripts/client-smoke/run.sh /tmp/tpch
```

The cluster listens on pgwire `15432`, Trino `18080` and the Web UI `16432`
(`PG_PORT` / `TRINO_PORT` change them), and stops when the script exits.
`scripts/client-smoke/cluster.sh start DATA_DIR RUN_DIR` starts the same
cluster on its own for manual testing.
