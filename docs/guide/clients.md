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
| SQLAlchemy 2.1 postgresql dialect (psycopg2 and psycopg 3) | pgwire | Inspector: schemas, tables, `has_table`, views, columns, primary key, foreign keys, indexes, unique and check constraints (the last five empty, because Arneb has none); `Table(..., autoload_with=engine)` queried with `GROUP BY` / `ORDER BY` / `LIMIT`; no `pg_catalog` fallback answer is counted. Also checks that arrays needing quotes round-trip through psycopg 3 |
| dbt-postgres 1.11 | pgwire | `dbt debug`, `dbt test`, `dbt show`, `dbt docs generate` (the same project as dbt-trino) |

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
- **PostgreSQL catalog introspection** is partial.
  - Over pgwire, `pg_catalog` reads run in the engine over tables that
    describe the session's catalog: `pg_namespace`, `pg_class`,
    `pg_attribute`, `pg_type`, `pg_database`, `pg_roles`, `pg_am` and
    `pg_tablespace`. `pg_depend`, `pg_description`, `pg_attrdef` and
    `pg_constraint` are there too, always empty.
  - These work: joins, filters, unqualified names, `'name'::regclass`,
    `= ANY (ARRAY[...])`, and the
    [catalog functions](/sql/functions#postgresql-catalog-functions).
  - Reflection queries from SQLAlchemy's postgresql dialect run in the
    engine, with `ARRAY[...]`, `array_agg`, `unnest`, `generate_subscripts`,
    `json_build_object` and correlated subqueries in the `SELECT` list (see
    [Expressions](/sql/expressions) and [Functions](/sql/functions)). Over
    `pg_catalog`, `pg_index`, `pg_inherits`, `pg_sequence`, `pg_collation`
    and `pg_opclass` are empty, and `current_setting()` answers a fixed set
    of settings.
  - A statement the engine cannot plan still gets the old fixed answer:
    the whole first `pg_catalog` table it names, whatever it asks for. Each
    such answer is logged at `warn` on `arneb::protocol` with its SQL and
    counted in `arneb_pg_catalog_fallbacks_total`. Metabase's PostgreSQL
    driver has not been tried.
  - Array and correlated-subquery limits (see the expression and function
    pages for the full rules):
    - `array_agg` over `smallint`, `tinyint`, `bytea` and `decimal64`
      values fails at run time.
    - Aggregate `ORDER BY` works in `array_agg` only. `FILTER` and
      `WITHIN GROUP` are refused.
    - `unnest` and `generate_subscripts` work only as whole `SELECT`
      items. `generate_subscripts` takes dimension 1 only, and an untyped
      argument is refused.
    - A correlated subquery in the `SELECT` list needs at least one
      equality correlation. A correlated subquery over a CTE, a derived
      table or `LATERAL`, or correlated through two levels, is refused.
    - A `smallint` column compared with a literal is widened, so the
      comparison is not pushed into the connector.
  - A query that mixes `pg_catalog` tables with your own tables cannot see
    your tables.
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
