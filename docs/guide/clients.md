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
through the SQLAlchemy dialect checked above. Metabase is checked by hand
over pgwire ([below](#metabase)); in CI, the `metabase_corpus` protocol test
runs every statement Metabase sent during that check. Metabase's Trino
driver uses the Trino JDBC driver; the `system.jdbc` tables its metadata
queries read are covered by the protocol test suite, but that driver has
not been run against Arneb.

## Metabase

Checked by hand with Metabase v0.64.8.3 and its built-in **PostgreSQL**
driver (pgJDBC), on the client-smoke cluster (a coordinator and two workers
on TPC-H SF1). The steps and scripts are in
`scripts/client-smoke/metabase/README.md`.

### Connecting

Add a database of type **PostgreSQL**:

| Setting | Value |
|---|---|
| Host, Port | The coordinator and its pgwire port (`5432` by default) |
| Database name | Any name; Arneb does not use it (see below) |
| Username, Password | An Arneb user, if [authentication](/guide/configuration#authentication) is on; otherwise any user name |
| Use a secure connection (SSL) | Off, unless pgwire [TLS](/guide/configuration#tls) is configured |
| Additional JDBC connection string options | **`binaryTransfer=false`** (required, see the gaps below) |
| Schemas | All, or the schemas to show |

One Metabase database shows one Arneb catalog: the session's catalog, which
is the server's default (`current_database()` returns its name).
`information_schema` and `pg_catalog` describe only the session's catalog,
as PostgreSQL's describe only the current database. A client that issues
`SET search_path TO catalog.schema` sees that catalog instead. Metabase
sends no `SET search_path`, so it always shows the default catalog.

### What works

With `binaryTransfer=false`, Metabase's sync finds all 8 TPC-H tables and
61 columns, with the same Metabase types (`base_type`) and database types
as against PostgreSQL 17 holding the same DDL, and no statement is answered
by the `pg_catalog` fallback. These work, with the same results as DuckDB
on the same data apart from the day-of-week and decimal-average gaps below:
- native SQL questions, including a `{{variable}}` parameter;
- table preview, count, sum, average, min, max, standard deviation,
  distinct count, median (`percentile_cont`);
- grouping by month, year, quarter of year, week and a numeric binning;
- date range and relative date filters ("last 30 years"), `contains` and
  `starts with`;
- sorting by an aggregate with a limit, custom expressions (arithmetic,
  `CASE`, string functions, `datetime-diff`, `get-year`), cumulative count,
  explicit joins and nested questions;
- filter drop-downs (field values) and the x-ray of a table.

The SQL features this needed are general and documented in
[Expressions](/sql/expressions) (regex operators, intervals, `date - date`,
subscripts, field access, untyped literals) and
[Functions](/sql/functions) (`format`, `percentile_cont`/`percentile_disc`,
`current_schemas`, `col_description`).

### Known gaps

- **`binaryTransfer=false` is required.** Arneb sends every result column
  as text, even when the client asks for binary. pgJDBC asks for binary
  once a statement has run 5 times on a connection (it then prepares it on
  the server), and misreads the text: in a sync, some tables after the
  fifth end up with no fields, with
  `ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 1` in
  Metabase's log. Binary result encoding is a follow-up.
- **Day of week is ISO.** `EXTRACT(DOW ...)` follows Trino (Monday = 1 …
  Sunday = 7), not PostgreSQL (Sunday = 0), so Metabase's "day of week"
  grouping labels Sunday as day 8 instead of day 1. The counts per day are
  right.
- **Averages of decimals keep the column's scale** (Trino's rule), so
  Metabase shows `151219.54` where PostgreSQL shows `151219.5376…`.
- **No views or constraints in the catalog.** `views`, primary and foreign
  keys are always empty, so Metabase finds no keys and no table
  relationships; set them in the table metadata settings.
- **No row counts.** `pg_stat_user_tables.n_live_tup` is 0, so Metabase
  shows no estimated row counts.
- **Regular expressions are Rust's `regex` syntax.** POSIX-only syntax
  (back-references, look-around) in a native question or a regex filter is
  refused.
- **One-part intervals only.** `INTERVAL '1 day 2 hours'` and
  `INTERVAL '...' DAY TO SECOND` are refused; `INTERVAL '-30 year'` and
  `INTERVAL '3' MONTH` work.
- **Column names of unaliased expressions** are the expression's text
  (`upper(n_name)`, `1 + 1`), where PostgreSQL uses the function name or
  `?column?`. Metabase aliases every column it generates, so this only shows
  in native questions.

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
  - `information_schema` reads run in the engine the same way, over
    `schemata`, `tables`, `columns`, `views`, `table_constraints` and
    `key_column_usage`, and can join `pg_catalog` tables. They describe the
    session's catalog only (PostgreSQL's current database; set
    `search_path` to `catalog.schema` to switch). `columns` reports
    PostgreSQL's `data_type` and `udt_name` spellings. `views`,
    `table_constraints` and `key_column_usage` are always empty, and every
    table is a `BASE TABLE`; listing Arneb views there is a follow-up.
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
    the whole first `pg_catalog` table it names (or the whole of
    `information_schema.tables`, `columns` or `schemata` across every
    visible catalog), whatever it asks for. Each
    such answer is logged at `warn` on `arneb::protocol` with its SQL and
    counted in `arneb_pg_catalog_fallbacks_total`.
  - Array and correlated-subquery limits (see the expression and function
    pages for the full rules):
    - `array_agg` over `smallint`, `tinyint`, `bytea` and `decimal64`
      values fails at run time.
    - Aggregate `ORDER BY` works in `array_agg` only, and `WITHIN GROUP`
      in `percentile_cont`/`percentile_disc` only. `FILTER` is refused.
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
