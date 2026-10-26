## Why

PostgreSQL clients find tables and columns by querying `pg_catalog`, and
over pgwire Arneb answers those queries with fixed results. A handler
matches a table name in the SQL text and returns the whole table, whatever
the query asks for. Joins, filters, unqualified names (`FROM pg_class`) and
casts such as `'pg_rewrite'::regclass` therefore do not work. That is the
gap G6 left (#185, stage 2).

The queries below were recorded from a real PostgreSQL, and every one of
them fails or returns the wrong rows on Arneb:
- dbt-postgres fails at the start of `dbt test`, `dbt show` and
  `dbt docs generate`.
- SQLAlchemy's postgresql dialect cannot list schemas or tables.

## What Changes

- pgwire statements that read the `pg_catalog` tables Arneb provides run in
  the engine, over a per-statement in-memory `pg_catalog` schema built from
  the session's catalog. Filters, joins, `DISTINCT`, `ORDER BY` and
  unqualified names then behave as in PostgreSQL.
  - The tables are `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`,
    `pg_database`, `pg_roles`, `pg_am` and `pg_tablespace`.
  - `pg_depend`, `pg_description`, `pg_attrdef` and `pg_constraint` are
    empty tables with the right columns.
  - Rows follow the session's access rules.
- A `'name'::regclass` literal is resolved at plan time to the relation's
  oid. `CAST(x AS oid)` is a `BIGINT` cast.
- `x = ANY (ARRAY[…])` and `x <> ALL (ARRAY[…])` become `IN` / `NOT IN`.
- `'type'::regtype`, `x::regtype` and `to_regtype` work for psycopg 3's
  type lookup. `SAVEPOINT` and `RELEASE` are accepted.
- `LIKE … ESCAPE 'c'` honors its escape character. The parser used to drop
  it without telling anyone, and SQLAlchemy relies on it.
- PostgreSQL catalog functions are added. A `pg_catalog.` prefix on a
  function name is accepted.
  - `format_type` and `pg_get_userbyid` return names.
  - `pg_table_is_visible` returns true; `pg_is_other_temp_schema` returns
    false.
  - `obj_description` and `col_description` return NULL.
- When the engine cannot run such a statement, the old fixed result is
  returned as before. For example, a query that needs arrays, which is how
  SQLAlchemy reflects columns and constraints.
- CI: dbt-postgres `test`, `show` and `docs generate`, and SQLAlchemy
  postgresql schema and table listing, join the client smoke.

Not in this change (stage 2b): array types, `unnest`, `array_agg`,
`json_build_object`, correlated subqueries in `CASE`, and with them
SQLAlchemy postgresql reflection (`Table(..., autoload_with=…)`) and
Metabase.

## Impact

- `crates/protocol`: the new `pg_catalog` module, routing in the metadata
  intercept, and the old pg handlers kept as the fallback.
- `crates/sql-parser`: `regclass` / `oid` casts, `ANY` / `ALL` over array
  literals, and the `pg_catalog.` prefix on functions.
- `crates/planner`: `regclass` literals and the function return types.
- `crates/execution`: the catalog functions.
- `crates/common`: the shared oid scheme.
- `scripts/client-smoke`, `docs/guide/clients.md`.
