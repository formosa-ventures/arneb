# client-compatibility Specification

## Purpose
TBD - created by archiving change client-smoke. Update Purpose after archive.
## Requirements
### Requirement: Transaction statements are accepted over pgwire
`BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK` and `ABORT` SHALL
complete with their PostgreSQL command tag and no other effect.

#### Scenario: psycopg2 default mode
- **WHEN** psycopg2 connects without autocommit and runs a query
- **THEN** the query returns its rows

### Requirement: Driver session values are PostgreSQL's
`version()` SHALL start with `PostgreSQL 14.0` and name Arneb. The startup
`server_version` and `SHOW server_version` SHALL both be `14.0`, and
`SHOW server_version_num` SHALL be `140000`. `SHOW standard_conforming_strings`
SHALL be `on` and `SHOW transaction isolation level` `read committed`.
`current_user` SHALL be the session user.

#### Scenario: SQLAlchemy reads the version
- **WHEN** a client runs `SELECT version()`
- **THEN** the value matches `PostgreSQL \d+\.\d+`

### Requirement: Bind parameters are decoded by their declared type
A Bind parameter SHALL be decoded according to the type the client declared
and the format it chose: binary `int2`, `int4`, `int8`, `float4`, `float8`,
`bool`, `date`, `timestamp`, `timestamptz` and text types, and every type in
text format. A binary parameter of any other type SHALL fail the statement
with SQLSTATE `0A000`.

#### Scenario: psycopg 3
- **WHEN** psycopg 3 binds an integer, a boolean and a date
- **THEN** the statement sees the integer, the boolean and the date

### Requirement: Names resolve like PostgreSQL and Trino clients expect
A catalog, schema or table name that matches nothing as written SHALL match
its lowercase form. A column reference SHALL accept `schema.table.column` and
`catalog.schema.table.column`. `INNER` and `LEFT JOIN … USING (columns)`
SHALL join on equality of the columns and show each once, and `RIGHT` /
`FULL JOIN … USING` SHALL be refused. On the Trino protocol,
`current_catalog` and `current_schema` SHALL return the session's catalog
and schema, and `"system"."metadata"."table_comments"` SHALL be readable.

#### Scenario: Upper-case information schema
- **WHEN** dbt-trino runs its catalog query on `"file".INFORMATION_SCHEMA.columns` joined `USING (...)`
- **THEN** `dbt docs generate` writes a catalog with the sources' columns

### Requirement: Clients are checked on every pull request
CI SHALL start a coordinator and two workers on TPC-H SF1 and fail unless
the 22 TPC-H queries match DuckDB, 5 rounds of 20 concurrent queries all
succeed, the Python client checks pass (psycopg2, psycopg 3, Trino DB-API,
SQLAlchemy Trino dialect), and dbt-trino `debug`, `test`, `show` and
`docs generate` and dbt-postgres `debug` succeed.

#### Scenario: CI job
- **WHEN** a pull request runs CI
- **THEN** the "Cluster smoke" job runs `scripts/client-smoke/run.sh` and fails on any failed step

### Requirement: pg_catalog tables are queryable over pgwire
A pgwire statement that reads `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_database`, `pg_roles`, `pg_am`, `pg_tablespace`, `pg_depend`, `pg_description`, `pg_attrdef` or `pg_constraint` SHALL run in the engine over tables describing the session's catalog. Filters, joins, ordering and unqualified names SHALL behave as in PostgreSQL, and objects hidden by the access rules SHALL be absent.

#### Scenario: Tables of one schema
- **WHEN** a client runs `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'default' AND c.relkind = 'r'`
- **THEN** the result holds exactly the tables of schema `default` in the session's catalog

#### Scenario: dbt-postgres
- **WHEN** dbt-postgres runs `test`, `show` and `docs generate`
- **THEN** each command succeeds, and the catalog it writes lists the sources' columns in order

### Requirement: regclass literals and oid casts
`'<name>'::regclass` and `CAST('<name>' AS REGCLASS)` SHALL evaluate to the oid of the named relation as `pg_class.oid` reports it, a PostgreSQL system relation's name SHALL resolve to its PostgreSQL oid, and `CAST(x AS oid)` SHALL cast to `BIGINT`. A non-literal `regclass` argument SHALL fail as unsupported.

#### Scenario: System relation
- **WHEN** a client runs `SELECT 'pg_rewrite'::regclass`
- **THEN** the result is 2618

### Requirement: regtype and savepoints for psycopg 3
`'<type>'::regtype` SHALL be the type's oid, `x::regtype` of an oid column SHALL be the type's name as `format_type` prints it, and `to_regtype(name)` SHALL return the oid of a built-in type or NULL. `SAVEPOINT` and `RELEASE` SHALL complete with their command tags and no other effect.

#### Scenario: psycopg 3 type lookup
- **WHEN** SQLAlchemy's psycopg dialect looks up the `hstore` type on connect
- **THEN** the lookup returns no row and the connection proceeds

### Requirement: LIKE ESCAPE
`x LIKE pattern ESCAPE 'c'` SHALL treat `c` as the escape character, and an empty escape string SHALL mean none.

#### Scenario: SQLAlchemy schema listing
- **WHEN** a client filters `nspname NOT LIKE 'pg/_' || '%' ESCAPE '/'`
- **THEN** `pg_catalog` is excluded and `information_schema` is kept

### Requirement: ANY and ALL over array literals
`x = ANY (ARRAY[…])` and `x = SOME (ARRAY[…])` SHALL behave as `x IN (…)`, and `x <> ALL (ARRAY[…])` as `x NOT IN (…)`.

#### Scenario: SQLAlchemy table listing
- **WHEN** SQLAlchemy's postgresql dialect calls `get_table_names(schema='default')`
- **THEN** it returns the schema's tables

### Requirement: PostgreSQL catalog functions
`format_type(type_oid, typmod)`, `pg_get_userbyid(oid)`, `pg_table_is_visible(oid)`, `pg_is_other_temp_schema(oid)`, `obj_description(oid[, catalog])` and `col_description(oid, column)` SHALL be callable, with or without a `pg_catalog.` qualifier.

#### Scenario: Column type names
- **WHEN** a client runs `SELECT format_type(atttypid, atttypmod) FROM pg_attribute` for a `DECIMAL(12,2)` column
- **THEN** the value is `numeric(12,2)`

### Requirement: Fixed results remain the fallback
When the engine cannot run a pg_catalog statement, the fixed result the server returned before SHALL be returned if one matches.

#### Scenario: Array-based reflection
- **WHEN** a client sends a pg_catalog query that uses `unnest`
- **THEN** the server answers it as before this change

#### Scenario: Fallback is visible
- **WHEN** a pg_catalog statement is answered with the fixed result
- **THEN** the server logs a warning with the statement on `arneb::protocol` and increments `arneb_pg_catalog_fallbacks_total`

