## ADDED Requirements

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
