## ADDED Requirements

### Requirement: PostgreSQL catalog configuration
The server SHALL mount a PostgreSQL database as a catalog when `[[catalogs]]`
has `type = "postgresql"`. The catalog SHALL require `connection_url` in the
form `postgresql://host:port/database`, and SHALL accept `user`, `password`,
`max_connections` (default 8), `split_min_rows` (default 500000), `max_splits`
(default 16) and `default_schema` (default `public`). An unreachable database
at startup SHALL be logged and the catalog skipped, the same way an
unreachable Hive Metastore is handled. The server SHALL keep serving.

#### Scenario: Catalog is mounted
- **WHEN** the config has `[[catalogs]] name = "pg" type = "postgresql" connection_url = "postgresql://127.0.0.1:5433/tpch"` and the database is reachable
- **THEN** `SELECT count(*) FROM pg.public.nation` returns the table's row count

#### Scenario: Unreachable database does not stop the server
- **WHEN** the configured PostgreSQL is not reachable at startup
- **THEN** the server logs a warning naming the catalog, skips it, and keeps serving the other catalogs

#### Scenario: Missing connection_url is a config error
- **WHEN** a `postgresql` catalog has no `connection_url`
- **THEN** startup logs an error naming the catalog and the missing field, and skips the catalog

### Requirement: Schema and table discovery
The catalog SHALL list schemas and tables from the remote `information_schema`,
excluding `pg_catalog`, `information_schema` and `pg_toast*` schemas. It SHALL
include ordinary tables, views, materialized views, foreign tables and
partitioned tables. Column order SHALL follow `ordinal_position`. A table's
estimated row count SHALL come from `pg_class.reltuples`, and SHALL be exposed
as table statistics when it is non-negative.

#### Scenario: Tables are listed
- **WHEN** a client lists the tables of `pg.public`
- **THEN** every table and view in the remote `public` schema is returned

#### Scenario: Unknown table
- **WHEN** a query references `pg.public.missing`
- **THEN** the query fails with the same table-not-found error as other catalogs

### Requirement: Type mapping
The connector SHALL map PostgreSQL column types to Arneb types as follows:
`boolean` → BOOLEAN, `smallint` → SMALLINT, `integer` → INTEGER, `bigint` →
BIGINT, `real` → REAL, `double precision` → DOUBLE, `numeric(p,s)` with
`p ≤ 38` → DECIMAL(p,s), unconstrained `numeric` → DECIMAL(38,
`unconstrained_numeric_scale`) (default scale 10), `char(n)` / `varchar(n)` /
`text` → VARCHAR, `date` → DATE, `timestamp` → TIMESTAMP(6),
`timestamptz` → TIMESTAMP(6) WITH TIME ZONE (UTC), `uuid` → VARCHAR in
canonical lowercase form, `bytea` → VARBINARY. A value that cannot be
represented exactly in the mapped type SHALL fail the query with an error
naming the column, never be silently rounded or truncated. Examples are an
unconstrained `numeric` with more fractional digits than the scale, `NaN` or
±Infinity in `numeric`, or `numeric(p,s)` with `p > 38`.

#### Scenario: TPC-H types round-trip
- **WHEN** a PostgreSQL TPC-H `lineitem` table (`integer`, `numeric(15,2)`, `char(1)`, `date`, `varchar`) is scanned
- **THEN** every value equals the value Trino's `postgresql` catalog returns for the same row

#### Scenario: Unsupported column type
- **WHEN** a table has a column of type `jsonb` and a query selects it
- **THEN** the query fails with an error naming the column and the type `jsonb`

#### Scenario: Unsupported column not selected
- **WHEN** the same table is queried selecting only supported columns
- **THEN** the query succeeds

#### Scenario: Inexact numeric value fails
- **WHEN** an unconstrained `numeric` column contains `1.000000000001` and the scale is 10
- **THEN** the query fails with an error naming the column rather than returning a rounded value

### Requirement: Binary COPY reads
The connector SHALL fetch rows with `COPY (<generated SELECT>) TO STDOUT
(FORMAT binary)` and decode them directly into Arrow arrays. Batches SHALL hold
at most the scan's batch size (default 8192 rows). Each batch's memory SHALL be
reserved from the scan context's memory pool, so a query memory limit covers
PostgreSQL scans too.

#### Scenario: Batches respect the batch size
- **WHEN** a 20,000-row table is scanned with the default batch size
- **THEN** the scan emits batches of at most 8192 rows

#### Scenario: Memory limit applies
- **WHEN** a query with a memory limit smaller than one batch scans a PostgreSQL table
- **THEN** the query fails with the resource-exhausted error used for other scans

### Requirement: Connection pooling
Each PostgreSQL catalog SHALL keep one connection pool per node, bounded by
`max_connections`. A scan partition SHALL hold a connection only while its
`COPY` is streaming. Dropping or cancelling the query SHALL return or close the
connection.

#### Scenario: Pool bound holds
- **WHEN** a table is scanned with 16 partitions and `max_connections = 4`
- **THEN** at most 4 connections from that node are open to the database at any time, and the query completes

### Requirement: Credentials handling
The catalog password SHALL be redacted from `Debug` output, logs and error
messages. The password MAY be supplied by the environment variable
`ARNEB_CATALOG_<NAME>_PASSWORD`, where `<NAME>` is the catalog name in
uppercase with every non-alphanumeric character replaced by `_`. The
environment variable SHALL take precedence over the config file.

#### Scenario: Password from env
- **WHEN** catalog `pg` has no `password` in the file and `ARNEB_CATALOG_PG_PASSWORD=secret` is set
- **THEN** the connector authenticates with `secret`

#### Scenario: Password not logged
- **WHEN** the catalog fails to connect
- **THEN** the logged error does not contain the password

### Requirement: Distributed execution
PostgreSQL catalogs SHALL be registered on workers as well as on the
coordinator, from the same `[[catalogs]]` configuration. Scan partitions
SHALL be distributable to workers like any other multi-partition source, and
distributed results SHALL equal standalone results.

#### Scenario: Distributed scan matches standalone
- **WHEN** the same query over a split PostgreSQL table runs in standalone mode and in coordinator + 2 workers mode
- **THEN** both return the same rows
