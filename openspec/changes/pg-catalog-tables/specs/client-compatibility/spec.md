## ADDED Requirements

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
