## ADDED Requirements

### Requirement: pg_catalog.pg_type returns type metadata
The system SHALL return a result set for queries against `pg_catalog.pg_type` containing PostgreSQL type information. Each row SHALL include at minimum: `oid` (Int64), `typname` (Utf8), `typnamespace` (Int64), `typlen` (Int64), `typtype` (Utf8). The rows SHALL include entries for built-in types: bool, int2, int4, int8, float4, float8, numeric, varchar, text, bytea, date, timestamp.

#### Scenario: Query pg_type
- **WHEN** client sends `SELECT oid, typname FROM pg_catalog.pg_type`
- **THEN** server returns rows for all supported PostgreSQL types with correct OIDs (e.g., bool=16, int4=23, int8=20, text=25, float8=701)

#### Scenario: Query pg_type with WHERE filter
- **WHEN** client sends `SELECT typname FROM pg_catalog.pg_type WHERE oid = 23`
- **THEN** server returns one row with typname = 'int4'

### Requirement: pg_catalog.pg_namespace returns schema metadata
The system SHALL return a result set for queries against `pg_catalog.pg_namespace` containing one row per schema in the CatalogManager. Each row SHALL include: `oid` (Int64), `nspname` (Utf8).

#### Scenario: Query pg_namespace
- **WHEN** client sends `SELECT nspname FROM pg_catalog.pg_namespace` and the CatalogManager has schemas "default" and "public"
- **THEN** server returns rows including "default" and "public"

### Requirement: pg_catalog.pg_class returns table metadata
The system SHALL return a result set for queries against `pg_catalog.pg_class` containing one row per table across all schemas. Each row SHALL include: `oid` (Int64), `relname` (Utf8), `relnamespace` (Int64, referencing pg_namespace.oid), `relkind` (Utf8, 'r' for tables).

#### Scenario: Query pg_class
- **WHEN** client sends `SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r'` and the CatalogManager has tables "lineitem", "orders"
- **THEN** server returns rows including "lineitem" and "orders"

### Requirement: pg_catalog.pg_attribute returns column metadata
The system SHALL return a result set for queries against `pg_catalog.pg_attribute` containing one row per column across all tables. Each row SHALL include: `attrelid` (Int64, referencing pg_class.oid), `attname` (Utf8), `atttypid` (Int64, referencing pg_type.oid), `attnum` (Int64, 1-based ordinal), `attnotnull` (Boolean).

#### Scenario: Query pg_attribute for a table
- **WHEN** client sends `SELECT attname, atttypid FROM pg_catalog.pg_attribute WHERE attrelid = <lineitem_oid>`
- **THEN** server returns rows for all columns of the lineitem table with correct type OIDs

### Requirement: version() function returns server identity
The system SHALL handle `SELECT version()` by returning a single row with a string identifying the server (e.g., "arneb 0.1.0").

#### Scenario: SELECT version()
- **WHEN** client sends `SELECT version()`
- **THEN** server returns one row with a string containing "arneb"
