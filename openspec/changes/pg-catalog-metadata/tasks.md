## 1. Metadata Query Detection

- [x] 1.1 Create `crates/protocol/src/metadata.rs` module with `MetadataHandler` struct holding `Arc<CatalogManager>`
- [x] 1.2 Implement `try_handle_metadata(sql: &str, catalog: &CatalogManager) -> Option<Result<Vec<RecordBatch>>>` that detects and routes metadata queries
- [x] 1.3 Wire `try_handle_metadata` into `execute_query()` in handler.rs — call before the SQL parser, return early if matched

## 2. version() Function

- [x] 2.1 Detect `SELECT version()` and return a single-row result with "trino-alt {version}"
- [x] 2.2 Also handle common variants: `select version()`, `SELECT VERSION()`

## 3. pg_catalog.pg_type

- [x] 3.1 Build static type table with rows for: bool(16), int2(21), int4(23), int8(20), float4(700), float8(701), numeric(1700), varchar(1043), text(25), bytea(17), date(1082), timestamp(1114)
- [x] 3.2 Return columns: oid, typname, typnamespace, typlen, typtype, typbasetype, typnotnull

## 4. pg_catalog.pg_namespace

- [x] 4.1 Enumerate all schemas from CatalogManager (catalog → schema_names)
- [x] 4.2 Return columns: oid (hash of name), nspname
- [x] 4.3 Include built-in namespaces: pg_catalog, information_schema

## 5. pg_catalog.pg_class

- [x] 5.1 Enumerate all tables from CatalogManager (catalog → schema → table_names)
- [x] 5.2 Return columns: oid (hash of catalog.schema.table), relname, relnamespace (matching pg_namespace.oid), relkind ('r')
- [x] 5.3 Include relnatts (number of columns) and relowner (0)

## 6. pg_catalog.pg_attribute

- [x] 6.1 Enumerate all columns from CatalogManager (catalog → schema → table → columns)
- [x] 6.2 Return columns: attrelid (matching pg_class.oid), attname, atttypid (matching pg_type.oid), attnum (1-based), attnotnull, attlen, atttypmod
- [x] 6.3 Map trino DataType to PostgreSQL type OID using existing `datatype_to_pg_type()` from encoding.rs

## 7. information_schema.tables

- [x] 7.1 Enumerate all tables and return: table_catalog, table_schema, table_name, table_type ('BASE TABLE')

## 8. information_schema.columns

- [x] 8.1 Enumerate all columns and return: table_catalog, table_schema, table_name, column_name, ordinal_position, data_type (PG type name), is_nullable ('YES'/'NO')

## 9. information_schema.schemata

- [x] 9.1 Enumerate all schemas and return: catalog_name, schema_name

## 10. Integration Tests

- [x] 10.1 Test `SELECT version()` returns string containing "trino-alt"
- [x] 10.2 Test `SELECT * FROM pg_catalog.pg_type` returns rows with known type OIDs
- [x] 10.3 Test `SELECT table_name FROM information_schema.tables` returns registered table names
- [x] 10.4 Test `SELECT column_name FROM information_schema.columns WHERE table_name = 'nation'` returns nation's columns
- [x] 10.5 Test regular queries still work after metadata handler is wired in (no regression)

## 11. Quality

- [x] 11.1 `cargo build` compiles without warnings
- [x] 11.2 `cargo test` — all tests pass
- [x] 11.3 `cargo clippy -- -D warnings` — clean
- [x] 11.4 `cargo fmt -- --check` — clean
