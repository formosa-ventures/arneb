# Proposal: DDL/DML Connector-Delegated

## Why

The engine is currently read-only. Users cannot create tables, insert data, drop tables, or define views. This prevents basic data management workflows, testing scenarios that require data setup, and any use case that needs write capabilities. Without DDL/DML support, users must prepare all data externally before querying.

## What

Add support for Data Definition Language (DDL) and Data Manipulation Language (DML) statements, delegated to connectors:

- **CREATE TABLE**: Create a new table in a connector with a specified schema.
- **CREATE TABLE AS SELECT (CTAS)**: Create a new table populated with query results.
- **DROP TABLE**: Remove a table from a connector.
- **INSERT INTO VALUES**: Insert literal rows into a table.
- **INSERT INTO SELECT**: Insert query results into an existing table.
- **DELETE FROM**: Delete rows from a table (with optional WHERE clause).
- **CREATE VIEW**: Define a named view stored as a subquery in the catalog.
- **DROP VIEW**: Remove a view from the catalog.

All write operations are delegated to connectors via a new `DDLProvider` trait. Connectors that do not support writes return a "not supported" error.

## New Capabilities

- `ddl-support` — Parse, plan, and execute CREATE TABLE, DROP TABLE, CREATE TABLE AS SELECT.
- `dml-support` — Parse, plan, and execute INSERT INTO VALUES, INSERT INTO SELECT, DELETE FROM.
- `view-support` — Parse, plan, and execute CREATE VIEW, DROP VIEW.
- `ddl-provider-trait` — New trait defining the DDL/DML interface for connectors.

## Modified Capabilities

- `connector-traits` — Extended with optional `DDLProvider` support.
- `memory-connector` — Implements DDLProvider for CREATE TABLE, INSERT INTO, DROP TABLE.
- `file-connector` — Implements CREATE TABLE AS SELECT to write Parquet files.

## Success Criteria

- `CREATE TABLE memory.default.test (id INT, name VARCHAR)` creates a table in the memory connector.
- `INSERT INTO test VALUES (1, 'alice'), (2, 'bob')` inserts rows into the memory table.
- `CREATE TABLE output AS SELECT * FROM input WHERE x > 10` writes a Parquet file via the file connector.
- `DROP TABLE test` removes the table from the connector.
- `CREATE VIEW v AS SELECT * FROM t WHERE active = true` registers a view that can be queried.
- Connectors that do not implement DDLProvider return a clear "DDL not supported" error.
