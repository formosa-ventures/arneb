# Spec: DML Support

## ADDED Requirements

### Requirement: Parse INSERT INTO VALUES statement
The SQL parser SHALL recognize `INSERT INTO name VALUES (v1, v2), (v3, v4)` syntax.

#### Scenario: Single row INSERT
- **WHEN** the engine receives `INSERT INTO test VALUES (1, 'alice')`
- **THEN** the parser produces an InsertInto AST node with one row of literal values.

#### Scenario: Multi-row INSERT
- **WHEN** the engine receives `INSERT INTO test VALUES (1, 'alice'), (2, 'bob')`
- **THEN** the parser produces an InsertInto AST node with two rows of literal values.

#### Scenario: INSERT with column list
- **WHEN** the engine receives `INSERT INTO test (id, name) VALUES (1, 'alice')`
- **THEN** the parser produces an InsertInto AST node with explicit column list and values.

### Requirement: Parse INSERT INTO SELECT statement
The SQL parser SHALL recognize `INSERT INTO name SELECT ...` syntax.

#### Scenario: INSERT INTO SELECT
- **WHEN** the engine receives `INSERT INTO archive SELECT * FROM orders WHERE year < 2020`
- **THEN** the parser produces an InsertInto AST node with the subquery as the source.

### Requirement: Parse DELETE FROM statement
The SQL parser SHALL recognize `DELETE FROM name` and `DELETE FROM name WHERE predicate` syntax.

#### Scenario: DELETE all rows
- **WHEN** the engine receives `DELETE FROM test`
- **THEN** the parser produces a DeleteFrom AST node with no predicate (truncate).

#### Scenario: DELETE with WHERE
- **WHEN** the engine receives `DELETE FROM test WHERE id > 100`
- **THEN** the parser produces a DeleteFrom AST node with the predicate `id > 100`.

### Requirement: Execute INSERT INTO via connector
The execution engine SHALL convert values to RecordBatches and delegate to the connector's DDLProvider::insert_into.

#### Scenario: INSERT VALUES into memory table
- **WHEN** `INSERT INTO test VALUES (1, 'alice')` is executed against a memory connector table
- **THEN** the row is inserted and "INSERT 0 1" confirmation is returned.

#### Scenario: INSERT SELECT into memory table
- **WHEN** `INSERT INTO archive SELECT * FROM orders WHERE year < 2020` is executed
- **THEN** the SELECT is executed, results are inserted into "archive", and "INSERT 0 N" confirmation is returned with the row count.

#### Scenario: Type mismatch
- **WHEN** INSERT provides a string value for an integer column
- **THEN** a type error SHALL be raised.

#### Scenario: Column count mismatch
- **WHEN** INSERT provides fewer values than the table has columns (without explicit column list)
- **THEN** an error SHALL be raised.

### Requirement: Execute DELETE FROM via connector
The execution engine SHALL delegate DELETE to the connector's DDLProvider::delete_from.

#### Scenario: DELETE all rows
- **WHEN** `DELETE FROM test` is executed
- **THEN** all rows are removed and "DELETE N" confirmation is returned with the deleted count.

#### Scenario: DELETE with predicate
- **WHEN** `DELETE FROM test WHERE id > 100` is executed
- **THEN** only matching rows are removed and "DELETE N" confirmation is returned.

#### Scenario: Connector does not support DELETE
- **WHEN** DELETE is executed against a connector that does not implement DDLProvider
- **THEN** an error "DML not supported by this connector" SHALL be returned.
