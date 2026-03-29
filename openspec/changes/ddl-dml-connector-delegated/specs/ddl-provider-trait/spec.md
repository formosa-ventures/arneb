# Spec: DDLProvider Trait

## ADDED Requirements

### Requirement: Define DDLProvider trait
The engine SHALL define a `DDLProvider` trait that connectors can optionally implement to support DDL and DML operations.

#### Scenario: Trait method signatures
- **WHEN** a connector implements DDLProvider
- **THEN** it provides implementations for `create_table`, `drop_table`, `insert_into`, `delete_from`, and `create_table_as_select`.

### Requirement: DDLProvider is optional
Connectors MUST NOT be required to implement DDLProvider. It is an optional extension.

#### Scenario: Read-only connector
- **WHEN** a connector does not implement DDLProvider
- **THEN** the connector continues to function for read operations (SELECT queries).

#### Scenario: DDL on read-only connector
- **WHEN** a DDL statement targets a connector without DDLProvider
- **THEN** the engine returns a clear error message: "DDL not supported by connector '<name>'".

### Requirement: create_table method
The `create_table` method SHALL create a new empty table with the specified name and schema.

#### Scenario: Successful creation
- **WHEN** `create_table("users", schema)` is called with a valid schema
- **THEN** the table is created and `Ok(())` is returned.

#### Scenario: Duplicate table
- **WHEN** `create_table("users", schema)` is called and "users" already exists
- **THEN** an error is returned indicating the table already exists.

### Requirement: drop_table method
The `drop_table` method SHALL remove an existing table and its data.

#### Scenario: Successful drop
- **WHEN** `drop_table("users")` is called for an existing table
- **THEN** the table and its data are removed and `Ok(())` is returned.

#### Scenario: Non-existent table
- **WHEN** `drop_table("nonexistent")` is called
- **THEN** an error is returned indicating the table was not found.

### Requirement: insert_into method
The `insert_into` method SHALL append record batches to an existing table and return the number of inserted rows.

#### Scenario: Successful insert
- **WHEN** `insert_into("users", batches)` is called with schema-compatible batches
- **THEN** the rows are appended and the row count is returned.

#### Scenario: Schema mismatch
- **WHEN** `insert_into("users", batches)` is called with batches whose schema does not match the table
- **THEN** an error is returned indicating a schema mismatch.

### Requirement: delete_from method
The `delete_from` method SHALL remove rows matching the predicate and return the number of deleted rows. If no predicate is provided, all rows are deleted.

#### Scenario: Delete with predicate
- **WHEN** `delete_from("users", Some(predicate))` is called
- **THEN** matching rows are removed and the count of deleted rows is returned.

#### Scenario: Delete all (truncate)
- **WHEN** `delete_from("users", None)` is called
- **THEN** all rows are removed and the total row count is returned.

### Requirement: create_table_as_select method
The `create_table_as_select` method SHALL create a new table and populate it with the provided record batches.

#### Scenario: Successful CTAS
- **WHEN** `create_table_as_select("output", batches)` is called
- **THEN** a new table "output" is created with the schema and data from the batches.
