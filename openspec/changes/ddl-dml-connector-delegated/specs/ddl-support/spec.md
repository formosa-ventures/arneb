# Spec: DDL Support

## ADDED Requirements

### Requirement: Parse CREATE TABLE statement
The SQL parser SHALL recognize `CREATE TABLE name (col1 type1, col2 type2, ...)` syntax.

#### Scenario: Simple CREATE TABLE
- **WHEN** the engine receives `CREATE TABLE test (id INT, name VARCHAR)`
- **THEN** the parser produces a CreateTable AST node with table name "test" and two column definitions.

#### Scenario: CREATE TABLE with qualified name
- **WHEN** the engine receives `CREATE TABLE memory.default.test (id INT)`
- **THEN** the parser produces a CreateTable AST node with the fully qualified table reference.

### Requirement: Parse DROP TABLE statement
The SQL parser SHALL recognize `DROP TABLE name` and `DROP TABLE IF EXISTS name` syntax.

#### Scenario: DROP TABLE
- **WHEN** the engine receives `DROP TABLE test`
- **THEN** the parser produces a DropTable AST node with table name "test".

#### Scenario: DROP TABLE IF EXISTS
- **WHEN** the engine receives `DROP TABLE IF EXISTS test`
- **THEN** the parser produces a DropTable AST node with `if_exists` flag set to true.

### Requirement: Parse CREATE TABLE AS SELECT statement
The SQL parser SHALL recognize `CREATE TABLE name AS SELECT ...` syntax.

#### Scenario: CTAS
- **WHEN** the engine receives `CREATE TABLE output AS SELECT * FROM input WHERE x > 10`
- **THEN** the parser produces a CreateTableAsSelect AST node with the table name and the subquery.

### Requirement: Execute CREATE TABLE via connector
The execution engine SHALL delegate CREATE TABLE to the connector's DDLProvider.

#### Scenario: Connector supports CREATE TABLE
- **WHEN** CREATE TABLE is executed against a connector that implements DDLProvider
- **THEN** the table is created and a "CREATE TABLE" confirmation is returned.

#### Scenario: Connector does not support CREATE TABLE
- **WHEN** CREATE TABLE is executed against a connector that does not implement DDLProvider
- **THEN** an error "DDL not supported by this connector" SHALL be returned.

#### Scenario: Table already exists
- **WHEN** CREATE TABLE is executed for a table name that already exists
- **THEN** an error "Table already exists" SHALL be returned.

### Requirement: Execute DROP TABLE via connector
The execution engine SHALL delegate DROP TABLE to the connector's DDLProvider.

#### Scenario: Drop existing table
- **WHEN** DROP TABLE is executed for an existing table
- **THEN** the table is removed and a "DROP TABLE" confirmation is returned.

#### Scenario: Drop non-existent table
- **WHEN** DROP TABLE is executed for a table that does not exist
- **THEN** an error "Table not found" SHALL be returned.

#### Scenario: Drop non-existent table with IF EXISTS
- **WHEN** DROP TABLE IF EXISTS is executed for a table that does not exist
- **THEN** no error is raised and a "DROP TABLE" confirmation is returned.

### Requirement: Execute CTAS via connector
The execution engine SHALL execute the SELECT subquery, then delegate the resulting batches to the connector's DDLProvider::create_table_as_select.

#### Scenario: CTAS with query results
- **WHEN** `CREATE TABLE output AS SELECT id, name FROM input WHERE active = true` is executed
- **THEN** the SELECT is executed, results are passed to the connector, and a new table "output" is created with the query results.
