# Spec: Memory Connector (DDL Implementation)

## MODIFIED Requirements

### Requirement: Implement DDLProvider for memory connector
The memory connector SHALL implement the full DDLProvider trait, supporting CREATE TABLE, DROP TABLE, INSERT INTO, DELETE FROM, and CTAS.

#### Scenario: CREATE TABLE in memory
- **WHEN** `CREATE TABLE memory.default.test (id INT, name VARCHAR)` is executed
- **THEN** a new empty table "test" is created in the memory connector with the specified schema.

#### Scenario: INSERT INTO memory table
- **WHEN** `INSERT INTO test VALUES (1, 'alice'), (2, 'bob')` is executed against a memory table
- **THEN** two rows are added to the table's in-memory data store and "INSERT 0 2" is returned.

#### Scenario: SELECT after INSERT
- **WHEN** rows are inserted into a memory table and then queried with SELECT
- **THEN** the inserted rows are visible in the query results.

#### Scenario: DROP TABLE from memory
- **WHEN** `DROP TABLE test` is executed for an existing memory table
- **THEN** the table and all its data are removed from the memory connector.

#### Scenario: DELETE FROM memory table
- **WHEN** `DELETE FROM test WHERE id > 1` is executed against a memory table containing rows with id 1, 2, 3
- **THEN** rows with id 2 and 3 are removed, and "DELETE 2" is returned.

#### Scenario: DELETE all from memory table
- **WHEN** `DELETE FROM test` is executed without a WHERE clause
- **THEN** all rows are removed (truncate) and the total deleted count is returned.

#### Scenario: CTAS in memory
- **WHEN** `CREATE TABLE output AS SELECT * FROM input WHERE x > 10` is executed
- **THEN** a new memory table "output" is created with the filtered data.

### Requirement: Thread-safe memory mutations
The memory connector's DDLProvider MUST use appropriate synchronization (e.g., RwLock) to support concurrent reads and writes.

#### Scenario: Concurrent read and write
- **WHEN** one thread inserts rows while another reads from the same table
- **THEN** the operations do not cause data corruption or panics.
