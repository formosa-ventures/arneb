# Spec: File Connector (CTAS Implementation)

## MODIFIED Requirements

### Requirement: Implement CTAS for file connector
The file connector SHALL implement `create_table_as_select` by writing query results as a Parquet file.

#### Scenario: CTAS writes Parquet file
- **WHEN** `CREATE TABLE file.default.output AS SELECT * FROM input` is executed
- **THEN** a Parquet file is written to the configured output directory with filename derived from the table name.

#### Scenario: CTAS output is queryable
- **WHEN** a CTAS operation completes and the resulting file is registered in the catalog
- **THEN** subsequent `SELECT * FROM output` queries read from the written Parquet file.

### Requirement: Unsupported DDL operations return clear errors
The file connector SHALL return clear error messages for DDL operations it does not support.

#### Scenario: CREATE TABLE without AS SELECT
- **WHEN** `CREATE TABLE file.default.test (id INT)` is executed
- **THEN** an error "CREATE TABLE not supported by file connector; use CREATE TABLE AS SELECT" SHALL be returned.

#### Scenario: INSERT INTO file table
- **WHEN** `INSERT INTO file_table VALUES (1, 'a')` is executed against a file connector table
- **THEN** an error "INSERT not supported by file connector" SHALL be returned.

#### Scenario: DROP TABLE on file table
- **WHEN** `DROP TABLE file_table` is executed against a file connector table
- **THEN** an error "DROP TABLE not supported by file connector" SHALL be returned.

#### Scenario: DELETE FROM file table
- **WHEN** `DELETE FROM file_table WHERE id = 1` is executed against a file connector table
- **THEN** an error "DELETE not supported by file connector" SHALL be returned.

### Requirement: Parquet write configuration
The file connector MUST use sensible Parquet write defaults (e.g., snappy compression, row group size).

#### Scenario: Written file is valid Parquet
- **WHEN** a CTAS operation writes a Parquet file
- **THEN** the file is a valid Parquet file readable by standard Parquet tools and the engine's Parquet reader.
