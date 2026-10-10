## ADDED Requirements

### Requirement: Read ORC Hive tables
The `HiveDataSource` SHALL read a Hive table whose storage descriptor declares
`org.apache.hadoop.hive.ql.io.orc.OrcInputFormat` or
`org.apache.hadoop.hive.ql.io.orc.OrcSerde` as ORC, using the same file listing,
scan partitioning and projection as Parquet tables.

#### Scenario: Trino-written ORC table
- **WHEN** a query scans a Hive table created by Trino with its default ORC storage format
- **THEN** the system SHALL return the same rows and values as Trino for the same query

#### Scenario: Column mapping by name
- **WHEN** an ORC file's column names differ from the HMS column names only in case
- **THEN** the system SHALL match them case-insensitively

#### Scenario: Positional Hive column names
- **WHEN** every column of an ORC file is named `_colN` and no table column matches by name
- **THEN** the system SHALL map file columns to table columns by position

#### Scenario: Column missing from a file
- **WHEN** a table column is absent from an ORC file
- **THEN** the system SHALL return NULL for that column for the file's rows

#### Scenario: Allowed type widening
- **WHEN** an ORC file column is narrower than the table column in a way Hive allows (e.g. `int` file column, `bigint` table column)
- **THEN** the system SHALL cast the values to the table type, failing the query on overflow rather than returning NULL

#### Scenario: Incompatible column type
- **WHEN** an ORC file column cannot be read as the table column's type (e.g. `string` as `bigint`)
- **THEN** the scan SHALL fail with an error naming the column, the ORC type and the table type

#### Scenario: ORC timestamps
- **WHEN** an ORC `timestamp` column is read
- **THEN** the system SHALL return the wall-clock value in the writer time zone recorded in the file, matching Trino

### Requirement: Read partitioned Hive tables
The `HiveDataSource` SHALL expose a partitioned table's partition key columns after its data columns, SHALL read exactly the partitions registered in Hive Metastore, each at its own location, and SHALL fill the partition columns from each partition's HMS values.

#### Scenario: Partition values from HMS
- **WHEN** a partition is registered with values `('a/b', '2024-01-01')` for keys `k varchar, day date`
- **THEN** the rows of its files SHALL have `k = 'a/b'` and `day = DATE '2024-01-01'`

#### Scenario: Default partition
- **WHEN** a partition value is `__HIVE_DEFAULT_PARTITION__`
- **THEN** the partition column SHALL be NULL for that partition's rows

#### Scenario: Partition outside the table directory
- **WHEN** a partition is registered at a location outside the table directory
- **THEN** its files SHALL be read

#### Scenario: Unregistered partition directory
- **WHEN** a `key=value` directory under the table location is not a registered partition
- **THEN** its files SHALL NOT be read

#### Scenario: Stray file
- **WHEN** a data file sits under the table location but in no registered partition
- **THEN** it SHALL be ignored and the table SHALL remain readable

### Requirement: Reject unsupported Hive tables
The `HiveDataSource` SHALL fail with a clear error, and SHALL NOT attempt a best-effort read, for Hive ACID tables and for storage formats other than Parquet and ORC.

#### Scenario: Transactional table
- **WHEN** a table's HMS parameters contain `transactional=true`
- **THEN** creating the data source SHALL fail with an error stating that Hive ACID tables are not supported

#### Scenario: ACID directory layout
- **WHEN** the table location contains `base_N`, `delta_N_M` or `delete_delta_N_M` directories
- **THEN** creating the data source SHALL fail with the same ACID error

#### Scenario: Unsupported storage format
- **WHEN** a table's input format and SerDe are neither Parquet nor ORC (e.g. `TextInputFormat`)
- **THEN** creating the data source SHALL fail with an error naming the input format and SerDe
