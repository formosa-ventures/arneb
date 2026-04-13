## ADDED Requirements

### Requirement: Read Parquet data from HMS table location
The `HiveDataSource` SHALL read Parquet files from the storage location specified in HMS table metadata (`sd.location`).

#### Scenario: Read non-partitioned Hive table
- **WHEN** a query scans a non-partitioned Hive table with `sd.location = "s3://warehouse/db/events/"`
- **THEN** the system SHALL list all `.parquet` files at that location via ObjectStore and read them as a combined RecordBatchStream

#### Scenario: Read partitioned Hive table (no pruning)
- **WHEN** a query scans a partitioned Hive table
- **THEN** the system SHALL list all partition directories and read all `.parquet` files across all partitions (no filter-based partition pruning in v1)

### Requirement: Schema consistency validation
The system SHALL validate that the Parquet file schema matches the HMS table schema.

#### Scenario: Schema mismatch
- **WHEN** a Parquet file's schema differs from the HMS table schema (e.g., missing column, type mismatch)
- **THEN** the system SHALL return an error identifying the file and the schema difference

### Requirement: Projection pushdown on Hive tables
The `HiveDataSource` SHALL support projection pushdown when scanning Hive Parquet tables.

#### Scenario: Column subset query
- **WHEN** a query selects specific columns from a Hive table
- **THEN** the system SHALL only read the requested columns from the Parquet files using column projection
