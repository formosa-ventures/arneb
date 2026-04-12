## MODIFIED Requirements

### Requirement: Parquet data source reads from ObjectStore
The `ParquetDataSource` SHALL read Parquet files through the `ObjectStore` trait using async `ParquetObjectReader`, replacing direct `std::fs::File` access. Projection pushdown SHALL continue to work via Parquet metadata.

#### Scenario: Read local Parquet file
- **WHEN** a table references a local path (e.g., `/data/file.parquet`)
- **THEN** the system SHALL read the file via `LocalFileSystem` ObjectStore and return a RecordBatchStream

#### Scenario: Read S3 Parquet file
- **WHEN** a table references an S3 path (e.g., `s3://bucket/file.parquet`)
- **THEN** the system SHALL read the file via `AmazonS3` ObjectStore using async range reads and return a RecordBatchStream

#### Scenario: Projection pushdown on remote Parquet
- **WHEN** a query selects a subset of columns from a remote Parquet file
- **THEN** the system SHALL only fetch the row groups and columns needed, using Parquet metadata for column pruning

### Requirement: CSV data source reads from ObjectStore
The `CsvDataSource` SHALL read CSV files through the `ObjectStore` trait, buffering remote content before parsing with `arrow-csv`.

#### Scenario: Read local CSV file
- **WHEN** a table references a local CSV path
- **THEN** the system SHALL read the file via `LocalFileSystem` ObjectStore and return a RecordBatchStream

#### Scenario: Read remote CSV file
- **WHEN** a table references a remote CSV path (e.g., `s3://bucket/file.csv`)
- **THEN** the system SHALL download the file content via `ObjectStore::get()` and parse it with the arrow-csv reader

### Requirement: Schema inference from remote Parquet
The system SHALL infer Parquet schemas from remote files by reading only the file footer metadata.

#### Scenario: Auto-detect schema from S3 Parquet
- **WHEN** a Parquet table is registered without an explicit schema and the path is remote
- **THEN** the system SHALL read the Parquet footer from the remote file to extract the Arrow schema
