## ADDED Requirements

### Requirement: CsvDataSource
The system SHALL define a `CsvDataSource` struct that reads a CSV file from a local filesystem path and produces `Vec<RecordBatch>`. It SHALL implement the `DataSource` trait. The schema SHALL be provided explicitly at construction time (no header inference for MVP).

#### Scenario: Reading a CSV file
- **WHEN** `CsvDataSource::new(path, schema)` is called with a valid CSV file path and matching schema
- **THEN** `scan()` returns `Ok(Vec<RecordBatch>)` containing the parsed CSV data

#### Scenario: CSV file not found
- **WHEN** `CsvDataSource::new(path, schema)` is called with a nonexistent file path
- **THEN** `scan()` returns `Err(ExecutionError::InvalidOperation(...))` indicating the file was not found

#### Scenario: CSV schema mismatch
- **WHEN** a CSV file is read with a schema that does not match the file's columns
- **THEN** `scan()` returns an error from the Arrow CSV reader

### Requirement: ParquetDataSource
The system SHALL define a `ParquetDataSource` struct that reads a Parquet file from a local filesystem path and produces `Vec<RecordBatch>`. It SHALL implement the `DataSource` trait. The schema SHALL be derived from the Parquet file metadata.

#### Scenario: Reading a Parquet file
- **WHEN** `ParquetDataSource::new(path)` is called with a valid Parquet file path
- **THEN** `schema()` returns column info derived from the Parquet metadata, and `scan()` returns the file's data as RecordBatches

#### Scenario: Parquet file not found
- **WHEN** `ParquetDataSource::new(path)` is called with a nonexistent file path
- **THEN** construction returns an error indicating the file was not found

### Requirement: FileConnectorFactory
The system SHALL implement `ConnectorFactory` for file-based tables. It SHALL support registering file paths with their format (CSV or Parquet) and optional schema. Given a `TableReference`, it SHALL create the appropriate `CsvDataSource` or `ParquetDataSource`.

#### Scenario: Creating a CSV data source
- **WHEN** a CSV file is registered as table "sales" and `factory.create_data_source(&table_ref, &schema)` is called
- **THEN** it returns an `Arc<dyn DataSource>` wrapping a `CsvDataSource` for that file

#### Scenario: Creating a Parquet data source
- **WHEN** a Parquet file is registered as table "events" and `factory.create_data_source(&table_ref, &schema)` is called
- **THEN** it returns an `Arc<dyn DataSource>` wrapping a `ParquetDataSource` for that file

#### Scenario: Unregistered file table
- **WHEN** `factory.create_data_source(&table_ref, &schema)` is called for a table not registered in the file connector
- **THEN** it returns `Err(ConnectorError::TableNotFound(...))`

### Requirement: FileFormat enum
The system SHALL define a `FileFormat` enum with variants `Csv` and `Parquet` to distinguish file types when registering tables.

#### Scenario: Registering a CSV table
- **WHEN** `factory.register_table("sales", path, FileFormat::Csv, Some(schema))` is called
- **THEN** subsequent `create_data_source` calls for "sales" produce a `CsvDataSource`

#### Scenario: Registering a Parquet table
- **WHEN** `factory.register_table("events", path, FileFormat::Parquet, None)` is called
- **THEN** subsequent `create_data_source` calls for "events" produce a `ParquetDataSource`

### Requirement: FileCatalog integration
The system SHALL provide `FileSchema` and `FileCatalog` structs implementing `SchemaProvider` and `CatalogProvider` respectively. `FileSchema` SHALL expose registered file tables as `TableProvider` instances with schema metadata derived from file registration.

#### Scenario: Listing file tables
- **WHEN** two files are registered as tables "sales" and "events" in a `FileSchema`
- **THEN** `schema.table_names()` returns both names

#### Scenario: Getting file table metadata
- **WHEN** `schema.table("sales")` is called for a registered CSV table
- **THEN** it returns `Some(Arc<dyn TableProvider>)` whose `schema()` matches the provided column info
