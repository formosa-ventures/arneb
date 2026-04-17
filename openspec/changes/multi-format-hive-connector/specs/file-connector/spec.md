## MODIFIED Requirements

### Requirement: FileFormat enum
The system SHALL define a `FileFormat` enum with variants `Csv`, `Parquet`, `Json`, `Orc`, and `Avro` to distinguish file types when registering tables or when the Hive connector dispatches readers. Enum variants for `Orc` and `Avro` SHALL be gated behind the `orc` and `avro` cargo features respectively so that downstream users can opt out of those dependencies.

#### Scenario: Registering a CSV table
- **WHEN** `factory.register_table("sales", path, FileFormat::Csv, Some(schema))` is called
- **THEN** subsequent `create_data_source` calls for "sales" produce a `CsvDataSource`

#### Scenario: Registering a Parquet table
- **WHEN** `factory.register_table("events", path, FileFormat::Parquet, None)` is called
- **THEN** subsequent `create_data_source` calls for "events" produce a `ParquetDataSource`

#### Scenario: Registering a JSON table
- **WHEN** `factory.register_table("logs", path, FileFormat::Json, Some(schema))` is called
- **THEN** subsequent `create_data_source` calls for "logs" produce a `JsonDataSource`

#### Scenario: Registering an ORC table (feature gated)
- **WHEN** the `orc` cargo feature is enabled and `factory.register_table("ledger", path, FileFormat::Orc, None)` is called
- **THEN** subsequent `create_data_source` calls for "ledger" produce an `OrcDataSource`

#### Scenario: Registering an Avro table (feature gated)
- **WHEN** the `avro` cargo feature is enabled and `factory.register_table("clickstream", path, FileFormat::Avro, None)` is called
- **THEN** subsequent `create_data_source` calls for "clickstream" produce an `AvroDataSource`

## ADDED Requirements

### Requirement: JsonDataSource
The system SHALL define a `JsonDataSource` struct that reads newline-delimited JSON files from a filesystem or object-store path and produces `Vec<RecordBatch>`. It SHALL implement the `DataSource` trait. The schema SHALL be provided explicitly at construction time; there is no schema inference in v1.

#### Scenario: Reading a newline-delimited JSON file
- **WHEN** `JsonDataSource::new(path, schema)` is called with a valid NDJSON file and matching schema
- **THEN** `scan()` returns `Ok(RecordBatchStream)` whose batches contain the parsed records in schema order, projecting missing fields as NULL

#### Scenario: Malformed JSON line
- **WHEN** a JSON file contains a line that is not valid JSON
- **THEN** `scan()` returns an error that identifies the file and line offset

### Requirement: OrcDataSource
Under the `orc` cargo feature the system SHALL define an `OrcDataSource` struct that reads ORC files via the `arrow-orc` reader and produces `Vec<RecordBatch>`. It SHALL implement the `DataSource` trait and derive its schema from the ORC file metadata.

#### Scenario: Reading an ORC file
- **WHEN** `OrcDataSource::new(path)` is called with a valid ORC file path (feature `orc` enabled)
- **THEN** `schema()` returns column info derived from the ORC metadata, and `scan()` returns the file's data as RecordBatches

#### Scenario: ORC feature disabled
- **WHEN** the `orc` feature is not enabled and a caller instantiates `OrcDataSource`
- **THEN** the code SHALL fail to compile (the type is gated behind `#[cfg(feature = "orc")]`)

### Requirement: AvroDataSource
Under the `avro` cargo feature the system SHALL define an `AvroDataSource` struct that reads Avro Object Container Files via an `arrow-avro`-compatible reader and produces `Vec<RecordBatch>`. It SHALL implement the `DataSource` trait and derive its schema from the embedded Avro schema.

#### Scenario: Reading an Avro container file
- **WHEN** `AvroDataSource::new(path)` is called with a valid Avro file (feature `avro` enabled)
- **THEN** `schema()` returns the column info derived from the Avro writer schema, and `scan()` returns the file's data as RecordBatches

#### Scenario: Avro schema evolution (reader schema differs)
- **WHEN** an Avro file's writer schema differs from the reader schema supplied by HMS
- **THEN** the reader SHALL apply Avro schema resolution (promoted types, added fields with defaults) and surface an error only when resolution is impossible

### Requirement: FileConnectorFactory extends to new formats
The `FileConnectorFactory` SHALL accept registrations using any `FileFormat` variant compiled into the build, producing the corresponding `DataSource` on `create_data_source`. Attempting to register a format not compiled in SHALL return a clear error naming the missing feature.

#### Scenario: Registering an ORC table without the orc feature
- **WHEN** the `orc` feature is disabled and `factory.register_table("t", path, FileFormat::Orc, None)` is called via a dynamically-configured registration path
- **THEN** the call SHALL return `Err(ConnectorError::UnsupportedFormat)` naming `orc` as the disabled feature

#### Scenario: Forwarding SerDe / reader parameters
- **WHEN** `factory.create_data_source(&table_ref, &schema, &properties)` is called with CSV-specific keys (`field.delim`, `skip.header.line.count`) in `properties`
- **THEN** the produced `CsvDataSource` SHALL honour those keys when parsing
