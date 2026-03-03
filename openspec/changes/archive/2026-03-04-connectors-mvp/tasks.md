## 1. Crate Setup

- [x] 1.1 Add `crates/connectors` to workspace members in root `Cargo.toml`
- [x] 1.2 Add `parquet` to workspace dependencies in root `Cargo.toml`
- [x] 1.3 Create `crates/connectors/Cargo.toml` with package name `trino-connectors`, dependencies: `trino-common`, `trino-catalog`, `trino-execution`, `arrow` (with `csv` feature), `parquet` (with `arrow` feature)
- [x] 1.4 Create `crates/connectors/src/lib.rs` with module declarations and re-exports

## 2. Connector Traits (`traits` module)

- [x] 2.1 Define `ConnectorFactory` trait with `name()` and `create_data_source()` methods
- [x] 2.2 Define `ConnectorRegistry` struct with `register()` and `get()` methods

## 3. Memory Connector (`memory` module)

- [x] 3.1 Define `MemoryTable` struct storing `Vec<ColumnInfo>` + `Vec<RecordBatch>`, implementing `TableProvider`
- [x] 3.2 Define `MemorySchema` struct implementing `SchemaProvider` with `register_table()`
- [x] 3.3 Define `MemoryCatalog` struct implementing `CatalogProvider` with `register_schema()`
- [x] 3.4 Implement `MemoryConnectorFactory` implementing `ConnectorFactory` — looks up `MemoryTable` and creates `InMemoryDataSource`

## 4. File Connector (`file` module)

- [x] 4.1 Define `FileFormat` enum with `Csv` and `Parquet` variants
- [x] 4.2 Implement `CsvDataSource` — reads CSV file using `arrow::csv::ReaderBuilder` with explicit schema, implements `DataSource`
- [x] 4.3 Implement `ParquetDataSource` — reads Parquet file using `parquet::arrow::arrow_reader`, derives schema from file metadata, implements `DataSource`
- [x] 4.4 Implement `FileConnectorFactory` with `register_table(name, path, format, schema)` and `ConnectorFactory` impl
- [x] 4.5 Define `FileTable` struct implementing `TableProvider` for registered file tables
- [x] 4.6 Define `FileSchema` struct implementing `SchemaProvider` listing registered file tables
- [x] 4.7 Define `FileCatalog` struct implementing `CatalogProvider`

## 5. Tests — Connector Traits

- [x] 5.1 Test `ConnectorRegistry` register and get
- [x] 5.2 Test `ConnectorRegistry` get returns None for unregistered

## 6. Tests — Memory Connector

- [x] 6.1 Test `MemoryTable` schema and TableProvider impl
- [x] 6.2 Test `MemorySchema` register_table, table_names, table lookup
- [x] 6.3 Test `MemoryCatalog` register_schema, schema_names, schema lookup
- [x] 6.4 Test `MemoryConnectorFactory` create_data_source with registered table
- [x] 6.5 Test `MemoryConnectorFactory` create_data_source with unknown table returns error

## 7. Tests — File Connector

- [x] 7.1 Test `CsvDataSource` reads a CSV file and produces correct RecordBatch
- [x] 7.2 Test `CsvDataSource` returns error for nonexistent file
- [x] 7.3 Test `ParquetDataSource` reads a Parquet file and produces correct RecordBatch
- [x] 7.4 Test `ParquetDataSource` schema derived from Parquet metadata
- [x] 7.5 Test `FileConnectorFactory` register and create CSV data source
- [x] 7.6 Test `FileConnectorFactory` register and create Parquet data source
- [x] 7.7 Test `FileConnectorFactory` returns error for unregistered table
- [x] 7.8 Test `FileSchema` and `FileCatalog` table listing and metadata

## 8. Integration & Quality

- [x] 8.1 Integration test: register memory table → create data source → scan → verify rows
- [x] 8.2 Integration test: register CSV file → create data source → scan → verify RecordBatch contents
- [x] 8.3 Integration test: register Parquet file → create data source → scan → verify RecordBatch contents
- [x] 8.4 `cargo build` compiles without warnings
- [x] 8.5 `cargo test -p trino-connectors` all tests pass
- [x] 8.6 `cargo clippy -- -D warnings` clean
- [x] 8.7 `cargo fmt -- --check` clean
