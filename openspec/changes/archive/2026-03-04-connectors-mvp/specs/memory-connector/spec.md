## ADDED Requirements

### Requirement: MemoryTable
The system SHALL define a `MemoryTable` struct that stores both column metadata (`Vec<ColumnInfo>`) and data (`Vec<RecordBatch>`). It SHALL implement the catalog `TableProvider` trait (returning its schema). It SHALL provide access to its stored batches for the connector factory.

#### Scenario: Creating a MemoryTable with data
- **WHEN** `MemoryTable::new(schema, batches)` is called with a schema and RecordBatches
- **THEN** `table.schema()` returns the provided column info and the stored batches are accessible

#### Scenario: MemoryTable as TableProvider
- **WHEN** a `MemoryTable` is used as `Arc<dyn TableProvider>`
- **THEN** `schema()` returns the column metadata matching the stored data

### Requirement: MemorySchema
The system SHALL define a `MemorySchema` struct that stores named `MemoryTable` instances. It SHALL implement the catalog `SchemaProvider` trait. It SHALL provide `register_table(name, table)` for adding tables.

#### Scenario: Registering and retrieving a table
- **WHEN** `schema.register_table("users", table)` is called followed by `schema.table("users")`
- **THEN** it returns `Some(Arc<dyn TableProvider>)` pointing to the registered table

#### Scenario: Listing table names
- **WHEN** `schema.table_names()` is called on a schema with tables "users" and "orders"
- **THEN** it returns a list containing both names

### Requirement: MemoryCatalog
The system SHALL define a `MemoryCatalog` struct that stores named `MemorySchema` instances. It SHALL implement the catalog `CatalogProvider` trait. It SHALL provide `register_schema(name, schema)` for adding schemas.

#### Scenario: Registering and retrieving a schema
- **WHEN** `catalog.register_schema("default", schema)` is called followed by `catalog.schema("default")`
- **THEN** it returns `Some(Arc<dyn SchemaProvider>)` pointing to the registered schema

### Requirement: MemoryConnectorFactory
The system SHALL implement `ConnectorFactory` for memory-backed tables. Given a `TableReference`, it SHALL look up the `MemoryTable` and create an `InMemoryDataSource` from its stored batches.

#### Scenario: Creating a DataSource from a MemoryTable
- **WHEN** `factory.create_data_source(&table_ref, &schema)` is called for a registered memory table with 3 rows
- **THEN** it returns an `Arc<dyn DataSource>` whose `scan()` returns those 3 rows

#### Scenario: Table not found in memory connector
- **WHEN** `factory.create_data_source(&table_ref, &schema)` is called for an unregistered table
- **THEN** it returns `Err(ConnectorError::TableNotFound(...))`
