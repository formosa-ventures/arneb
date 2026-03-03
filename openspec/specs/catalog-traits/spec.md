## ADDED Requirements

### Requirement: CatalogProvider trait
The system SHALL define a `CatalogProvider` trait with methods to list schema names and retrieve a schema by name. The trait SHALL return `Arc<dyn SchemaProvider>` for schema lookups.

#### Scenario: Listing schemas in a catalog
- **WHEN** `catalog.schema_names()` is called on a catalog containing schemas "default" and "analytics"
- **THEN** it returns a `Vec<String>` containing `["default", "analytics"]`

#### Scenario: Retrieving an existing schema
- **WHEN** `catalog.schema("default")` is called and the schema exists
- **THEN** it returns `Some(Arc<dyn SchemaProvider>)`

#### Scenario: Retrieving a non-existent schema
- **WHEN** `catalog.schema("nonexistent")` is called and the schema does not exist
- **THEN** it returns `None`

### Requirement: SchemaProvider trait
The system SHALL define a `SchemaProvider` trait with methods to list table names and retrieve a table by name. The trait SHALL return `Arc<dyn TableProvider>` for table lookups.

#### Scenario: Listing tables in a schema
- **WHEN** `schema.table_names()` is called on a schema containing tables "users" and "orders"
- **THEN** it returns a `Vec<String>` containing `["users", "orders"]`

#### Scenario: Retrieving an existing table
- **WHEN** `schema.table("users")` is called and the table exists
- **THEN** it returns `Some(Arc<dyn TableProvider>)`

#### Scenario: Retrieving a non-existent table
- **WHEN** `schema.table("nonexistent")` is called and the table does not exist
- **THEN** it returns `None`

### Requirement: TableProvider trait
The system SHALL define a `TableProvider` trait that exposes table schema metadata. It SHALL provide a `schema()` method returning a list of `ColumnInfo` describing all columns in the table.

#### Scenario: Getting table schema
- **WHEN** `table.schema()` is called on a table with columns (id: Int64, name: Utf8)
- **THEN** it returns a `Vec<ColumnInfo>` with two entries matching those column definitions

### Requirement: Trait Send + Sync bounds
All catalog traits (`CatalogProvider`, `SchemaProvider`, `TableProvider`) SHALL require `Send + Sync` so that catalog references can be shared across async tasks and threads.

#### Scenario: Sharing catalog across threads
- **WHEN** an `Arc<dyn CatalogProvider>` is sent to another thread
- **THEN** it compiles and can be used from the other thread
