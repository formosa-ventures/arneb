## ADDED Requirements

### Requirement: MemoryCatalog implementation
The system SHALL provide a `MemoryCatalog` struct that implements `CatalogProvider` using in-memory storage. It SHALL support registering and deregistering schemas at runtime.

#### Scenario: Registering a schema
- **WHEN** `catalog.register_schema("analytics", schema)` is called
- **THEN** the schema becomes accessible via `catalog.schema("analytics")`

#### Scenario: Deregistering a schema
- **WHEN** `catalog.deregister_schema("analytics")` is called on a catalog with that schema
- **THEN** `catalog.schema("analytics")` returns `None` afterward

#### Scenario: Empty catalog
- **WHEN** a new `MemoryCatalog` is created
- **THEN** `catalog.schema_names()` returns an empty list

### Requirement: MemorySchema implementation
The system SHALL provide a `MemorySchema` struct that implements `SchemaProvider` using in-memory storage. It SHALL support registering and deregistering tables at runtime.

#### Scenario: Registering a table
- **WHEN** `schema.register_table("users", table)` is called
- **THEN** the table becomes accessible via `schema.table("users")`

#### Scenario: Deregistering a table
- **WHEN** `schema.deregister_table("users")` is called on a schema with that table
- **THEN** `schema.table("users")` returns `None` afterward

#### Scenario: Empty schema
- **WHEN** a new `MemorySchema` is created
- **THEN** `schema.table_names()` returns an empty list

### Requirement: MemoryTable implementation
The system SHALL provide a `MemoryTable` struct that implements `TableProvider`, storing a fixed list of `ColumnInfo`. It SHALL be constructable from a `Vec<ColumnInfo>`.

#### Scenario: Creating a table from column info
- **WHEN** `MemoryTable::new(vec![col1, col2])` is called
- **THEN** `table.schema()` returns a vector with those two columns

#### Scenario: Table with no columns
- **WHEN** `MemoryTable::new(vec![])` is called
- **THEN** `table.schema()` returns an empty vector
