## MODIFIED Requirements

### Requirement: CatalogProvider trait is async
The `CatalogProvider` trait methods SHALL be async, allowing implementations to perform I/O during catalog operations.

#### Scenario: Async schema lookup
- **WHEN** the planner calls `catalog.schema("default").await`
- **THEN** the system SHALL resolve the schema asynchronously and return `Option<Arc<dyn SchemaProvider>>`

### Requirement: SchemaProvider trait is async
The `SchemaProvider` trait methods SHALL be async, allowing implementations to perform I/O during schema operations.

#### Scenario: Async table listing
- **WHEN** the system calls `schema.table_names().await`
- **THEN** the system SHALL return table names asynchronously

#### Scenario: Async table lookup
- **WHEN** the system calls `schema.table("events").await`
- **THEN** the system SHALL resolve the table asynchronously and return `Option<Arc<dyn TableProvider>>`

### Requirement: CatalogManager async resolution
The `CatalogManager::resolve_table()` method SHALL be async.

#### Scenario: Async table resolution
- **WHEN** the planner resolves a three-part table reference
- **THEN** the system SHALL call async methods on CatalogProvider and SchemaProvider and return the resolved TableProvider
