## ADDED Requirements

### Requirement: CatalogManager with multi-catalog support
The system SHALL provide a `CatalogManager` struct that holds multiple named catalogs and provides methods to register, deregister, and list catalogs.

#### Scenario: Registering a catalog
- **WHEN** `manager.register_catalog("memory", catalog)` is called
- **THEN** the catalog becomes accessible via `manager.catalog("memory")`

#### Scenario: Listing catalogs
- **WHEN** `manager.catalog_names()` is called on a manager with catalogs "memory" and "hive"
- **THEN** it returns a `Vec<String>` containing both names

#### Scenario: Deregistering a catalog
- **WHEN** `manager.deregister_catalog("memory")` is called on a manager with that catalog
- **THEN** `manager.catalog("memory")` returns `None` afterward

### Requirement: Default catalog and schema
The system SHALL support configuring a default catalog name and default schema name. These defaults are used when resolving table references with fewer than three parts.

#### Scenario: Setting defaults
- **WHEN** a `CatalogManager` is created with `default_catalog = "memory"` and `default_schema = "default"`
- **THEN** `manager.default_catalog()` returns `"memory"` and `manager.default_schema()` returns `"default"`

### Requirement: Table reference resolution
The system SHALL provide a `resolve_table(reference: &TableReference) -> Result<Arc<dyn TableProvider>, CatalogError>` method that resolves a `TableReference` to a `TableProvider`.

Resolution logic:
- Three-part (`catalog.schema.table`): Use all parts as-is
- Two-part (`schema.table`): Use default catalog + provided schema + table
- One-part (`table`): Use default catalog + default schema + table

#### Scenario: Resolving a fully-qualified reference
- **WHEN** `resolve_table` is called with `TableReference { catalog: Some("mem"), schema: Some("public"), table: "users" }`
- **THEN** it looks up catalog "mem", schema "public", table "users" and returns the `TableProvider`

#### Scenario: Resolving a one-part reference with defaults
- **WHEN** `resolve_table` is called with `TableReference { catalog: None, schema: None, table: "users" }` and defaults are catalog="memory", schema="default"
- **THEN** it resolves as `memory.default.users`

#### Scenario: Resolving a two-part reference with default catalog
- **WHEN** `resolve_table` is called with `TableReference { catalog: None, schema: Some("analytics"), table: "events" }` and default catalog is "memory"
- **THEN** it resolves as `memory.analytics.events`

#### Scenario: Catalog not found
- **WHEN** `resolve_table` is called with a catalog name that is not registered
- **THEN** it returns `Err(CatalogError::CatalogNotFound(...))`

#### Scenario: Schema not found
- **WHEN** `resolve_table` is called with a valid catalog but the schema does not exist
- **THEN** it returns `Err(CatalogError::SchemaNotFound(...))`

#### Scenario: Table not found
- **WHEN** `resolve_table` is called with a valid catalog and schema but the table does not exist
- **THEN** it returns `Err(CatalogError::SchemaNotFound(...))` or a table-not-found error

### Requirement: Convenience builder
The system SHALL provide a builder or convenience method on `CatalogManager` to quickly set up a catalog with schemas and tables for testing and MVP usage.

#### Scenario: Building a manager with one catalog and one table
- **WHEN** a `CatalogManager` is constructed with a "memory" catalog containing a "default" schema with a "users" table
- **THEN** `resolve_table` with `TableReference::table("users")` succeeds
