## Why

The planner needs to resolve table references (e.g., `SELECT * FROM users`) into concrete schemas (column names and types). Without a catalog system, there is no way to validate that tables exist or to determine their structure. The catalog is the bridge between SQL parsing and query planning — it answers "what columns does this table have?" and "does this table exist?".

## What Changes

- Create `crates/catalog/` crate (package name: `arneb-catalog`)
- Define a `CatalogProvider` trait for registering and looking up catalogs
- Define a `SchemaProvider` trait for listing and resolving tables within a schema
- Define a `TableProvider` trait representing a table's schema metadata
- Implement an in-memory catalog provider for MVP use (tests, built-in tables)
- Provide a `CatalogManager` that composes multiple catalogs with default catalog/schema resolution

## Capabilities

### New Capabilities

- `catalog-traits`: Core trait definitions (`CatalogProvider`, `SchemaProvider`, `TableProvider`) that abstract catalog metadata access. These traits will also be used by connector implementations in later phases.
- `catalog-memory`: In-memory catalog implementation for testing and built-in data. Supports programmatic registration of schemas and tables.
- `catalog-manager`: Top-level `CatalogManager` that holds registered catalogs, resolves multi-part table references (catalog.schema.table), and applies default catalog/schema when parts are omitted.

### Modified Capabilities

(No existing capabilities modified)

## Impact

- **New crate**: `crates/catalog/`
- **New dependency**: `arneb-common` (for `TableReference`, `ColumnInfo`, `DataType`, `CatalogError`)
- **Downstream impact**: The `planner` crate will use `CatalogManager` to resolve table references during planning
- **Connector integration point**: Future connectors will implement `SchemaProvider`/`TableProvider` to expose their tables through the catalog
