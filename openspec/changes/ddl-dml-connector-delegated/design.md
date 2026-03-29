# Design: DDL/DML Connector-Delegated

## Overview

DDL and DML operations are implemented as a delegation pattern. The engine parses and plans these statements, but the actual data mutation is performed by connectors that implement the `DDLProvider` trait. Views are handled at the catalog level as named subqueries.

## DDLProvider Trait

A new trait defines the interface for connectors that support write operations:

```rust
pub trait DDLProvider: Send + Sync {
    /// Create a new table with the given schema.
    fn create_table(&self, name: &str, schema: &Schema) -> Result<()>;

    /// Drop an existing table.
    fn drop_table(&self, name: &str) -> Result<()>;

    /// Insert record batches into an existing table.
    fn insert_into(&self, name: &str, batches: Vec<RecordBatch>) -> Result<u64>;

    /// Delete rows matching a predicate. None means delete all.
    fn delete_from(&self, name: &str, predicate: Option<&Expr>) -> Result<u64>;

    /// Create a table and populate it with the given record batches.
    fn create_table_as_select(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()>;
}
```

The trait is **optional**. Connectors that do not support writes do not implement it. The engine checks for DDLProvider support at execution time and returns a clear error if the connector does not provide it.

## Connector Implementations

### Memory Connector

The memory connector implements all DDLProvider methods:

- **create_table**: Creates a new entry in the in-memory table map with an empty data set and the specified schema.
- **drop_table**: Removes the entry from the in-memory table map.
- **insert_into**: Appends record batches to the existing table's data.
- **delete_from**: Filters out rows matching the predicate. If no predicate, truncates the table.
- **create_table_as_select**: Creates a new table and populates it with the provided batches.

### File Connector

The file connector implements a subset of DDLProvider:

- **create_table_as_select**: Writes the provided record batches as a Parquet file to the configured output directory. The file path is derived from the table name.
- **create_table**: Not supported (returns error). File tables are defined by file paths in config.
- **drop_table**: Not supported. File management is external to the engine.
- **insert_into**: Not supported. Appending to existing files is not implemented.
- **delete_from**: Not supported. File modification is not implemented.

## View Support

Views are implemented at the catalog level, not at the connector level:

1. **CREATE VIEW**: Stores the view name and its defining SQL/subquery in the catalog's view registry.
2. **DROP VIEW**: Removes the view from the registry.
3. **Query resolution**: When a table reference matches a view name, the planner substitutes the view's subquery in place of a table scan.

Views are non-materialized. They are expanded inline at planning time on every query.

### View Storage

Views are stored in the catalog as:

```rust
struct ViewDefinition {
    name: String,
    sql: String,           // Original SQL for display
    plan: LogicalPlan,     // Pre-planned logical plan for substitution
}
```

## Planning Phase

New logical plan nodes:

- `LogicalPlan::CreateTable { name, schema }` — Delegates to DDLProvider::create_table.
- `LogicalPlan::DropTable { name }` — Delegates to DDLProvider::drop_table.
- `LogicalPlan::InsertInto { table, source }` — Executes source plan, delegates batches to DDLProvider::insert_into.
- `LogicalPlan::DeleteFrom { table, predicate }` — Delegates to DDLProvider::delete_from.
- `LogicalPlan::CreateTableAsSelect { name, source }` — Executes source plan, delegates batches to DDLProvider::create_table_as_select.
- `LogicalPlan::CreateView { name, sql, plan }` — Registers view in catalog.
- `LogicalPlan::DropView { name }` — Removes view from catalog.

## Execution Phase

DDL/DML operators return a simple result message (e.g., "CREATE TABLE", "INSERT 0 5", "DROP TABLE") rather than data rows. The protocol layer formats these as command-complete responses.

## Data Flow

```
DDL/DML SQL Statement
  → Parser → AST (CreateTable, InsertInto, etc.)
  → Planner → LogicalPlan (DDL/DML node)
  → ExecutionContext:
      - For InsertInto/CTAS: execute source subplan → Vec<RecordBatch>
      - Delegate to connector's DDLProvider
      - Return command-complete message
  → Protocol → PostgreSQL command-complete response
```
