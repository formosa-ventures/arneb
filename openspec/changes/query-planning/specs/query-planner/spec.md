## ADDED Requirements

### Requirement: QueryPlanner struct
The system SHALL define a `QueryPlanner` struct that holds a reference to a `CatalogManager` and converts parsed SQL statements into logical plans.

#### Scenario: Creating a planner
- **WHEN** `QueryPlanner::new(&catalog_manager)` is called
- **THEN** a planner is created that can plan queries against the catalogs in that manager

### Requirement: Planning SELECT statements
The `QueryPlanner` SHALL convert a parsed `Statement::Query` into a `LogicalPlan` by:
1. Resolving FROM clause tables via the catalog
2. Building join trees for multiple FROM items
3. Adding a Filter node for the WHERE clause
4. Adding an Aggregate node for GROUP BY / HAVING
5. Adding a Projection node for the SELECT list (expanding wildcards)
6. Adding a Sort node for ORDER BY
7. Adding a Limit node for LIMIT/OFFSET

#### Scenario: Simple SELECT with filter
- **WHEN** `SELECT name FROM users WHERE id > 10` is planned with a catalog containing "users" table (id: Int64, name: Utf8)
- **THEN** it produces `Projection(Filter(TableScan(users), id > 10), [name])`

#### Scenario: SELECT with wildcard expansion
- **WHEN** `SELECT * FROM users` is planned with a catalog containing "users" table (id: Int64, name: Utf8)
- **THEN** the Projection contains columns for both "id" and "name"

### Requirement: Table resolution via catalog
The planner SHALL resolve each table in the FROM clause using `CatalogManager::resolve_table`. If a table is not found, it SHALL return `PlanError::TableNotFound`.

#### Scenario: Table not found
- **WHEN** `SELECT * FROM nonexistent` is planned
- **THEN** it returns `Err(PlanError::TableNotFound("nonexistent"))`

### Requirement: Column validation
The planner SHALL validate that all column references in the query exist in the schema of the referenced table. If a column is not found, it SHALL return `PlanError::ColumnNotFound`.

#### Scenario: Column not found
- **WHEN** `SELECT nonexistent FROM users` is planned and "users" has columns (id, name)
- **THEN** it returns `Err(PlanError::ColumnNotFound("nonexistent"))`

### Requirement: Planning EXPLAIN
The planner SHALL convert a `Statement::Explain` into a `LogicalPlan::Explain` wrapping the inner plan.

#### Scenario: EXPLAIN SELECT
- **WHEN** `EXPLAIN SELECT * FROM users` is planned
- **THEN** it produces `Explain(Projection(TableScan(users), [*]))`
