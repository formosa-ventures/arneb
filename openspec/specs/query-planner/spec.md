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

### Requirement: Aggregate with non-group-by column arguments
The planner SHALL correctly resolve aggregate function arguments (e.g., `SUM(age)`) against the pre-aggregate input schema, and the projection after aggregation SHALL reference the aggregate output column by index rather than re-resolving the argument.

#### Scenario: GROUP BY with SUM on different column
- **WHEN** `SELECT name, SUM(age) FROM users GROUP BY name` is planned with "users" table (id: Int64, name: Utf8, age: Int32)
- **THEN** it produces Projection(Aggregate(TableScan)) where Projection references aggregate output columns by index

#### Scenario: GROUP BY with multiple aggregates
- **WHEN** `SELECT name, SUM(age), COUNT(*) FROM users GROUP BY name` is planned
- **THEN** it produces an Aggregate with 1 group-by + 2 aggregate expressions, and Projection references all 3 output columns

### Requirement: Implicit aggregate without GROUP BY
The planner SHALL detect aggregate functions in the SELECT list even when no GROUP BY clause is present, and create an Aggregate node with an empty group-by (entire input = one group).

#### Scenario: SUM without GROUP BY
- **WHEN** `SELECT SUM(age) FROM users` is planned
- **THEN** it produces Projection(Aggregate(TableScan)) with empty group_by and one aggr_expr

#### Scenario: COUNT without GROUP BY
- **WHEN** `SELECT COUNT(*) FROM users` is planned
- **THEN** it produces Projection(Aggregate(TableScan)) with empty group_by and COUNT(*) aggr_expr

### Requirement: ORDER BY on aggregate expressions
The planner SHALL resolve ORDER BY expressions that reference aggregate functions (e.g., `ORDER BY SUM(x) DESC`) by matching against projection output column names.

#### Scenario: ORDER BY SUM
- **WHEN** `SELECT name, SUM(age) FROM users GROUP BY name ORDER BY SUM(age) DESC` is planned
- **THEN** the Sort node references the SUM output column from the Projection, not re-resolving SUM(age)

### Requirement: Qualified column references in aggregate context
The planner SHALL correctly resolve qualified column references (e.g., `t.name`) after aggregation, even when the post-aggregate context has unqualified column names.

#### Scenario: Table alias in GROUP BY and SELECT
- **WHEN** `SELECT n1.n_name, COUNT(*) FROM nation n1 GROUP BY n1.n_name` is planned
- **THEN** it resolves `n1.n_name` to the unqualified `n_name` in the aggregate output

### Requirement: ORDER BY on aliased columns
The planner SHALL resolve ORDER BY expressions that reference a column by its pre-alias name when the SELECT list renames it with AS. For example, `SELECT n_name AS nation ... ORDER BY n_name` SHALL resolve `n_name` to the projection output column `nation`.

#### Scenario: ORDER BY references pre-alias name
- **WHEN** `SELECT n_name AS nation, SUM(x) FROM t GROUP BY n_name ORDER BY n_name` is planned
- **THEN** the Sort node references the projected column (aliased to `nation`), not re-resolving `n_name`

### Requirement: Complex expressions wrapping aggregates in projection
The planner SHALL handle SELECT expressions that contain arithmetic on aggregate functions (e.g., `100 * SUM(x) / SUM(y)`). It SHALL rewrite the aggregate sub-expressions as column references to the aggregate output, then plan the remaining arithmetic normally.

#### Scenario: Arithmetic on aggregates
- **WHEN** `SELECT 100 * SUM(a) / SUM(b) FROM t` is planned
- **THEN** the Projection contains a BinaryOp that references the two SUM aggregate output columns by index

### Requirement: HAVING with aggregate expression rewriting
The planner SHALL apply the same aggregate-to-column-reference rewriting for HAVING expressions as it does for the SELECT projection. Aggregate functions in HAVING SHALL reference aggregate output columns, not re-resolve their arguments.

#### Scenario: HAVING with SUM
- **WHEN** `SELECT x, SUM(y) FROM t GROUP BY x HAVING SUM(y) > 100` is planned
- **THEN** the Filter node's predicate references the SUM output column, not re-resolving `y`
