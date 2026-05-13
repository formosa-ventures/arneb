# Spec: Query Planner (TPC-H Correctness)

## MODIFIED Requirements

### Requirement: Qualified column references in aggregate context
The planner SHALL correctly resolve qualified column references (e.g.,
`t.name`) after aggregation, even when the post-aggregate context has
unqualified column names. When the FROM clause aliases the same table
twice and the GROUP BY clause names both aliases' instances of a
shared column, each alias's projection in the SELECT list SHALL
resolve to its own group-by output slot — they SHALL NOT collapse to
the first matching slot.

#### Scenario: Table alias in GROUP BY and SELECT
- **WHEN** `SELECT n1.n_name, COUNT(*) FROM nation n1 GROUP BY n1.n_name` is planned
- **THEN** it resolves `n1.n_name` to the unqualified `n_name` in the aggregate output

#### Scenario: Self-join alias preserved through GROUP BY
- **WHEN** `SELECT u1.name, u2.name, COUNT(*) FROM users u1 JOIN users u2 ON u1.id = u2.id GROUP BY u1.name, u2.name` is planned
- **THEN** the Projection has three column references where the first two point at *distinct* aggregate-output indices (one per alias), not the same index

### Requirement: Aggregate with non-group-by column arguments
The planner SHALL correctly resolve aggregate function arguments (e.g., `SUM(age)`) against the pre-aggregate input schema, and the projection after aggregation SHALL reference the aggregate output column by index rather than re-resolving the argument. When two aggregate functions in the same SELECT list share a function name but differ in arguments, each SHALL resolve to its own aggregate-output slot. Aggregate-slot lookup SHALL match by an AST expression formatter that strips column qualifiers and unwraps parenthesized `Nested` nodes, so that the AST form of a SELECT-list aggregate matches the stored aggregate column name (which is built from the unqualified, paren-flattened `PlanExpr` Display).

#### Scenario: GROUP BY with SUM on different column
- **WHEN** `SELECT name, SUM(age) FROM users GROUP BY name` is planned with "users" table (id: Int64, name: Utf8, age: Int32)
- **THEN** it produces Projection(Aggregate(TableScan)) where Projection references aggregate output columns by index

#### Scenario: GROUP BY with multiple aggregates
- **WHEN** `SELECT name, SUM(age), COUNT(*) FROM users GROUP BY name` is planned
- **THEN** it produces an Aggregate with 1 group-by + 2 aggregate expressions, and Projection references all 3 output columns

#### Scenario: Duplicate aggregate function disambiguation
- **WHEN** `SELECT SUM(CASE WHEN age > 18 THEN id ELSE 0 END) AS guarded, SUM(users.id) AS total FROM users` is planned
- **THEN** the Projection contains two distinct Column references at different aggregate-output indices, and the underlying Aggregate node carries exactly two aggregate expressions (not one)

#### Scenario: Aggregate argument with parenthesized sub-expression
- **WHEN** `SELECT SUM(id * (1 - id)) AS s, SUM(age) AS a FROM users` is planned
- **THEN** both SELECT items resolve to Column references in the post-aggregate Projection (neither falls through to `ColumnNotFound`)
