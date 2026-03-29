# Spec: Query Planner (Subquery Support)

## MODIFIED Requirements

### Requirement: Detect subquery expressions in WHERE clause
The query planner SHALL walk the WHERE clause AST and identify `InSubquery`, `Exists`, and `ScalarSubquery` expression nodes.

#### Scenario: Multiple subquery types in one query
- **WHEN** a query contains both an IN subquery and an EXISTS subquery in the WHERE clause
- **THEN** the planner detects and processes each subquery independently.

### Requirement: Classify subqueries as correlated or uncorrelated
The planner SHALL analyze each subquery to determine whether it references columns from the outer query scope.

#### Scenario: Uncorrelated subquery
- **WHEN** a subquery contains no references to outer query columns
- **THEN** the planner classifies it as uncorrelated.

#### Scenario: Correlated subquery
- **WHEN** a subquery references a column from the outer query (e.g., `WHERE inner.id = outer.id`)
- **THEN** the planner classifies it as correlated.

### Requirement: Rewrite uncorrelated IN as semi-join
The planner SHALL transform `WHERE x IN (SELECT y FROM ...)` into a `SemiJoin` logical plan node when the subquery is uncorrelated.

#### Scenario: IN subquery rewrite
- **WHEN** the planner processes an uncorrelated IN subquery
- **THEN** the resulting logical plan contains a `SemiJoin` node with the subquery as the right child and the join key derived from the IN column.

### Requirement: Rewrite EXISTS as semi-join
The planner SHALL transform `WHERE EXISTS (SELECT ... WHERE corr_pred)` into a `SemiJoin` node using the correlation predicate as the join condition.

#### Scenario: EXISTS rewrite
- **WHEN** the planner processes a correlated EXISTS subquery with condition `l.order_id = o.id`
- **THEN** the resulting logical plan contains a `SemiJoin` with join condition on those columns.

### Requirement: Handle nested subqueries
The planner SHALL support subqueries nested within other subqueries.

#### Scenario: Subquery within a subquery
- **WHEN** an IN subquery contains another IN subquery in its WHERE clause
- **THEN** the planner recursively processes both subqueries and produces a valid plan.

### Requirement: Preserve existing planning behavior
All existing query planning functionality (simple SELECT, JOIN, GROUP BY, ORDER BY) MUST continue to work unchanged.

#### Scenario: Query without subqueries
- **WHEN** a query contains no subquery expressions
- **THEN** the planner produces the same plan as before this change.
