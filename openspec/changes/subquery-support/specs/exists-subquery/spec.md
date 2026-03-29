# Spec: EXISTS Subquery

## ADDED Requirements

### Requirement: Parse EXISTS subquery expressions
The SQL parser SHALL recognize `WHERE EXISTS (SELECT ...)` and `WHERE NOT EXISTS (SELECT ...)` as subquery expressions in the AST.

#### Scenario: Simple EXISTS
- **WHEN** a query contains `WHERE EXISTS (SELECT 1 FROM orders WHERE orders.cust_id = customers.id)`
- **THEN** the parser produces an `Exists` AST node containing the subquery.

#### Scenario: NOT EXISTS
- **WHEN** a query contains `WHERE NOT EXISTS (SELECT 1 FROM blacklist WHERE blacklist.id = users.id)`
- **THEN** the parser produces a negated `Exists` AST node.

### Requirement: Plan correlated EXISTS as semi-join
The query planner SHALL rewrite a correlated `EXISTS` subquery as a left semi-join with the correlation predicate as the join condition.

#### Scenario: Correlated EXISTS becomes semi-join
- **WHEN** the planner encounters `WHERE EXISTS (SELECT 1 FROM lineitem l WHERE l.order_id = o.id)`
- **THEN** the logical plan contains a `SemiJoin` node with join condition `l.order_id = o.id`.

#### Scenario: NOT EXISTS becomes anti-join
- **WHEN** the planner encounters `WHERE NOT EXISTS (SELECT 1 FROM returns r WHERE r.order_id = o.id)`
- **THEN** the logical plan contains an `AntiJoin` node with join condition `r.order_id = o.id`.

### Requirement: EXISTS with non-correlated subquery
The planner SHALL handle non-correlated EXISTS subqueries correctly.

#### Scenario: Non-correlated EXISTS returning rows
- **WHEN** the subquery in EXISTS is not correlated and returns at least one row
- **THEN** the EXISTS predicate evaluates to TRUE for all outer rows.

#### Scenario: Non-correlated EXISTS returning no rows
- **WHEN** the subquery in EXISTS is not correlated and returns zero rows
- **THEN** the EXISTS predicate evaluates to FALSE for all outer rows, producing an empty result.

### Requirement: EXISTS ignores SELECT list
The engine MUST NOT require specific columns in the SELECT list of an EXISTS subquery.

#### Scenario: SELECT * in EXISTS
- **WHEN** the subquery is `EXISTS (SELECT * FROM t WHERE ...)`
- **THEN** execution proceeds correctly without materializing unnecessary columns.

#### Scenario: SELECT 1 in EXISTS
- **WHEN** the subquery is `EXISTS (SELECT 1 FROM t WHERE ...)`
- **THEN** execution proceeds correctly and the literal is ignored.
