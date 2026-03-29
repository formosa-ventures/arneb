# Spec: Scalar Subquery

## ADDED Requirements

### Requirement: Parse scalar subquery expressions
The SQL parser SHALL recognize a subquery in a scalar expression context, such as `SELECT (SELECT MAX(x) FROM t)` or `WHERE col = (SELECT MIN(y) FROM t)`.

#### Scenario: Scalar subquery in SELECT list
- **WHEN** a query contains `SELECT name, (SELECT MAX(total) FROM orders) AS max_total FROM customers`
- **THEN** the parser produces a `ScalarSubquery` AST node in the select item expression.

#### Scenario: Scalar subquery in WHERE clause
- **WHEN** a query contains `WHERE price > (SELECT AVG(price) FROM products)`
- **THEN** the parser produces a `ScalarSubquery` AST node in the comparison expression.

### Requirement: Plan scalar subquery as independent execution
The query planner SHALL plan a non-correlated scalar subquery as an independently executed subplan whose single result value is inlined into the outer expression.

#### Scenario: Non-correlated scalar subquery
- **WHEN** the planner encounters `(SELECT MAX(total) FROM orders)` with no outer references
- **THEN** the logical plan contains a `ScalarSubquery` node that executes the subquery independently.

### Requirement: Plan correlated scalar subquery
The query planner SHALL plan a correlated scalar subquery using a nested-loop strategy that re-executes the subquery for each outer row.

#### Scenario: Correlated scalar subquery
- **WHEN** the planner encounters `(SELECT MAX(l.price) FROM lineitem l WHERE l.order_id = o.id)`
- **THEN** the logical plan uses a nested-loop node that binds `o.id` for each outer row.

### Requirement: Enforce single-row constraint
The engine MUST raise a runtime error if a scalar subquery returns more than one row.

#### Scenario: Scalar subquery returns one row
- **WHEN** the scalar subquery execution returns exactly one row with one column
- **THEN** the value is extracted and used as a scalar in the parent expression.

#### Scenario: Scalar subquery returns zero rows
- **WHEN** the scalar subquery execution returns zero rows
- **THEN** the result SHALL be NULL.

#### Scenario: Scalar subquery returns multiple rows
- **WHEN** the scalar subquery execution returns more than one row
- **THEN** a runtime error SHALL be raised indicating that a scalar subquery produced more than one row.

### Requirement: Scalar subquery type compatibility
The scalar subquery result MUST be type-compatible with the surrounding expression context.

#### Scenario: Type mismatch
- **WHEN** a scalar subquery returning a string is used in an arithmetic expression expecting a number
- **THEN** the engine SHALL raise a type error during planning or execution.
