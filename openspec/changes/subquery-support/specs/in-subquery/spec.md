# Spec: IN Subquery

## ADDED Requirements

### Requirement: Parse IN subquery expressions
The SQL parser SHALL recognize `WHERE column IN (SELECT ...)` and `WHERE column NOT IN (SELECT ...)` as subquery expressions in the AST.

#### Scenario: Simple IN subquery
- **WHEN** a query contains `WHERE id IN (SELECT customer_id FROM customers)`
- **THEN** the parser produces an `InSubquery` AST node with the column reference and the subquery.

#### Scenario: NOT IN subquery
- **WHEN** a query contains `WHERE id NOT IN (SELECT customer_id FROM blacklist)`
- **THEN** the parser produces a negated `InSubquery` AST node.

### Requirement: Plan IN subquery as semi-join
The query planner SHALL rewrite an uncorrelated `IN` subquery as a left semi-join between the outer query and the subquery.

#### Scenario: Uncorrelated IN becomes semi-join
- **WHEN** the planner encounters `WHERE x IN (SELECT y FROM t)` with no correlation
- **THEN** the logical plan contains a `SemiJoin` node joining on `x = y`.

#### Scenario: NOT IN becomes anti-join
- **WHEN** the planner encounters `WHERE x NOT IN (SELECT y FROM t)`
- **THEN** the logical plan contains an `AntiJoin` node joining on `x = y`.

### Requirement: Execute semi-join correctly
The semi-join operator SHALL return each left row at most once when a matching right row exists.

#### Scenario: Duplicates on the right side
- **WHEN** the right side of a semi-join contains duplicate values for the join key
- **THEN** the left row is returned exactly once, not once per duplicate.

#### Scenario: No matches
- **WHEN** no right-side rows match a given left row
- **THEN** that left row is excluded from the result.

### Requirement: Handle NULL values in IN subquery
The engine MUST follow SQL semantics for NULL handling in IN lists.

#### Scenario: NULL in subquery result
- **WHEN** the subquery result contains NULL values and the outer column value does not match any non-NULL value
- **THEN** the IN predicate evaluates to NULL (unknown), not FALSE.
