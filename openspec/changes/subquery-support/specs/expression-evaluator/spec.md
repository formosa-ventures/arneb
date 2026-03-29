# Spec: Expression Evaluator (Subquery Support)

## MODIFIED Requirements

### Requirement: Evaluate scalar subquery results
The expression evaluator SHALL support a `ScalarSubquery` expression variant that holds a pre-computed scalar value obtained from executing a subquery.

#### Scenario: Scalar subquery in arithmetic
- **WHEN** evaluating `price - (SELECT AVG(price) FROM products)` where the scalar subquery resolved to 42.5
- **THEN** the evaluator computes `price - 42.5` for each row.

#### Scenario: Scalar subquery result is NULL
- **WHEN** the scalar subquery resolved to NULL (zero rows returned)
- **THEN** the evaluator propagates NULL through the expression following standard SQL NULL semantics.

### Requirement: Evaluate IN subquery as boolean
The expression evaluator SHALL support evaluating the result of an IN subquery rewritten as a semi-join by checking join membership.

#### Scenario: Row matches semi-join
- **WHEN** the semi-join operator marks a row as having a match
- **THEN** the IN predicate evaluates to TRUE for that row.

#### Scenario: Row does not match semi-join
- **WHEN** the semi-join operator does not find a match for a row
- **THEN** the IN predicate evaluates to FALSE for that row.

### Requirement: Evaluate EXISTS as boolean
The expression evaluator SHALL treat the EXISTS predicate result as a boolean derived from the semi-join operator.

#### Scenario: EXISTS is TRUE
- **WHEN** the semi-join finds at least one matching row for the outer row
- **THEN** the EXISTS expression evaluates to TRUE.

#### Scenario: EXISTS is FALSE
- **WHEN** the semi-join finds no matching rows for the outer row
- **THEN** the EXISTS expression evaluates to FALSE.

### Requirement: Preserve existing expression evaluation
All existing expression evaluation (literals, column references, arithmetic, comparisons, LIKE, CAST, CASE) MUST continue to work unchanged.

#### Scenario: Non-subquery expression
- **WHEN** evaluating an expression with no subquery components
- **THEN** the evaluator produces the same result as before this change.
