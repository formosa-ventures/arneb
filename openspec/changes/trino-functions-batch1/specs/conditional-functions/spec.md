## ADDED Requirements

### Requirement: IF
The system SHALL support Trino's `IF(condition, true_value [, false_value])` by desugaring it in the SQL parser to `CASE WHEN condition THEN true_value [ELSE false_value] END`. A NULL condition SHALL select the false branch (or NULL when omitted). Calls with fewer than 2 or more than 3 arguments SHALL be rejected at parse time.

#### Scenario: Two branches
- **WHEN** `SELECT IF(id > 1, 'big', 'small') FROM t` is executed where `id` is `[1, 2]`
- **THEN** it returns `['small', 'big']`

#### Scenario: Missing false branch
- **WHEN** `IF(id > 2, 'x')` is evaluated where `id` is `[1, 3]`
- **THEN** it returns `[NULL, 'x']`

### Requirement: TRY
The expression evaluator SHALL evaluate `TRY(expr)` by evaluating `expr` over the batch and, if that fails, re-evaluating it row by row so that only the rows whose evaluation raises an error become NULL. The result type SHALL be the type of `expr`.

#### Scenario: Only failing rows become NULL
- **WHEN** `TRY(chr(x))` is evaluated where `x` is `[65, -1, 66]`
- **THEN** it returns `['A', NULL, 'B']`

#### Scenario: Error-free batch
- **WHEN** `TRY(x)` is evaluated where `x` is `[10, 20, 30]`
- **THEN** it returns `[10, 20, 30]` without row-by-row re-evaluation

### Requirement: GREATEST and LEAST
The system SHALL implement `GREATEST(v1, ..., vn)` and `LEAST(v1, ..., vn)` returning the largest / smallest argument per row. Arguments SHALL be coerced to their common supertype (shared `variadic_supertype` rule used by both planner and executor), which SHALL be the return type. The result SHALL be NULL if any argument is NULL.

#### Scenario: Mixed numeric types
- **WHEN** `LEAST(int_col, double_col)` is evaluated with `[1, 7]` and `[0.5, 9.0]`
- **THEN** it returns DOUBLE `[0.5, 7.0]`

#### Scenario: NULL argument
- **WHEN** `GREATEST(1, NULL)` is evaluated
- **THEN** it returns NULL
