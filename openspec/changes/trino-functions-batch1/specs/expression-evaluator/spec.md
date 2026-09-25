## ADDED Requirements

### Requirement: TRY evaluation
The expression evaluator SHALL handle `PlanExpr::Function { name: "TRY", args: [expr] }` itself rather than through the registry: evaluate `expr` over the batch; if that returns an error, evaluate `expr` on each 1-row slice and return the successful values with NULL for failing rows. If every row fails, the result SHALL be an all-NULL array of the batch length. `TRY` with other than one argument SHALL error.

#### Scenario: Partial failure
- **WHEN** `TRY(chr(x))` is evaluated where `x` is `[65, -1, 66]`
- **THEN** it returns `['A', NULL, 'B']`

#### Scenario: Total failure
- **WHEN** `TRY(chr(x))` is evaluated where `x` is `[-1, -2]`
- **THEN** it returns two NULLs
