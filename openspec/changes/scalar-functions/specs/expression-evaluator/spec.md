## MODIFIED Requirements

### Requirement: Scalar function invocation in expression evaluator
The expression evaluator SHALL handle `PlanExpr::Function` nodes for scalar functions by: (1) looking up the function name in the `FunctionRegistry`, (2) recursively evaluating all argument expressions, (3) applying type coercion to arguments if needed, and (4) invoking `ScalarFunction::evaluate()` with the evaluated argument arrays. If the function is not found in the registry, it SHALL return `Err(ExecutionError::InvalidOperation(...))` with a descriptive message.

#### Scenario: Evaluating a scalar function call
- **WHEN** `evaluate(PlanExpr::Function { name: "upper", args: [PlanExpr::Column { index: 0 }] }, &batch, &registry)` is called where column 0 is `["hello", "world"]`
- **THEN** it returns a Utf8 array `["HELLO", "WORLD"]`

#### Scenario: Unknown function
- **WHEN** `evaluate(PlanExpr::Function { name: "nonexistent", args: [...] }, &batch, &registry)` is called
- **THEN** it returns `Err(ExecutionError::InvalidOperation("Unknown function: nonexistent"))` or similar

#### Scenario: Nested function calls
- **WHEN** `evaluate(UPPER(SUBSTRING(name, 1, 3)))` is called where name column is `["hello", "world"]`
- **THEN** it evaluates SUBSTRING first to get `["hel", "wor"]`, then UPPER to get `["HEL", "WOR"]`

### Requirement: Type coercion for function arguments
The expression evaluator SHALL apply implicit type coercion to function arguments when the provided types do not match the function's expected types. Coercion rules: (1) integer types widen to larger integer types (Int32 → Int64), (2) integer types widen to float types (Int32/Int64 → Float64) for math functions, (3) non-Utf8 types are not implicitly coerced to Utf8 for string functions (explicit CAST required).

#### Scenario: Integer argument to float math function
- **WHEN** `ROUND(int32_column)` is evaluated where int32_column is `[1, 2, 3]` (Int32)
- **THEN** the Int32 array is cast to Float64 before invoking ROUND, returning `[1.0, 2.0, 3.0]` (Float64)

#### Scenario: Non-coercible argument type
- **WHEN** `UPPER(int32_column)` is evaluated where int32_column is `[1, 2, 3]` (Int32)
- **THEN** it returns `Err(ExecutionError::InvalidOperation(...))` indicating that Int32 cannot be coerced to Utf8
