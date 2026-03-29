## Why

The engine cannot evaluate scalar functions (SUBSTRING, UPPER, ABS, EXTRACT, etc.). These are used in virtually all SQL queries and 8/22 TPC-H queries. Without them, even simple transformations like uppercase or date extraction are impossible. Currently, `PlanExpr::Function` is only handled for aggregate functions by the aggregate operator — scalar function calls hit an error in expression.rs at execution time.

## What Changes

- Create function registry in execution crate (register name → impl)
- Implement ScalarFunction trait with signature validation + evaluation
- String functions: SUBSTRING, UPPER, LOWER, TRIM, CONCAT, LENGTH, REPLACE, POSITION
- Math functions: ABS, ROUND, CEIL, FLOOR, MOD, POWER
- Date functions: EXTRACT(field FROM date), CURRENT_DATE, DATE_TRUNC
- Update expression evaluator to look up and invoke scalar functions
- Add type coercion for function arguments

## Capabilities

### New Capabilities

- `function-registry`: Extensible scalar function registration and lookup. `ScalarFunction` trait with `name()`, `return_type()`, and `evaluate()` methods. `FunctionRegistry` stores registered functions and provides case-insensitive lookup. Default registry pre-populated with all built-in functions at startup.
- `string-functions`: Built-in string scalar functions: SUBSTRING(str, start, length), UPPER, LOWER, TRIM/LTRIM/RTRIM, CONCAT, LENGTH, REPLACE, POSITION. All operate on Arrow Utf8 arrays using Arrow compute kernels where available.
- `math-functions`: Built-in math scalar functions: ABS, ROUND, CEIL, FLOOR, MOD, POWER. Operate on numeric Arrow arrays (Int32, Int64, Float32, Float64) with automatic type coercion.
- `date-functions`: Built-in date scalar functions: EXTRACT(YEAR/MONTH/DAY FROM date), CURRENT_DATE, DATE_TRUNC(field, date). Operate on Date32/Timestamp arrays.

### Modified Capabilities

- `expression-evaluator`: Update `evaluate()` to look up scalar functions from a `FunctionRegistry` when encountering `PlanExpr::Function` nodes. Add basic type coercion for function arguments before invocation.

## Impact

- **Crates**: `execution` (new `functions/` module, modified `expression.rs`)
- **Dependencies**: No new external crate dependencies (uses existing Arrow compute kernels)
- **Unlocks**: TPC-H Q2, Q7, Q8, Q15, Q20, Q22
