## 1. Function Registry

- [x] 1.1 Define `ScalarFunction` trait with `name()`, `return_type(arg_types: &[DataType]) -> Result<DataType>`, `evaluate(args: &[ArrayRef]) -> Result<ArrayRef>`
- [x] 1.2 Implement `FunctionRegistry` with `register()` and `get()` (case-insensitive lookup via lowercase keys)
- [x] 1.3 Create `default_registry()` that returns a `FunctionRegistry` pre-populated with all built-in functions
- [x] 1.4 Wire `Arc<FunctionRegistry>` into `ExecutionContext`

## 2. String Functions

- [x] 2.1 Implement `UpperFunction` and `LowerFunction` using `arrow::compute::kernels::string::upper` and `lower`
- [x] 2.2 Implement `SubstringFunction` using `arrow::compute::kernels::substring`
- [x] 2.3 Implement `TrimFunction`, `LtrimFunction`, `RtrimFunction` using Arrow trim kernels or manual iteration
- [x] 2.4 Implement `ConcatFunction` (variadic, concatenates all Utf8 arguments) and `LengthFunction` using Arrow `length` kernel
- [x] 2.5 Implement `ReplaceFunction` (manual iteration over string array) and `PositionFunction` (find substring offset)
- [x] 2.6 Write tests for all string functions: normal input, null handling, empty strings, edge cases

## 3. Math Functions

- [x] 3.1 Implement `AbsFunction` using `arrow::compute::kernels::numeric::abs`
- [x] 3.2 Implement `RoundFunction`, `CeilFunction`, `FloorFunction` using Arrow math kernels
- [x] 3.3 Implement `ModFunction` (modulo) and `PowerFunction` (exponentiation) via element-wise computation
- [x] 3.4 Write tests for all math functions: integer input, float input, null handling, negative values, edge cases (division by zero for MOD)

## 4. Date Functions

- [x] 4.1 Implement `ExtractFunction` for EXTRACT(YEAR/MONTH/DAY FROM date) — decompose Date32 days-since-epoch into components
- [x] 4.2 Implement `CurrentDateFunction` returning today's date as a constant Date32 array
- [x] 4.3 Implement `DateTruncFunction` for DATE_TRUNC('year'/'month'/'day', date) — truncate date to specified precision
- [x] 4.4 Write tests for date functions: known dates, null handling, all extract fields, all truncation levels

## 5. Expression Evaluator Integration

- [x] 5.1 Update `evaluate()` in `expression.rs` to handle `PlanExpr::Function` by looking up the function in `FunctionRegistry`, evaluating argument expressions, and invoking `ScalarFunction::evaluate()`
- [x] 5.2 Add basic type coercion for function arguments: numeric widening (Int32 → Int64 → Float64) and CAST to expected types when safe
- [x] 5.3 End-to-end test: `SELECT UPPER(name), ABS(balance) FROM table` through logical plan → physical plan → execute → verify RecordBatch contents
- [x] 5.4 `cargo build` compiles without warnings
- [x] 5.5 `cargo test -p trino-execution` — all tests pass
- [x] 5.6 `cargo clippy -- -D warnings` — clean
- [x] 5.7 `cargo fmt -- --check` — clean
