## Context

arneb has a working execution engine with expression evaluation, aggregate functions, and physical operators. Currently `PlanExpr::Function` is only handled for aggregate functions by the `HashAggregateExec` operator. Scalar function calls (e.g., `UPPER('hello')`, `ABS(-1)`) are not evaluated — they produce an error in `expression.rs`. The function registry and scalar function implementations will live in a new `functions/` module within the `execution` crate.

Project conventions: `Arc<dyn Trait>` for polymorphism, `thiserror` for errors, trait-based extensibility, Arrow columnar format for all intermediate data. Functions operate on Arrow arrays (vectorized), not row-by-row.

## Goals / Non-Goals

**Goals:**

- Define a `ScalarFunction` trait for vectorized function evaluation on Arrow arrays
- Implement an extensible `FunctionRegistry` with case-insensitive lookup
- Implement 20+ built-in scalar functions across string, math, and date categories
- Update the expression evaluator to resolve and invoke scalar functions
- Add basic type coercion for function arguments
- Use Arrow compute kernels where available for performance and null handling

**Non-Goals:**

- User-defined functions (UDFs) — extensibility via the trait is sufficient for now
- Aggregate function registration (already works via `create_accumulator()`)
- Window functions (separate Phase 2 change)
- Table-valued functions
- Function overload resolution (single signature per function name)

## Decisions

### D1: ScalarFunction trait design

**Choice**: Three methods — `fn name() -> &str`, `fn return_type(arg_types: &[DataType]) -> Result<DataType>`, `fn evaluate(args: &[ArrayRef]) -> Result<ArrayRef>`.

**Rationale**: `return_type()` enables the planner/optimizer to infer output types without executing. `evaluate()` takes `&[ArrayRef]` for vectorized execution on entire columns. This mirrors the DataFusion scalar UDF pattern and keeps the interface minimal.

**Alternative**: Row-by-row evaluation (`fn evaluate_row(args: &[ScalarValue]) -> ScalarValue`). Rejected because it loses vectorization benefits and does not leverage Arrow compute kernels.

### D2: FunctionRegistry as HashMap

**Choice**: `FunctionRegistry` wraps `HashMap<String, Arc<dyn ScalarFunction>>`. Function names are stored lowercase for case-insensitive lookup.

**Rationale**: Simple, O(1) lookup. `Arc<dyn ScalarFunction>` allows shared ownership when the registry is passed into the expression evaluator. Pre-populated with all built-in functions via `default_registry()`.

**Alternative**: Trait-based plugin system with dynamic loading. Rejected — over-engineered for built-in functions. The HashMap approach still allows external registration via `register()`.

### D3: Use Arrow compute kernels where available

**Choice**: Delegate to `arrow::compute::kernels::*` for operations like `substring`, `upper`, `lower`, `length`, `cast`, `abs`, `ceil`, `floor`, `round`.

**Rationale**: Arrow kernels are SIMD-optimized, handle null propagation correctly, and are well-tested. No need to reimplement.

**Trade-off**: Some functions (POSITION, REPLACE, DATE_TRUNC) are not covered by Arrow compute kernels and require manual implementation iterating over array elements.

### D4: Type coercion for function arguments

**Choice**: Implicit CAST inserted when argument types do not match the function signature. Coercion rules: integer → float for math functions, any → Utf8 for string functions where reasonable.

**Rationale**: Matches SQL semantics. `SELECT ROUND(integer_col, 2)` should work without explicit CAST. Coercion is applied in the expression evaluator before calling `evaluate()`.

**Alternative**: Require exact type matching. Rejected — too restrictive for SQL usability.

### D5: Wire registry into ExecutionContext

**Choice**: `ExecutionContext` holds an `Arc<FunctionRegistry>`. The expression evaluator receives a reference to the registry when evaluating `PlanExpr::Function` nodes.

**Rationale**: The registry needs to be accessible during expression evaluation, which happens inside operators. Passing it through `ExecutionContext` follows the existing pattern for `DataSource` registration.

## Risks / Trade-offs

**[Manual implementations]** → Some functions (POSITION, REPLACE, DATE_TRUNC) lack Arrow compute kernel support and require element-wise iteration. **Mitigation**: Implement with `as_string_array()` iteration. Performance is acceptable for MVP data sizes. Arrow may add these kernels in future versions.

**[Type coercion complexity]** → Implicit coercion can produce surprising results (e.g., CONCAT(integer, string)). **Mitigation**: Keep coercion rules conservative — only numeric widening and obvious conversions. Reject ambiguous cases with a clear error.

**[Date function limitations]** → Date32 stores days since epoch, limiting precision to day-level. **Mitigation**: Sufficient for TPC-H queries. Timestamp support can be extended later.
