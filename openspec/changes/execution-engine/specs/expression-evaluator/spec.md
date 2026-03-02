## ADDED Requirements

### Requirement: Expression evaluation
The system SHALL provide an `evaluate()` function that takes a `PlanExpr` and a `RecordBatch` and returns an `ArrayRef`. Column references use the column index to retrieve the array from the batch. Literals are broadcast to constant arrays matching the batch row count.

#### Scenario: Evaluating a column reference
- **WHEN** `evaluate(PlanExpr::Column { index: 0, name: "id" }, &batch)` is called on a batch with column 0 being `[1, 2, 3]`
- **THEN** it returns the `ArrayRef` for column 0 containing `[1, 2, 3]`

#### Scenario: Evaluating a literal
- **WHEN** `evaluate(PlanExpr::Literal(ScalarValue::Int64(42)), &batch)` is called on a batch with 3 rows
- **THEN** it returns an `Int64Array` of `[42, 42, 42]`

#### Scenario: Column index out of bounds
- **WHEN** `evaluate(PlanExpr::Column { index: 99, .. }, &batch)` is called on a batch with 4 columns
- **THEN** it returns `Err(ExecutionError::InvalidOperation(...))`

### Requirement: Binary operations
The evaluator SHALL support all `BinaryOp` variants: arithmetic (Plus, Minus, Multiply, Divide, Modulo), comparison (Eq, NotEq, Lt, LtEq, Gt, GtEq), logical (And, Or), and string matching (Like, NotLike). It SHALL use Arrow compute kernels for all operations.

#### Scenario: Arithmetic with type coercion
- **WHEN** an Int32 column is added to an Int64 column
- **THEN** the Int32 column is cast to Int64 before addition, producing an Int64 result

#### Scenario: Comparison
- **WHEN** `column_a > 1` is evaluated where column_a is `[1, 2, 3]`
- **THEN** it returns a `BooleanArray` of `[false, true, true]`

#### Scenario: LIKE pattern matching
- **WHEN** `column_c LIKE 'he%'` is evaluated where column_c is `["hello", "world", "foo"]`
- **THEN** it returns a `BooleanArray` of `[true, false, false]`

### Requirement: Unary operations
The evaluator SHALL support `UnaryOp::Not` (boolean negation), `UnaryOp::Minus` (arithmetic negation), and `UnaryOp::Plus` (identity).

#### Scenario: Boolean NOT
- **WHEN** `NOT column_d` is evaluated where column_d is `[true, false, true]`
- **THEN** it returns `[false, true, false]`

### Requirement: Null checks
The evaluator SHALL support `IsNull` and `IsNotNull` expressions using Arrow's `is_null` and `is_not_null` kernels.

#### Scenario: IS NULL
- **WHEN** `column IS NULL` is evaluated where the column is `[1, NULL, 3]`
- **THEN** it returns `[false, true, false]`

### Requirement: BETWEEN and IN
The evaluator SHALL support `BETWEEN` (composed as `>= low AND <= high`) and `InList` (composed as OR of equality checks). Both support the `negated` flag.

#### Scenario: BETWEEN
- **WHEN** `a BETWEEN 1 AND 2` is evaluated where a is `[1, 2, 3]`
- **THEN** it returns `[true, true, false]`

#### Scenario: IN list
- **WHEN** `a IN (1, 3)` is evaluated where a is `[1, 2, 3]`
- **THEN** it returns `[true, false, true]`

### Requirement: CAST
The evaluator SHALL support `Cast` expressions using Arrow's `cast()` kernel to convert between data types.

#### Scenario: CAST Int32 to Int64
- **WHEN** `CAST(a AS BIGINT)` is evaluated where a is an Int32 column `[1, 2, 3]`
- **THEN** it returns an Int64 array `[1, 2, 3]`

### Requirement: Numeric type coercion
When a binary operation has mismatched numeric types, the evaluator SHALL automatically widen both sides to a common type: Int32+Int64→Int64, int+float→Float64, Float32+Float64→Float64.

#### Scenario: Mixed type addition
- **WHEN** Int32 column `[1, 2, 3]` is added to Int64 column `[10, 20, 30]`
- **THEN** the result is Int64 `[11, 22, 33]`

### Requirement: Scalar to array conversion
The system SHALL provide a `scalar_to_array()` function that broadcasts a `ScalarValue` to a constant array of a given length. It SHALL support Null, Boolean, Int32, Int64, Float32, Float64, Utf8, and Date32 types.

#### Scenario: Broadcasting a string literal
- **WHEN** `scalar_to_array(ScalarValue::Utf8("test"), 3)` is called
- **THEN** it returns a `StringArray` of `["test", "test", "test"]`
