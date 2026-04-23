# type-coercion Specification

## Purpose
TBD - created by archiving change planner-type-coercion. Update Purpose after archive.
## Requirements
### Requirement: Coercion matrix is data-driven

Arneb SHALL define the set of implicit type-coercion rules as a declarative table (constant slice or `lazy_static` map) keyed on `(from: DataType, to: DataType)`, with each entry tagged with a `Safety` classification.

```rust
pub enum Safety {
    AlwaysSafe,
    LiteralOnly,
    PrecisionLoss,
}
```

Adding a new implicit cast SHALL require only a new entry in this table — no change to the traversal or application logic.

#### Scenario: Matrix includes TPC-H essentials

- **WHEN** the matrix is loaded
- **THEN** it includes entries for: Int32↔Int64, Int↔Float64, Int↔Decimal128, Float32↔Float64, Decimal↔Decimal (with precision/scale reconciliation), Utf8→Date32, Utf8→Timestamp

### Requirement: Asymmetric literal safety

The coercion matrix SHALL distinguish safety levels so that `LiteralOnly` rules (e.g., `Utf8 → Date32`) apply **only** when the source operand is a literal or a folded-literal (`PlanExpr::Literal` or `PlanExpr::Cast { expr: Literal, .. }` prior to folding). Column-to-column applications of `LiteralOnly` rules SHALL be rejected at plan time.

#### Scenario: Column-to-literal allowed

- **GIVEN** a column `l_shipdate: Date32` and a literal `'1998-12-01'`
- **WHEN** coercion runs on `l_shipdate <= '1998-12-01'`
- **THEN** the literal is rewritten as `Cast(Literal(Utf8), Date32)`
- **AND** the resulting operator has `Date32 <= Date32`

#### Scenario: Column-to-column rejected

- **GIVEN** a column `str_date: Utf8` and a column `real_date: Date32`
- **WHEN** coercion runs on `str_date <= real_date`
- **THEN** it returns `Err(PlanError::TypeMismatch { left_type: Utf8, right_type: Date32, .. })`
- **AND** the error message hints at using explicit CAST

### Requirement: Common supertype for multi-operand sites

Arneb SHALL provide a `common_supertype(a: &DataType, b: &DataType, site: CoercionSite) -> Option<DataType>` function that computes the smallest type both operands can be safely cast to, given the site context (`Binary`, `CaseBranch`, `UnionColumn`, `InList`, `FunctionArg`).

#### Scenario: Supertype of Int32 and Int64

- **WHEN** `common_supertype(&Int32, &Int64, CoercionSite::Binary{..})` is called
- **THEN** it returns `Some(Int64)`

#### Scenario: Supertype of Decimal(10,2) and Decimal(12,4)

- **WHEN** `common_supertype(&Decimal(10,2), &Decimal(12,4), _)` is called
- **THEN** it returns `Some(Decimal(p, 4))` where `p` covers both original precisions per Trino's formula

#### Scenario: Incompatible types

- **WHEN** `common_supertype(&Boolean, &Date32, _)` is called
- **THEN** it returns `None`

### Requirement: Binary operator coercion

The `TypeCoercion` pass SHALL rewrite `PlanExpr::BinaryOp { left, op, right }` by inserting `Cast` nodes so both operands share a common type, per the matrix. Comparison (`=, !=, <, <=, >, >=`) and logical (`AND, OR`) operators and arithmetic (`+, -, *, /, %`) operators are all covered.

#### Scenario: Arithmetic widening

- **GIVEN** `Int32 + Float64` operands
- **WHEN** coercion runs
- **THEN** the Int32 side is wrapped in `Cast(_, Float64)`

#### Scenario: TPC-H arithmetic

- **GIVEN** `l_extendedprice * (1 - l_discount)` where `extendedprice`, `discount` are Decimal(15,2) and `1` is Int32
- **WHEN** coercion runs
- **THEN** the inner `1 - l_discount` has `1` cast to `Decimal(15,2)`
- **AND** the outer `*` has both sides as a common Decimal supertype

### Requirement: CASE, COALESCE, NULLIF branch unification

The `TypeCoercion` pass SHALL unify the result types of all branches of `CASE`, `COALESCE`, and `NULLIF`. The common supertype is computed across all result expressions; each branch is then wrapped in a `Cast` as needed.

#### Scenario: CASE with mixed numeric branches

- **GIVEN** `CASE WHEN x > 0 THEN 1 ELSE 2.5 END` (Int32 vs Float64)
- **WHEN** coercion runs
- **THEN** the Int32 branch is wrapped in `Cast(_, Float64)`
- **AND** the whole CASE's output type is Float64

#### Scenario: CASE with no common type

- **GIVEN** `CASE WHEN x > 0 THEN 'foo' ELSE 1 END` (Utf8 column result vs Int32)
- **WHEN** coercion runs
- **THEN** it returns `Err(PlanError::TypeMismatch { .. })`

### Requirement: IN list element coercion

The `TypeCoercion` pass SHALL unify the test expression's type with every element of the IN list, inserting `Cast` nodes as needed. `LiteralOnly` rules may apply to list elements that are literals.

#### Scenario: IN with mixed literals

- **GIVEN** `col_int IN (1, 2, 3.5)` where `col_int` is Int32
- **WHEN** coercion runs
- **THEN** `col_int` is cast to Float64
- **AND** `1` and `2` are cast to Float64

### Requirement: Set operation column unification

The `TypeCoercion` pass SHALL, for each column position in `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`, compute the common supertype across all branches' column types and insert `Cast` nodes on each branch's projected expression as needed.

#### Scenario: UNION with integer columns

- **GIVEN** `SELECT a::Int32 FROM t1 UNION SELECT b::Int64 FROM t2`
- **WHEN** coercion runs
- **THEN** branch 1's column is cast to Int64

### Requirement: Function argument coercion

For each `PlanExpr::Function { name, args, .. }` call where Arneb has a known signature (registered via `FunctionRegistry`), the `TypeCoercion` pass SHALL align each argument's type to the signature's expected parameter type, inserting `Cast` nodes as needed. Calls with no signature MAY be deferred to runtime coercion (pre-existing behavior).

#### Scenario: ABS with Int32

- **GIVEN** `ABS(col)` with `col: Int32` and `ABS` signature expects a numeric (any)
- **WHEN** coercion runs
- **THEN** the argument is unchanged (matches signature)

### Requirement: JOIN condition coercion

The `TypeCoercion` pass SHALL apply the same rules as binary-op coercion to each equality pair in a JOIN condition (`left_col = right_col` or `left_expr = right_expr`).

#### Scenario: Join on mismatched integer widths

- **GIVEN** `t1.id (Int32) = t2.id (Int64)`
- **WHEN** coercion runs
- **THEN** the Int32 side is cast to Int64

### Requirement: Plan-time error carries location

Every `PlanError` produced by the `TypeCoercion` pass SHALL include a `location: Option<Location>` populated from the best available source span of the offending expression (`PlanExpr::best_span()`).

#### Scenario: TypeMismatch includes span

- **GIVEN** a query that produces a coercion failure
- **WHEN** the error is caught
- **THEN** `err.location()` returns `Some(Location { line, column })` pointing at the offending operator's source position

### Requirement: ConstantFolding folds Cast over Literal

The `ConstantFolding` optimizer rule SHALL, when encountering `PlanExpr::Cast { expr: Literal(v), data_type }`, attempt to evaluate the cast at plan time and replace the whole expression with `Literal(cast_value)` when successful. If the cast fails (e.g., unparseable date string), a `PlanError` SHALL be raised at plan time rather than at execution.

#### Scenario: DATE literal folded

- **GIVEN** `Cast(Literal(Utf8("1998-12-01")), Date32)`
- **WHEN** ConstantFolding runs
- **THEN** the result is `Literal(Date32(10561))` (days since epoch)

#### Scenario: Invalid DATE literal raises plan-time error

- **GIVEN** `Cast(Literal(Utf8("1998-13-45")), Date32)`
- **WHEN** ConstantFolding runs
- **THEN** the result is `Err(PlanError::InvalidLiteral { .. })` with a location pointing at the literal

### Requirement: Predicate pushdown sees folded literals

After `ConstantFolding` + `TypeCoercion`, `crates/connectors/src/parquet_pushdown.rs::extract_column_literal_comparison` SHALL match predicates of the form `Column op Literal` where the literal's type already matches the column's type, enabling row-group pruning on date/timestamp predicates.

#### Scenario: Row-group pruning on DATE predicate

- **GIVEN** a Parquet file with a Date32 column and row-group statistics
- **AND** a query `WHERE l_shipdate <= DATE '1998-12-01'`
- **WHEN** the query runs
- **THEN** row groups whose min(l_shipdate) > 1998-12-01 are skipped
- **AND** a tracing debug message confirms pruning

### Requirement: Runtime type coercion removed

After `TypeCoercion` lands and the full test suite passes, the functions `coerce_numeric_pair` and `wider_numeric_type` in `crates/execution/src/expression.rs` SHALL be deleted. `compare_op` and `arithmetic_op` SHALL assume their inputs already have matching types and return a clear error (`ExecutionError::TypeMismatch { .. }`) if not — this becomes an internal invariant violation rather than a user-visible case.

#### Scenario: Runtime coercion is no longer needed

- **WHEN** any supported query runs after coercion
- **THEN** `compare_op` / `arithmetic_op` never compute type widening
- **AND** `coerce_numeric_pair` does not exist in the codebase

#### Scenario: Unmatched types at execution are internal bugs

- **GIVEN** a code path that constructs a `compare_op` with mismatched types (e.g., a test that bypasses the planner)
- **WHEN** execution runs
- **THEN** it returns `Err(ExecutionError::TypeMismatch { .. })` with an "internal: analyzer should have resolved this" context

### Requirement: All TPC-H queries pass through coercion path

The 16 TPC-H queries under `benchmarks/tpch/queries/` SHALL, after this change, run successfully against both local Parquet (post `fix-tpch-local-data-types` fix) and Hive-backed data with the `TypeCoercion` pass active. Runtime coercion is not exercised (measurable via absence of calls to the deleted helpers).

#### Scenario: Both Path A and Path B run clean

- **WHEN** the benchmark runner is invoked against each data source
- **THEN** all 16 queries report status=ok
- **AND** no ExecutionError for type coercion is logged

