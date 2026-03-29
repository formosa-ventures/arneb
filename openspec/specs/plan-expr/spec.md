## ADDED Requirements

### Requirement: PlanExpr enum for logical plan expressions
The system SHALL define a `PlanExpr` enum with variants for expressions within logical plans:
- `Column { index: usize, name: String }` — reference to a column by position in the input schema
- `Literal(ScalarValue)` — a constant value
- `BinaryOp { left: Box<PlanExpr>, op: BinaryOp, right: Box<PlanExpr> }` — binary operation
- `UnaryOp { op: UnaryOp, expr: Box<PlanExpr> }` — unary operation
- `Function { name: String, args: Vec<PlanExpr>, distinct: bool }` — function/aggregate call
- `IsNull(Box<PlanExpr>)` — null check
- `IsNotNull(Box<PlanExpr>)` — non-null check
- `Between { expr: Box<PlanExpr>, negated: bool, low: Box<PlanExpr>, high: Box<PlanExpr> }` — range check
- `InList { expr: Box<PlanExpr>, list: Vec<PlanExpr>, negated: bool }` — membership check
- `Cast { expr: Box<PlanExpr>, data_type: DataType }` — type cast
- `Wildcard` — represents `*` before expansion

#### Scenario: Resolved column reference
- **WHEN** column "name" is the second column (index 1) in the input schema
- **THEN** it is represented as `PlanExpr::Column { index: 1, name: "name" }`

#### Scenario: Literal expression
- **WHEN** the SQL literal `42` appears in a plan expression
- **THEN** it is represented as `PlanExpr::Literal(ScalarValue::Int64(42))`

### Requirement: Display for PlanExpr
The `PlanExpr` enum SHALL implement `Display` for human-readable output in EXPLAIN plans.

#### Scenario: Displaying a binary operation
- **WHEN** a `PlanExpr::BinaryOp { left: Column("a"), op: Gt, right: Literal(1) }` is displayed
- **THEN** it outputs `a > 1`

### Requirement: Type inference for BinaryOp expressions
The planner SHALL infer the output data type of `PlanExpr::BinaryOp` based on the operand types:
- Comparison operators (=, !=, <, >, <=, >=, AND, OR, LIKE) SHALL return Boolean
- If either operand is Float64 or Float32, arithmetic result SHALL be Float64
- If either operand is Int64, arithmetic result SHALL be Int64
- If one side is Null and the other is known, the known type SHALL be used

#### Scenario: Float arithmetic type
- **WHEN** `expr_to_column_info` is called for `PlanExpr::BinaryOp { Literal(100.0), Multiply, Column(SUM_result: Float64) }`
- **THEN** the resulting ColumnInfo has `data_type: Float64`

#### Scenario: Comparison type
- **WHEN** `expr_to_column_info` is called for `PlanExpr::BinaryOp { Column(a), Gt, Literal(1) }`
- **THEN** the resulting ColumnInfo has `data_type: Boolean`

### Requirement: Type inference for aggregate Function expressions
The planner SHALL infer the output data type of `PlanExpr::Function` for known aggregate functions:
- COUNT SHALL return Int64
- SUM/AVG SHALL return the argument type widened to Float64 for floats, Int64 for integers, Float64 as fallback
- MIN/MAX SHALL return the argument type
- The column name SHALL use the full display string (e.g., "SUM(age)") to avoid ambiguity with multiple aggregates

#### Scenario: SUM return type
- **WHEN** `expr_to_column_info` is called for `PlanExpr::Function { name: "SUM", args: [Column(age: Int32)] }`
- **THEN** the resulting ColumnInfo has `data_type: Int64` and `name: "SUM(age)"`
