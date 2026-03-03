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
