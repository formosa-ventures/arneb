## ADDED Requirements

### Requirement: Parameter placeholder in PlanExpr

Arneb's `PlanExpr` SHALL support a `Parameter { index: usize, type_hint: Option<DataType>, span: Option<Span> }` variant representing extended-query-protocol placeholders (`$1`, `$2`, …) prior to binding.

#### Scenario: Parser emits Parameter for $n

- **WHEN** a query `SELECT $1 FROM t` is parsed and planned
- **THEN** the resulting `PlanExpr::Parameter { index: 1, type_hint: None, .. }` appears in the plan

### Requirement: Type inference from binary-op sibling

The `TypeCoercion` pass SHALL, when encountering `PlanExpr::Parameter { type_hint: None }` as one operand of a binary operator, unify the parameter's type with the other operand's type.

#### Scenario: $1 inferred from column

- **GIVEN** a query `SELECT * FROM lineitem WHERE l_shipdate <= $1` where `l_shipdate` is Date32
- **WHEN** the analyzer runs
- **THEN** `AnalyzerContext.param_types[1] = Date32`
- **AND** the `Parameter` node is rewritten to `Parameter { type_hint: Some(Date32), .. }`

#### Scenario: $1 inferred from literal

- **GIVEN** `WHERE $1 <= 100`
- **WHEN** the analyzer runs
- **THEN** `ctx.param_types[1] = Int32` (literal's inferred type)

### Requirement: Type inference from IN list

When a parameter appears inside `IN (list)`, the parameter's type SHALL be unified with the type of the tested expression.

#### Scenario: $1 in IN list

- **GIVEN** `WHERE col_int64 IN ($1, $2, 100)`
- **WHEN** the analyzer runs
- **THEN** `ctx.param_types[1] = Int64` and `ctx.param_types[2] = Int64`

### Requirement: Type inference from function signature

When a parameter appears as an argument to a function with a known signature, the parameter's type SHALL be unified with the signature's expected type for that argument position.

#### Scenario: $1 as ABS argument

- **GIVEN** `SELECT ABS($1) FROM t` where `ABS` is declared as accepting a numeric
- **WHEN** the analyzer runs
- **THEN** `ctx.param_types[1]` is a numeric default (Int32), since the signature is `numeric → numeric`

### Requirement: Unresolved parameter defaults to Utf8

If no context allows inference, the parameter's type SHALL default to `Utf8` (matching Trino/Postgres `unknown` → `varchar` behaviour). A debug-level tracing message SHALL log the fallback.

#### Scenario: Isolated $1

- **GIVEN** `SELECT $1` with no surrounding context
- **WHEN** the analyzer runs
- **THEN** `ctx.param_types[1] = Utf8`
- **AND** a `tracing::debug!` message notes the fallback

### Requirement: Inferred types reported to pgwire

The pgwire `ParameterDescription` message in `crates/protocol/src/handler.rs` SHALL report the inferred parameter types (mapped to PostgreSQL OIDs) from `AnalyzerContext.param_types` rather than the generic `TEXT` OID used today.

#### Scenario: Client receives specific OID

- **GIVEN** a prepared statement `WHERE l_shipdate <= $1`
- **WHEN** the `Describe` message arrives at the server
- **THEN** the `ParameterDescription` response contains the OID for `DATE` for parameter 1

### Requirement: Conflicting type inference produces plan-time error

If multiple inference sites constrain a parameter to incompatible types, the analyzer SHALL return `Err(PlanError::ParameterTypeConflict { index, conflict_types, location })`.

#### Scenario: $1 used as both Date and Int

- **GIVEN** `WHERE l_shipdate <= $1 AND l_orderkey = $1` (Date32 and Int64)
- **WHEN** the analyzer runs
- **THEN** it returns `Err(PlanError::ParameterTypeConflict { index: 1, .. })`
