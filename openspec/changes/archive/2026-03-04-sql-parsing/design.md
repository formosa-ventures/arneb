## Context

arneb has established the `common` crate providing shared types (`DataType`, `ScalarValue`, `TableReference`, `ParseError`, etc.). Now we need to build the SQL parsing layer that converts SQL strings into a structured AST for the planner to consume.

Project conventions: use `sqlparser-rs` for low-level parsing, `thiserror` for error handling, all intermediate data in Arrow format.

## Goals / Non-Goals

**Goals:**

- Parse SQL strings into arneb-specific AST
- Use `sqlparser-rs` to avoid reimplementing a SQL parser
- Define clean AST types that are easy for the planner to consume
- Support the MVP SQL subset: SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT
- Return clear `ParseError` for unsupported syntax
- Comprehensive unit test coverage

**Non-Goals:**

- No DDL support (CREATE/ALTER/DROP)
- No DML support (INSERT/UPDATE/DELETE)
- No SQL semantic validation (type checking is the planner's responsibility)
- No SQL rewriting or normalization
- No multi-statement support (only one statement parsed at a time)

## Decisions

### D1: Architecture — Use sqlparser AST directly vs Custom AST + conversion layer

**Choice**: Custom AST + conversion layer. Define arneb-specific AST types and convert from `sqlparser::ast`.

**Rationale**:
- sqlparser's AST contains many variants that arneb doesn't need (DDL, DML, various dialect-specific syntax), and using it directly would force the planner to handle many unreachable branches
- A custom AST can precisely express the SQL subset supported by the MVP, and unsupported syntax is rejected at conversion time
- Swapping out the underlying parser in the future won't affect downstream crates

**Alternative**: Expose `sqlparser::ast` directly. Rejected because it would make sqlparser part of the public API, coupling all downstream crates to a specific sqlparser version.

### D2: Module structure — Organization of AST and conversion logic

**Choice**: Three modules:
- `ast.rs`: All AST type definitions
- `convert.rs`: sqlparser AST → arneb AST conversion logic
- `lib.rs`: Top-level `parse()` API and module declarations

**Rationale**: Separating AST type definitions from conversion logic keeps responsibilities clear. AST types can be used independently by downstream crates.

### D3: SQL Dialect — Generic vs Custom

**Choice**: Use `GenericDialect`.

**Rationale**: The MVP only needs standard SQL syntax, which GenericDialect already covers. A custom dialect can be considered in the future if Trino-specific syntax is needed.

### D4: Expression nesting — Box vs Arc

**Choice**: Use `Box<Expr>` for recursive types.

**Rationale**: The AST is a one-shot data structure after parsing and doesn't need shared ownership. `Box` is more lightweight than `Arc` and has clearer semantics.

### D5: Operator representation — Unified enum vs Separate enums

**Choice**: Separate `BinaryOp` and `UnaryOp` enums.

**Rationale**: Binary and unary operations are fundamentally different; combining them would make type checking meaningless. Separating them allows the planner to match more precisely.

## Risks / Trade-offs

**[sqlparser version dependency]** → sqlparser's AST structure may change across major versions, requiring conversion layer updates. **Mitigation**: The conversion layer is concentrated in `convert.rs`, limiting the blast radius.

**[Incomplete AST types]** → The MVP-defined AST subset may not be sufficient to express some valid queries. **Mitigation**: Use `ParseError::UnsupportedFeature` to explicitly inform users; no silent degradation.

**[Conversion overhead]** → Converting from sqlparser AST to arneb AST incurs extra memory allocation and copying. **Mitigation**: SQL parsing is not a performance bottleneck (compared to query execution); correctness takes priority over performance.
