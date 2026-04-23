# error-diagnostics Specification

## Purpose
TBD - created by archiving change ast-source-spans. Update Purpose after archive.
## Requirements
### Requirement: AST expression nodes carry span

Every variant of `crates/sql-parser/src/ast.rs::Expr` SHALL include a `span: sqlparser::tokenizer::Span` field populated at AST-construction time. The `Span` MUST cover the entire source range of the expression.

#### Scenario: Parse a BinaryOp, recover span

- **WHEN** `parse_sql("SELECT a <= b FROM t")` is called
- **THEN** the resulting `Expr::BinaryOp` has a `span` whose `start.line == 1` and covers at least columns 8..=13

#### Scenario: Parse a nested expression, children carry nested spans

- **WHEN** the input is `WHERE (x + 1) * 2 <= 10`
- **THEN** the outer `BinaryOp` span covers the entire `(x + 1) * 2 <= 10` range
- **AND** the inner `BinaryOp (x + 1)` span covers only `x + 1`
- **AND** the literal `1` span covers only the `1` character

### Requirement: AST statement and column references carry span

`ast::Statement` variants and `ast::ColumnRef` SHALL each include a `span` field. Spans on `ColumnRef` MUST point at the exact identifier (including any table qualifier) in the source.

#### Scenario: Column reference span

- **WHEN** parsing `SELECT t.foo FROM t`
- **THEN** the `ColumnRef { table: Some("t"), name: "foo", span }` span starts at `t.foo`'s position and ends after `foo`

### Requirement: Spans preserved during AST conversion

`crates/sql-parser/src/convert.rs` SHALL extract spans from sqlparser-rs's `Spanned::span()` method at every conversion site and assign the resulting `Span` to the corresponding Arneb AST node. No conversion may silently drop span information.

#### Scenario: Conversion round-trip preserves span

- **WHEN** a sqlparser `Expr::TypedString { data_type: Date, value: "1998-12-01" }` with `span: 3:19..3:35` is lowered to Arneb's `Expr::Cast { span, ... }`
- **THEN** the Arneb `Cast`'s outer span equals `3:19..3:35`

### Requirement: PlanExpr carries optional span

Every variant of `crates/planner/src/plan.rs::PlanExpr` SHALL carry a `span: Option<sqlparser::tokenizer::Span>`. Values planned from a user-visible AST node MUST have `Some(span)` matching the AST source. Values synthesized by analyzer or optimizer passes (inserted `Cast`, rewritten `BinaryOp`, etc.) MUST use `None`.

#### Scenario: Planned expression retains source span

- **WHEN** `QueryPlanner::plan_expr(ast::Expr::Column { span: S, .. })` runs
- **THEN** the resulting `PlanExpr::Column { span: Some(S), .. }`

#### Scenario: Synthetic Cast has no span

- **WHEN** a type-coercion rewrite wraps `PlanExpr::Column(span=Some(S))` in `PlanExpr::Cast`
- **THEN** the `Cast` node has `span: None`
- **AND** the inner `Column` keeps `span: Some(S)`

### Requirement: PlanExpr exposes best-span accessor

`PlanExpr` SHALL provide a `best_span(&self) -> Option<Span>` that returns the node's own span if present, or the first descendant's span otherwise. This lets error reporters point at the nearest user-visible construct even when the erroring node is synthetic.

#### Scenario: best_span falls back to descendant

- **WHEN** `Cast { span: None, expr: Literal(span=Some(S)) }.best_span()` is called
- **THEN** it returns `Some(S)`

### Requirement: PlanError carries optional location

Every `PlanError` variant that references a specific source construct SHALL include a `location: Option<sqlparser::tokenizer::Location>` field. A `location()` accessor method on `PlanError` SHALL return the variant's location regardless of which variant is matched.

#### Scenario: TypeMismatch carries location

- **WHEN** type coercion fails for `l_shipdate <= DATE '1998-12-01'` at line 3, column 19
- **THEN** the resulting `PlanError::TypeMismatch { location: Some(Location { line: 3, column: 19 }), .. }`

### Requirement: Rustc-style diagnostic rendering

Arneb SHALL integrate `codespan-reporting` and provide a `render_plan_error(&PlanError, &SourceFile) -> String` function that emits output matching the rustc/clang diagnostic style: header line, file/line/col marker, source excerpt, and caret-underlined span.

#### Scenario: Full diagnostic for type mismatch

- **GIVEN** a `SourceFile { name: "query.sql", text: "SELECT * FROM t\nWHERE l_shipdate <= DATE '1998-12-01'" }`
- **AND** a `PlanError::TypeMismatch { op: "<=", left_type: Utf8, right_type: Date32, location: Some(Location { line: 2, column: 19 }) }`
- **WHEN** `render_plan_error(&err, &source)` is called
- **THEN** the output contains the literal text `query.sql:2:19`
- **AND** contains a line with `WHERE l_shipdate <= DATE '1998-12-01'`
- **AND** contains carets (`^`) aligned under the `<=` token

#### Scenario: Fallback when source is unavailable

- **WHEN** `PlanError::TypeMismatch { location: Some(Location { line: 2, column: 19 }), .. }.to_string()` is called (Display impl, no source available)
- **THEN** the output is `cannot apply operator '<=' to Utf8 and Date32` (the position-free `thiserror` template — prefix formatting is the renderer's job, not `Display`'s)

### Requirement: pgwire error responses use diagnostic renderer

The pgwire error path (`crates/protocol/src/handler.rs`) SHALL invoke `render_plan_error` with the active `SourceFile` (captured from the submitted query string) before sending error responses, so PostgreSQL clients receive the full rustc-style diagnostic text in the error message body.

#### Scenario: psql sees rich error

- **GIVEN** arneb is running with a `FilterExec` that triggers a `TypeMismatch`
- **WHEN** a psql client submits the failing query
- **THEN** the `ERROR:` response body contains `line X:Y:` followed by the diagnostic block (exact formatting subject to pgwire wire framing)

### Requirement: EXPLAIN output omits spans

Any `EXPLAIN` (including `EXPLAIN (FORMAT JSON)`) output SHALL NOT include span information. Plan serialization MUST skip `span` fields so EXPLAIN output remains stable across source positions.

#### Scenario: EXPLAIN is position-independent

- **WHEN** `EXPLAIN SELECT a FROM t WHERE a > 1` is run
- **AND** the same query is run with different whitespace `EXPLAIN SELECT a  FROM  t  WHERE  a > 1`
- **THEN** the two EXPLAIN outputs are textually identical

