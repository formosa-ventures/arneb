## Why

Arneb's error messages currently point at no location in the user's SQL. When a query fails at plan time (column not found, type mismatch, unsupported expression) the user sees only the reason:

```
ERROR: cannot coerce Utf8 and Date32 to a common type
```

By contrast Trino and modern engines surface the exact source position:

```
ERROR: line 3:19: Cannot apply operator: varchar <= date
```

The gap is architectural: `sqlparser-rs` 0.61 already tracks `Span { start, end }` on every token and exposes a `Spanned` trait for AST nodes. Arneb's own AST layer (`crates/sql-parser/src/convert.rs`) discards this span information when converting upstream AST into Arneb's simpler `ast::Expr`. Consequently `PlanExpr` and `PlanError` have no notion of source position, and error rendering is position-free.

Without spans, every future error — type coercion (upcoming `planner-type-coercion` change), function resolution, subquery analysis — will be stuck at the same quality ceiling. Plumbing spans through is a one-time infrastructure investment that every future error benefits from.

## What Changes

- Add an inline `span: Span` field to every Arneb `ast::Expr`, `ast::Statement`, and `ast::ColumnRef` variant.
- Update `crates/sql-parser/src/convert.rs` to pull spans from sqlparser's `Spanned` trait at every conversion point instead of dropping them.
- Add `span: Option<Span>` to `PlanExpr` variants (`Option<_>` to handle synthetic nodes inserted by later analyzer/optimizer passes).
- Add `location: Option<Location>` to `PlanError` and `ParseError` variants that reference a specific source position.
- Integrate `codespan-reporting` (or equivalent) to produce rustc-style diagnostic output:
  ```
  error: cannot apply operator '<=' to Utf8 and Date32
    ┌─ query.sql:3:19
    │
  3 │   WHERE l_shipdate <= DATE '1998-12-01'
    │                    ^^ here
  ```
- Thread the original SQL source string through to error rendering (pgwire/CLI paths) so the renderer can extract source snippets.
- Ship a `PlanError::location()` accessor so error-consuming code can extract position without matching every variant.

## Capabilities

### New Capabilities

- `error-diagnostics`: structured error rendering with source-position context (line/column + code snippet with carets).

### Modified Capabilities

- `sql-parse-api` (`crates/sql-parser`): AST nodes carry span information.
- `plan-expr` (`crates/planner`): expressions carry optional span information.
- `error-types` (`crates/common`): error variants carry optional location information.
- `pg-server` (`crates/protocol`): pgwire error path formats diagnostics with source context.

## Impact

- **Zero behavior change** for successful queries. No functional semantics are altered.
- **All existing errors gain `line X:Y` prefix** and — where SQL source is available — a rustc-style diagnostic block with carets.
- **Dependency added**: `codespan-reporting` (BSD-3, already common in the Rust ecosystem; license is on Arneb's allowed list per `deny.toml`).
- **Downstream changes unblock**: `planner-type-coercion` depends on this so its type-mismatch errors can surface correctly. Other future errors (unknown column, ambiguous reference, unsupported feature) benefit for free.
- **Test suites**: every integration test that asserts error message text will need updating to include the location prefix. Unit tests that assert `PlanError::variant` patterns are unaffected.
