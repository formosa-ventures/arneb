# Tasks — ast-source-spans

## Infrastructure (crates/sql-parser)

- [x] 1. Re-export `sqlparser::tokenizer::{Span, Location}` from `crates/sql-parser/src/lib.rs` for downstream crates.
- [x] 2. Add `span: Span` field to every variant of `ast::Expr`. Update all call sites and pattern matches (expect noisy diffs: `span: _` on existing matches).
- [x] 3. Add `span: Span` to `ast::Statement` variants.
- [x] 4. Add `span: Span` to `ast::ColumnRef`.
- [x] 5. Update `crates/sql-parser/src/convert.rs` to pull spans from sqlparser's `Spanned::span()` at every conversion site. Dropping a span SHALL be a compile error (via a `#[must_use]` wrapper or reviewer checklist, not a type-level guarantee — pragmatic).
- [x] 6. Add test helper `Expr::test_at(variant, span)` or `Span::empty()` constants for fixture construction outside parser code.
- [x] 7. Unit test: parse representative expressions (column, literal, binary op, function call, CASE, CAST, typed-string DATE literal) and assert spans cover the expected source range.

## PlanExpr plumbing (crates/planner)

- [x] 8. Add `span: Option<Span>` to every variant of `PlanExpr`.
- [x] 9. Update `QueryPlanner::plan_expr` to populate `span: Some(ast_expr.span)` from the AST input.
- [x] 10. Add `PlanExpr::span(&self) -> Option<Span>` accessor and `PlanExpr::best_span(&self) -> Option<Span>` fallback walker.
- [x] 11. Any existing test constructing `PlanExpr` directly (unit tests): pass `span: None`.
- [x] 12. Mark `span` field `#[serde(skip)]` on `PlanExpr` so EXPLAIN output is position-independent.

## Error types (crates/common)

- [x] 13. Add `location: Option<Location>` field to `PlanError` variants that reference source constructs: `TypeMismatch`, `ColumnNotFound`, `FunctionNotFound`, `UnsupportedExpression`, `AmbiguousReference`. Variants that do not (e.g., `InternalError`) are left unchanged.
- [x] 14. Mirror for `ParseError` — variants gain `location: Option<Location>` where applicable.
- [x] 15. Implement `PlanError::location(&self) -> Option<Location>` accessor.
- [x] 16. Update all call sites that construct these errors to pass a location where known; use `None` elsewhere.

## Diagnostic rendering (crates/common)

- [x] 17. Add `codespan-reporting = "0.11"` (or latest compatible) to workspace dependencies. Verify license is Apache-2.0/MIT (on allowed list in `deny.toml`).
- [x] 18. Create `crates/common/src/diagnostic.rs` with `SourceFile { name, text }` and `render_plan_error(&PlanError, &SourceFile) -> String` using `codespan-reporting::Diagnostic`.
- [x] 19. Unit test: given a fixed `SourceFile` and each error variant, assert the rendered output contains expected substrings (file:line:col, source line, caret).
- [x] 20. Document in rustdoc when to call `render_plan_error` vs. relying on `Display`.

## Protocol integration (crates/protocol)

- [x] 21. Capture the original query string in the pgwire handler (`handler.rs`) and build a `SourceFile { name: "<query>", text: sql }` before invoking planning.
- [x] 22. In the pgwire error path, call `render_plan_error(&err, &source)` and use its output as the error message body. Preserve existing SQLSTATE codes (no wire-protocol change).
- [x] 23. Integration test: submit a failing query via pgwire and assert the error message contains `line X:Y`.

## Cross-cutting

- [x] 24. Update existing integration tests whose assertions on error text break under the new prefix format. (No assertions broke — rendered output is only used when the pgwire path calls `render_plan_error` with a `SourceFile`; existing tests that read `err.to_string()` see the unchanged position-free message.)
- [x] 25. Update `crates/sql-parser/src/lib.rs` module docs to note that AST nodes carry span information.
- [x] 26. Confirm `EXPLAIN` / `EXPLAIN FORMAT JSON` tests still produce byte-identical output for equivalent queries with different whitespace (regression guard for D7 decision). Added `explain_is_position_independent` test asserting both `to_string()` and `serde_json` outputs are equal across whitespace variants.

## Validation

- [x] 27. `cargo test --workspace` passes.
- [x] 28. `cargo clippy --workspace --all-targets -- -D warnings` passes.
- [x] 29. Manual: submit a typo-column query via psql, confirm the error now includes a caret pointing at the typo. Verified against local SF001: `SELECT lshipdate FROM lineitem LIMIT 1;` returned the rustc-style diagnostic with `┌─ <query>:1:8` and a `^` caret pointing at the misspelled column.
- [ ] 30. Publish update to `openspec/specs/` merging the `error-diagnostics` capability. (Happens at the archive step via `openspec archive ast-source-spans`.)
