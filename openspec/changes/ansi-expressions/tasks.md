## 1. AST Conversion

- [x] 1.1 Add Case variant to our AST Expr type in sql-parser (`Expr::Case { operand, conditions, results, else_result }`)
- [x] 1.2 Convert sqlparser CASE (searched form) to our AST in convert.rs
- [x] 1.3 Convert sqlparser CASE (simple form) to our AST in convert.rs
- [x] 1.4 Convert COALESCE to CASE WHEN IS NOT NULL chain in convert.rs
- [x] 1.5 Convert NULLIF to CASE WHEN = THEN NULL ELSE in convert.rs
- [x] 1.6 Write parser tests for CASE, COALESCE, NULLIF

## 2. PlanExpr

- [x] 2.1 Add CaseExpr variant to PlanExpr in crates/planner/src/plan.rs
- [x] 2.2 Implement Display for CaseExpr variant
- [x] 2.3 Convert AST Case to PlanExpr CaseExpr in planner expression conversion
- [x] 2.4 Write planner tests for CASE expressions

## 3. Expression Evaluation

- [x] 3.1 Implement CASE evaluation in expression.rs (`evaluate` match arm for CaseExpr)
- [x] 3.2 Handle NULL correctly in CASE conditions (NULL condition = not matched)
- [x] 3.3 Handle simple CASE form (compare operand to each when_clause condition)
- [x] 3.4 Write execution tests for searched CASE WHEN
- [x] 3.5 Write execution tests for simple CASE WHEN
- [x] 3.6 Write execution tests for COALESCE (desugared)
- [x] 3.7 Write execution tests for NULLIF (desugared)

## 4. HAVING Clause

- [x] 4.1 Add Filter node after Aggregate when HAVING is present in query planner
- [x] 4.2 Resolve HAVING expression column references against aggregate output schema
- [x] 4.3 Write planner test for SELECT ... GROUP BY ... HAVING
- [x] 4.4 Write end-to-end test for HAVING clause execution

## 5. Quality

- [x] 5.1 `cargo build` compiles without warnings
- [x] 5.2 `cargo test` passes (all crates)
- [x] 5.3 `cargo clippy -- -D warnings` clean
- [x] 5.4 `cargo fmt -- --check` clean
