## Why

The engine cannot evaluate conditional expressions (CASE WHEN), null-handling functions (COALESCE, NULLIF), or HAVING clauses. These are fundamental ANSI SQL features required by 10/22 TPC-H queries and virtually all real-world analytical queries.

## What Changes

- Convert CASE WHEN (simple + searched form) from sqlparser AST to our AST and PlanExpr
- Add CaseExpr variant to PlanExpr
- Implement CASE evaluation in expression evaluator (crates/execution/src/expression.rs)
- Implement COALESCE as syntactic sugar (CASE WHEN arg1 IS NOT NULL THEN arg1 WHEN arg2 IS NOT NULL THEN arg2 ...)
- Implement NULLIF as CASE WHEN a = b THEN NULL ELSE a END
- Execute HAVING by adding a Filter node after Aggregate in the planner

## Capabilities

### New Capabilities

- `case-expression`: CASE WHEN/THEN/ELSE conditional expression evaluation
- `null-functions`: COALESCE and NULLIF expression support

### Modified Capabilities

- `expression-evaluator`: Add CASE evaluation
- `plan-expr`: Add CaseExpr variant
- `sql-ast`: Handle CASE/COALESCE/NULLIF in AST conversion
- `query-planner`: HAVING becomes Filter after Aggregate

## Impact

- **Crates**: sql-parser, planner, execution
- **Unlocks**: TPC-H Q1 (full), Q7, Q8, Q9, Q12, Q13, Q14
