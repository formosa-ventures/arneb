# expression-evaluator

**Status**: MODIFIED
**Crate**: execution

## Overview

Add CASE expression evaluation to the existing expression evaluator in crates/execution/src/expression.rs.

## MODIFIED Requirements

### Requirement: Evaluate CaseExpr variant
The `evaluate()` function SHALL handle the `PlanExpr::CaseExpr` variant by iterating through when_clauses in order, evaluating each condition as a BooleanArray, and building a result array where each row's value comes from the first matching THEN branch (or ELSE, or NULL).

#### Scenario: Searched CASE in evaluate()
- **WHEN** `evaluate(PlanExpr::CaseExpr { operand: None, when_clauses: [(cond1, val1), (cond2, val2)], else_result: Some(default) }, &batch)` is called
- **THEN** for each row, it evaluates cond1; if true, returns val1; else evaluates cond2; if true, returns val2; else returns default

#### Scenario: Simple CASE in evaluate()
- **WHEN** `evaluate(PlanExpr::CaseExpr { operand: Some(expr), when_clauses: [(val1, res1), (val2, res2)], else_result: Some(default) }, &batch)` is called
- **THEN** for each row, it evaluates expr, compares to val1; if equal, returns res1; compares to val2; if equal, returns res2; else returns default

#### Scenario: CASE with all NULL conditions
- **WHEN** all when_clause conditions evaluate to NULL or false for a given row and no ELSE is present
- **THEN** the result for that row is NULL

### Requirement: CASE result array construction
The evaluator SHALL build the result array by tracking which rows have been assigned a value via a boolean mask. Once a row is assigned, it is excluded from subsequent condition evaluations.

#### Scenario: Short-circuit semantics
- **WHEN** the first WHEN clause matches all rows
- **THEN** subsequent WHEN clauses are still evaluated (for simplicity in MVP) but their results are not used for already-matched rows
