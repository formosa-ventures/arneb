## 1. AST — Subquery Expressions

- [x] 1.1 Add `Expr::InSubquery { expr, subquery, negated }` to expression AST; map sqlparser IN subquery nodes in convert.rs
- [x] 1.2 Add `Expr::Exists { subquery, negated }` to expression AST; map sqlparser EXISTS expressions in convert.rs
- [x] 1.3 Verify existing `Expr::Subquery(Box<Query>)` handles scalar subqueries correctly, extend if needed

## 2. AST Unit Tests

- [x] 2.1 Write tests parsing SQL with IN subquery, NOT IN subquery, EXISTS, NOT EXISTS, and scalar subqueries — verify correct AST nodes

## 3. Logical Plan Nodes

- [x] 3.1 Add `LogicalPlan::SemiJoin { left, right, on }` and `LogicalPlan::AntiJoin { left, right, on }` — output schema same as left
- [x] 3.2 Add `LogicalPlan::ScalarSubquery { subplan }` — output is a single scalar value

## 4. Logical Plan Unit Tests

- [x] 4.1 Test SemiJoin, AntiJoin, ScalarSubquery node construction and verify their output schemas

## 5. Query Planner

- [x] 5.1 Implement subquery detection pass: walk WHERE expressions, collect subquery nodes, classify as correlated or uncorrelated
- [x] 5.2 Transform uncorrelated `InSubquery` → `SemiJoin`, `NOT IN` → `AntiJoin`
- [x] 5.3 Transform `EXISTS` → `SemiJoin` (or `AntiJoin` for NOT EXISTS) with correlation predicates as join conditions
- [x] 5.4 Plan non-correlated scalar subqueries as independent subplans; correlated scalar subqueries with nested-loop strategy

## 6. Physical Operators

- [x] 6.1 Implement `SemiJoinExec`: build hash set from right join key column, probe with left, emit left rows where key exists, handle NULLs
- [x] 6.2 Implement `AntiJoinExec`: build hash set from right join key column, probe with left, emit left rows where key NOT in right
- [x] 6.3 Implement `ScalarSubqueryExec`: execute subquery plan, assert ≤1 row and 1 column, return scalar (NULL if 0 rows, error if >1 row)
- [x] 6.4 Wire SemiJoin→SemiJoinExec, AntiJoin→AntiJoinExec, ScalarSubquery→ScalarSubqueryExec in ExecutionContext

## 7. Expression Evaluator

- [x] 7.1 Extend expression evaluator for ScalarSubquery: look up pre-computed scalar value, propagate NULL for zero-row results

## 8. Integration Tests

- [x] 8.1 Test `WHERE id IN (SELECT ...)`, `WHERE NOT IN (SELECT ...)`, `WHERE EXISTS (SELECT ...)`, `WHERE NOT EXISTS (SELECT ...)`, empty subquery results
- [x] 8.2 Test scalar subquery in SELECT and WHERE, zero-row→NULL, multi-row→error, nested subqueries
