## 1. AST — CTEs

- [x] 1.1 Add CTE definition struct with name, optional column aliases, and subquery AST; extend Query to include optional `Vec<CTEDefinition>`
- [x] 1.2 Map sqlparser WITH clause to internal CTE structures in convert.rs

## 2. AST — Set Operations

- [x] 2.1 Add `SetOperation { op, left, right }` to query AST with `SetOperator` enum: `UnionAll`, `Union`, `Intersect`, `Except`
- [x] 2.2 Map sqlparser set expression nodes to internal SetOperation in convert.rs

## 3. AST — Window Functions

- [x] 3.1 Add `Expr::WindowFunction { name, args, partition_by, order_by }` to expression AST
- [x] 3.2 Map sqlparser window function expressions to this node in convert.rs

## 4. AST Unit Tests

- [x] 4.1 Test parsing `WITH a AS (SELECT 1) SELECT * FROM a`, `SELECT 1 UNION ALL SELECT 2`, `ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)` — verify all new AST nodes

## 5. Logical Plan — CTEs

- [x] 5.1 Add `LogicalPlan::CTE { name, plan }` and `LogicalPlan::CTERef { name }` with schema propagation from CTE plan

## 6. Logical Plan — Set Operations

- [x] 6.1 Add `LogicalPlan::UnionAll { inputs }`, `Intersect { left, right }`, `Except { left, right }`, `Distinct { input }` with schema compatibility validation

## 7. Logical Plan — Window

- [x] 7.1 Add `LogicalPlan::Window { input, functions }` where each function specifies name, args, partition keys, order keys; output schema = input + window result columns

## 8. Planner — CTEs

- [x] 8.1 Register CTE definitions in a scoped registry during planning; resolve CTE name references in FROM clauses with priority over catalog tables

## 9. Planner — Set Operations

- [x] 9.1 Transform UNION ALL → `UnionAll`, UNION → `UnionAll` + `Distinct`, INTERSECT → `Intersect`, EXCEPT → `Except`; validate column count and type compatibility

## 10. Planner — Window Functions

- [x] 10.1 Detect window function expressions in SELECT list, extract partition-by/order-by specs, add `Window` plan node above input plan

## 11. Planner Unit Tests

- [x] 11.1 Test CTE resolution and priority over catalog, set operation schema validation (match and mismatch), window function plan node creation

## 12. Physical Operators — UnionAll and Distinct

- [x] 12.1 Implement `UnionAllExec`: accept multiple children, concatenate batches in order, verify schema consistency
- [x] 12.2 Implement `DistinctExec`: build hash set of row values, emit each unique row once

## 13. Physical Operators — Intersect and Except

- [x] 13.1 Implement `IntersectExec`: build hash set from right, probe with left, emit rows in both, deduplicate
- [x] 13.2 Implement `ExceptExec`: build hash set from right, probe with left, emit rows NOT in right, deduplicate

## 14. Physical Operator — Window

- [x] 14.1 Implement `WindowExec`: sort by partition+order keys, iterate partitions, compute ROW_NUMBER/RANK/DENSE_RANK for ranking and running SUM/AVG/COUNT/MIN/MAX for aggregates, append result columns

## 15. Execution Wiring

- [x] 15.1 Wire all new operators in ExecutionContext: UnionAll→UnionAllExec, Distinct→DistinctExec, Intersect→IntersectExec, Except→ExceptExec, Window→WindowExec
- [x] 15.2 Implement CTE materialization: execute on first access, cache RecordBatches for subsequent references

## 16. Integration Tests — CTEs

- [x] 16.1 Test single CTE referenced once, referenced multiple times (self-join), multiple CTEs with inter-references, CTE with column aliases, CTE shadowing catalog table

## 17. Integration Tests — Set Operations

- [x] 17.1 Test UNION ALL preserves duplicates, UNION removes duplicates, INTERSECT returns common rows, EXCEPT returns difference, chained set operations, schema mismatch error

## 18. Integration Tests — Window Functions

- [x] 18.1 Test ROW_NUMBER with PARTITION BY + ORDER BY, RANK/DENSE_RANK with ties, SUM OVER partition, SUM OVER with ORDER BY (running total), multiple window functions in one query
