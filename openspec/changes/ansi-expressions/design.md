## Context

sqlparser-rs already parses CASE/COALESCE/NULLIF into its AST. Our convert.rs skips these. We need to convert them to our AST, plan them into PlanExpr, and evaluate them.

The expression evaluator (crates/execution/src/expression.rs) currently handles Column, Literal, BinaryOp, UnaryOp, IsNull, IsNotNull, Between, InList, and Cast. CASE is a new evaluation path. HAVING is parsed into `SelectBody.having` but the query planner ignores it.

## Goals / Non-Goals

**Goals:**

- CASE WHEN (searched form): `CASE WHEN cond1 THEN val1 WHEN cond2 THEN val2 ELSE default END`
- CASE WHEN (simple form): `CASE expr WHEN val1 THEN res1 WHEN val2 THEN res2 ELSE default END`
- COALESCE(a, b, ...): desugared to CASE WHEN chain during AST conversion
- NULLIF(a, b): desugared to CASE WHEN a = b THEN NULL ELSE a END during AST conversion
- HAVING clause execution via Filter node after Aggregate in the query planner

**Non-Goals:**

- Pattern matching CASE (regex-based)
- DECODE function
- Complex null semantics beyond standard SQL three-valued logic
- NVL, NVL2, IFNULL (vendor-specific null functions)

## Decisions

### D1: CaseExpr in PlanExpr

**Choice**: Add a `CaseExpr` variant to `PlanExpr`:
```
CaseExpr {
    operand: Option<Box<PlanExpr>>,
    when_clauses: Vec<(PlanExpr, PlanExpr)>,
    else_result: Option<Box<PlanExpr>>,
}
```

**Rationale**: Both searched and simple CASE forms map naturally to this structure. For simple CASE (`CASE x WHEN 1 THEN 'a'`), `operand` is `Some(x)` and each when_clause condition is compared to x. For searched CASE (`CASE WHEN x > 1 THEN 'a'`), `operand` is `None` and each when_clause condition is evaluated directly.

### D2: COALESCE desugared during AST conversion

**Choice**: COALESCE(a, b, c) is converted to `CASE WHEN a IS NOT NULL THEN a WHEN b IS NOT NULL THEN b ELSE c END` in convert.rs, not as a separate PlanExpr variant.

**Rationale**: Avoids adding another variant to PlanExpr and duplicating evaluation logic. The CASE evaluator handles it naturally. Trade-off: EXPLAIN output shows expanded CASE, not original COALESCE syntax. Acceptable for MVP.

### D3: NULLIF desugared during AST conversion

**Choice**: NULLIF(a, b) is converted to `CASE WHEN a = b THEN NULL ELSE a END` in convert.rs.

**Rationale**: Same simplification benefit as D2. One evaluation path for all conditional expressions.

### D4: HAVING as Filter after Aggregate

**Choice**: When the query has a HAVING clause, the planner inserts a Filter node between the Aggregate and Projection nodes.

**Rationale**: HAVING is semantically a filter on aggregate results. Reusing the existing Filter node avoids a new operator. The HAVING expression references output columns of the Aggregate, so the Filter must come after Aggregate but before Projection.

### D5: CASE evaluation via row-wise iteration

**Choice**: CASE evaluation iterates through when_clauses in order, evaluating each condition as a BooleanArray, then uses `zip` / selection logic to build the result array row-by-row.

**Rationale**: Simple and correct. Vectorized CASE (evaluating all branches and selecting per-row) is an optimization for later. The row-wise approach correctly handles short-circuit semantics where earlier WHEN clauses take precedence.

## Risks / Trade-offs

**[Desugaring hides original syntax]** EXPLAIN shows expanded CASE instead of COALESCE/NULLIF. **Mitigation**: Acceptable for MVP. A display-level mapping could be added later.

**[Row-wise CASE evaluation]** Not fully vectorized. **Mitigation**: For MVP data sizes this is fine. Vectorized CASE (evaluate all branches, select per-row via boolean masks) is a future optimization.

**[HAVING column resolution]** HAVING expressions must reference aggregate output columns, which requires resolving column names against the Aggregate node's output schema. **Mitigation**: Reuse the existing column resolution logic from the planner, applied to the aggregate output schema.
