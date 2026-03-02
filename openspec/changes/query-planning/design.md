## Context

trino-alt has `common` (shared types), `sql-parser` (SQL → AST), and `catalog` (table metadata resolution). The planner converts the AST into a `LogicalPlan` tree — a relational algebra representation that the optimizer and execution engine consume.

Project conventions: `Arc<dyn Trait>` for polymorphism, `thiserror` for errors, trait-based extensibility.

## Goals / Non-Goals

**Goals:**

- Convert parsed AST statements into a LogicalPlan tree
- Resolve table references via CatalogManager
- Validate column references against table schemas
- Expand wildcards (`SELECT *`, `SELECT t.*`) using catalog metadata
- Support all MVP SQL constructs: SELECT, FROM, JOIN, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, EXPLAIN
- Comprehensive unit tests

**Non-Goals:**

- No query optimization (separate optimizer crate)
- No physical plan generation (separate execution crate)
- No type inference or type coercion (MVP assumes compatible types)
- No correlated subqueries (only simple FROM subqueries and IN subqueries)
- No common table expressions (CTEs / WITH)

## Decisions

### D1: Expression types — Reuse AST Expr vs Separate PlanExpr

**Choice**: Separate `PlanExpr` enum distinct from `ast::Expr`. Column references use `index: usize` (position in input schema) plus `name: String` for display, rather than string-based names.

**Rationale**: Resolved column references (by index) are fundamentally different from unresolved AST column references (by name). The optimizer and executor need index-based access. Keeping them separate makes the resolved/unresolved boundary explicit.

**Alternative**: Reuse `ast::Expr` throughout. Rejected because it forces downstream consumers to re-resolve column names every time.

### D2: Schema propagation — Stored vs Computed

**Choice**: Store the output schema (`Vec<ColumnInfo>`) on plan nodes that transform columns (Projection, Aggregate, TableScan). Other nodes (Filter, Sort, Limit) inherit from their input.

**Rationale**: Avoids expensive recomputation during optimization. The schema is determined at planning time and carried through. Nodes like Filter don't change the schema, so they delegate to their input.

### D3: Module structure

**Choice**: Three modules in the planner crate:
- `plan.rs`: `LogicalPlan`, `PlanExpr`, `SortExpr` types
- `planner.rs`: `QueryPlanner` implementation
- `lib.rs`: module declarations, re-exports

**Rationale**: Separates data types from planning logic. The plan types are consumed by optimizer and executor; the planner logic is only used at the planning stage.

### D4: Wildcard expansion strategy

**Choice**: Expand `SELECT *` and `SELECT t.*` at planning time into explicit column references. The resulting Projection always has concrete column lists.

**Rationale**: Downstream consumers (optimizer, executor) never need to handle wildcards. Resolution happens once at planning time using catalog metadata.

### D5: Join planning — Implicit cross joins for multi-table FROM

**Choice**: When the FROM clause has multiple comma-separated tables (e.g., `FROM a, b`), treat them as cross joins. Explicit JOIN syntax produces the corresponding join type.

**Rationale**: Standard SQL semantics. The comma-separated form is equivalent to CROSS JOIN. This keeps the join tree builder simple.

## Risks / Trade-offs

**[No type checking]** → The planner does not verify type compatibility for operations (e.g., `1 + 'hello'`). **Mitigation**: Acceptable for MVP. Type checking can be added as a separate validation pass later.

**[Column ambiguity]** → Unqualified column references in JOINs may be ambiguous. **Mitigation**: For MVP, resolve against all available tables; return `ColumnNotFound` if not found in any. Ambiguity detection can be added later.

**[Index-based columns are fragile]** → Column indices must be carefully maintained through plan transformations. **Mitigation**: The optimizer must preserve index consistency. Unit tests verify schema propagation at each node.
