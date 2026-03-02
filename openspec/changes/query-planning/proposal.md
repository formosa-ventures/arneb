## Why

After parsing SQL into an AST, the next step is converting it into a logical query plan that represents the relational algebra operations to execute. The planner bridges SQL parsing and query execution — it resolves table references via the catalog, validates column references, and produces a tree of logical operators (Scan, Filter, Projection, Join, etc.) that downstream optimization and execution phases consume.

## What Changes

- Create `crates/planner/` crate (package name: `trino-planner`)
- Define `LogicalPlan` enum representing relational algebra operators
- Define `PlanExpr` enum for expressions within logical plans
- Implement `QueryPlanner` that converts a parsed AST `Statement` into a `LogicalPlan`, using the `CatalogManager` to resolve tables and validate columns
- Support MVP SQL: SELECT (with projection, filter, join, group by, order by, limit/offset), EXPLAIN

## Capabilities

### New Capabilities

- `logical-plan`: Logical plan node types (`LogicalPlan` enum) representing relational operations: TableScan, Projection, Filter, Join, Aggregate, Sort, Limit, Explain. Each node carries its output schema.
- `plan-expr`: Expression types for logical plans (`PlanExpr` enum) — column references (by index), literals, binary/unary operations, function calls, CAST, IS NULL, BETWEEN, IN. Separate from AST expressions to support resolved column references.
- `query-planner`: The `QueryPlanner` struct that converts AST → LogicalPlan. Resolves table references via CatalogManager, validates column existence, expands wildcards, and builds the operator tree.

### Modified Capabilities

(No existing capabilities modified)

## Impact

- **New crate**: `crates/planner/`
- **Dependencies**: `trino-common`, `trino-sql-parser`, `trino-catalog`
- **Downstream**: The `optimizer` crate will transform LogicalPlan trees; the `execution` crate will convert them to physical plans
