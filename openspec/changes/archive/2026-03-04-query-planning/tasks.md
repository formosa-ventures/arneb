## 1. Crate Setup

- [x] 1.1 Add `crates/planner` to workspace members in root `Cargo.toml`
- [x] 1.2 Create `crates/planner/Cargo.toml` with package name `trino-planner`, dependencies: `trino-common`, `trino-sql-parser`, `trino-catalog`
- [x] 1.3 Create `crates/planner/src/lib.rs` with module declarations and re-exports

## 2. Plan Types (`plan` module)

- [x] 2.1 Define `PlanExpr` enum with variants: Column, Literal, BinaryOp, UnaryOp, Function, IsNull, IsNotNull, Between, InList, Cast, Wildcard
- [x] 2.2 Define `SortExpr` struct with `expr: PlanExpr`, `asc: bool`, `nulls_first: bool`
- [x] 2.3 Define `LogicalPlan` enum with variants: TableScan, Projection, Filter, Join, Aggregate, Sort, Limit, Explain
- [x] 2.4 Implement `schema()` method on `LogicalPlan` returning `Vec<ColumnInfo>`
- [x] 2.5 Implement `Display` for `PlanExpr` and `LogicalPlan` (human-readable plan tree)

## 3. Query Planner (`planner` module)

- [x] 3.1 Define `QueryPlanner` struct holding a reference to `CatalogManager`
- [x] 3.2 Implement `plan_statement` dispatching `Statement::Query` and `Statement::Explain`
- [x] 3.3 Implement `plan_query` handling Query body, ORDER BY, LIMIT, OFFSET
- [x] 3.4 Implement `plan_select` building the plan from FROM → Filter → Aggregate → Projection
- [x] 3.5 Implement FROM clause planning: resolve tables via catalog, build join tree for multiple tables
- [x] 3.6 Implement expression conversion: AST `Expr` → `PlanExpr` with column resolution against input schema
- [x] 3.7 Implement wildcard expansion for `SELECT *` and `SELECT t.*`
- [x] 3.8 Implement projection building for named expressions and aliases

## 4. Tests

- [x] 4.1 Unit tests for `PlanExpr` Display
- [x] 4.2 Unit tests for `LogicalPlan` Display (plan tree formatting)
- [x] 4.3 Unit tests: simple SELECT (e.g., `SELECT a FROM t`)
- [x] 4.4 Unit tests: SELECT with WHERE filter
- [x] 4.5 Unit tests: SELECT * wildcard expansion
- [x] 4.6 Unit tests: SELECT with JOIN
- [x] 4.7 Unit tests: SELECT with GROUP BY and aggregate functions
- [x] 4.8 Unit tests: SELECT with ORDER BY, LIMIT, OFFSET
- [x] 4.9 Unit tests: EXPLAIN
- [x] 4.10 Unit tests: error cases — table not found, column not found
- [x] 4.11 Unit tests: aliases, qualified column references, expression in projection

## 5. Integration & Quality

- [x] 5.1 Verify `cargo build` compiles without warnings
- [x] 5.2 Verify `cargo test -p trino-planner` passes all tests
- [x] 5.3 Run `cargo clippy -- -D warnings` and fix any lints
- [x] 5.4 Run `cargo fmt -- --check` and ensure formatting is correct
