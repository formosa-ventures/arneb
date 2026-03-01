## 1. Crate Setup

- [x] 1.1 Add `crates/sql-parser` to workspace members in root `Cargo.toml`
- [x] 1.2 Create `crates/sql-parser/Cargo.toml` with package name `trino-sql-parser`, dependencies: `trino-common` (path), `sqlparser` (with sqlparser-rs)
- [x] 1.3 Create `crates/sql-parser/src/lib.rs` with module declarations (`pub mod ast;`, `pub mod convert;`), re-exports, and top-level `parse()` function

## 2. AST Types (`ast` module)

- [x] 2.1 Define `Statement` enum: `Query(Box<Query>)`, `Explain(Box<Statement>)`
- [x] 2.2 Define `Query` struct: `body: SelectBody`, `order_by: Vec<OrderByExpr>`, `limit: Option<Expr>`, `offset: Option<Expr>`
- [x] 2.3 Define `SelectBody` struct: `projection: Vec<SelectItem>`, `from: Vec<TableWithJoins>`, `selection: Option<Expr>`, `group_by: Vec<Expr>`, `having: Option<Expr>`, `distinct: bool`
- [x] 2.4 Define `SelectItem` enum: `UnnamedExpr(Expr)`, `ExprWithAlias { expr, alias }`, `Wildcard`, `QualifiedWildcard(TableReference)`
- [x] 2.5 Define `Expr` enum with all MVP variants: `Column`, `Literal`, `BinaryOp`, `UnaryOp`, `Function`, `IsNull`, `IsNotNull`, `Between`, `InList`, `Cast`, `Subquery`, `Nested`
- [x] 2.6 Define `BinaryOp` enum: arithmetic (`Plus`, `Minus`, `Multiply`, `Divide`, `Modulo`), comparison (`Eq`, `NotEq`, `Lt`, `LtEq`, `Gt`, `GtEq`), logical (`And`, `Or`), string (`Like`, `NotLike`)
- [x] 2.7 Define `UnaryOp` enum: `Not`, `Minus`, `Plus`
- [x] 2.8 Define `TableWithJoins` struct, `TableFactor` enum (`Table`, `Subquery`), `Join` struct, `JoinType` enum, `JoinCondition` enum
- [x] 2.9 Define `OrderByExpr` struct with `expr: Expr`, `asc: Option<bool>`, `nulls_first: Option<bool>`
- [x] 2.10 Define `ColumnRef` struct with `name: String`, `table: Option<String>` for column references
- [x] 2.11 Define `FunctionArg` enum for function argument handling
- [x] 2.12 Implement `Display` for key AST types (`Statement`, `Expr`, `BinaryOp`, `UnaryOp`) for debugging and error messages

## 3. Conversion Layer (`convert` module)

- [x] 3.1 Implement top-level `convert_statement(sqlparser::ast::Statement) -> Result<Statement, ParseError>` dispatching supported statements, returning `UnsupportedFeature` for others
- [x] 3.2 Implement `convert_query(sqlparser::ast::Query) -> Result<Query, ParseError>` handling body, ORDER BY, LIMIT, OFFSET
- [x] 3.3 Implement `convert_select(sqlparser::ast::Select) -> Result<SelectBody, ParseError>` handling projection, FROM, WHERE, GROUP BY, HAVING, DISTINCT
- [x] 3.4 Implement `convert_select_item(sqlparser::ast::SelectItem) -> Result<SelectItem, ParseError>`
- [x] 3.5 Implement `convert_expr(sqlparser::ast::Expr) -> Result<Expr, ParseError>` handling all supported expression types
- [x] 3.6 Implement `convert_table_with_joins` and `convert_table_factor` for FROM clause conversion
- [x] 3.7 Implement `convert_join` and `convert_join_type` for JOIN handling
- [x] 3.8 Implement `convert_value(sqlparser::ast::Value) -> Result<ScalarValue, ParseError>` for literal conversion
- [x] 3.9 Implement `convert_data_type(sqlparser::ast::DataType) -> Result<DataType, ParseError>` for CAST type conversion
- [x] 3.10 Implement `convert_binary_op` and `convert_unary_op` for operator conversion
- [x] 3.11 Implement `convert_order_by` for ORDER BY expression conversion
- [x] 3.12 Implement `convert_function` for function call conversion

## 4. Tests

- [x] 4.1 Unit tests for basic SELECT: `SELECT 1`, `SELECT a, b FROM t`, `SELECT *`, `SELECT t.*`
- [x] 4.2 Unit tests for WHERE clause: comparison operators, AND/OR, IS NULL, BETWEEN, IN
- [x] 4.3 Unit tests for JOIN: INNER, LEFT, RIGHT, CROSS, multiple joins, ON and USING conditions
- [x] 4.4 Unit tests for expressions: arithmetic, function calls, CAST, nested expressions
- [x] 4.5 Unit tests for GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
- [x] 4.6 Unit tests for subqueries: in FROM clause and in expressions
- [x] 4.7 Unit tests for EXPLAIN statement
- [x] 4.8 Unit tests for error cases: syntax errors, unsupported statements (CREATE, INSERT, etc.), unsupported expressions
- [x] 4.9 Unit tests for literal conversion: integers, floats, strings, booleans, NULL
- [x] 4.10 Unit tests for data type conversion: all supported SQL types, unsupported types

## 5. Integration & Quality

- [x] 5.1 Verify `cargo build` compiles without warnings
- [x] 5.2 Verify `cargo test -p trino-sql-parser` passes all tests
- [x] 5.3 Run `cargo clippy -- -D warnings` and fix any lints
- [x] 5.4 Run `cargo fmt -- --check` and ensure formatting is correct
