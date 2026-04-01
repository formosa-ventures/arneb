## Overview

Define arneb-specific AST types covering the SQL grammar subset needed for the MVP. These types serve as input to the planner and must clearly express SQL semantics while being easy to traverse.

## Requirements

### R1: Statement types

Supported top-level statements:
- `SELECT` queries (including subqueries)
- `EXPLAIN` for viewing query plans

Each statement must be an independently processable unit.

**Scenarios:**
- `SELECT 1` → `Statement::Query`
- `EXPLAIN SELECT * FROM t` → `Statement::Explain`
- `INSERT INTO t VALUES (1)` → Returns `ParseError::UnsupportedFeature`

### R2: Query / Select structure

`Query` must contain:
- `body`: The SELECT body (SelectBody)
- `order_by`: List of sort expressions (optional)
- `limit`: Result count limit (optional)
- `offset`: Result offset (optional)

`SelectBody` must contain:
- `projection`: List of selected columns/expressions
- `from`: List of data sources (including JOINs)
- `selection`: WHERE condition (optional)
- `group_by`: Grouping expressions (optional)
- `having`: Post-aggregation filter condition (optional)

**Scenarios:**
- `SELECT a, b FROM t WHERE x > 1 ORDER BY a LIMIT 10` → All fields populated correctly
- `SELECT *` → projection contains `SelectItem::Wildcard`
- `SELECT t.*` → projection contains `SelectItem::QualifiedWildcard`

### R3: Expression types

The `Expr` enum must support:
- Column references: `Expr::Column` (with optional table qualifier)
- Literals: `Expr::Literal(ScalarValue)`
- Binary operations: `Expr::BinaryOp { left, op, right }` (arithmetic +/-/\*/÷, comparison =/>/<, logical AND/OR)
- Unary operations: `Expr::UnaryOp { op, expr }` (NOT, negation)
- Function calls: `Expr::Function { name, args, distinct }`
- IS NULL / IS NOT NULL
- BETWEEN
- IN (list)
- CAST / type conversion
- Subqueries: `Expr::Subquery`
- Aliases: Handled via `SelectItem::ExprWithAlias`

**Scenarios:**
- `a + b * 2` → Correct operator precedence (handled by sqlparser)
- `CAST(x AS INTEGER)` → `Expr::Cast { expr, data_type }`
- `x BETWEEN 1 AND 10` → `Expr::Between { expr, low, high, negated }`
- `x IN (1, 2, 3)` → `Expr::InList { expr, list, negated }`

### R4: Table and JOIN types

`TableFactor` must support:
- Named tables: `TableFactor::Table { name: TableReference, alias }`
- Subqueries: `TableFactor::Subquery { query, alias }`

`Join` must contain:
- `relation`: The right-side TableFactor
- `join_type`: INNER, LEFT, RIGHT, FULL, CROSS
- `condition`: ON condition or USING column list

**Scenarios:**
- `FROM t1 JOIN t2 ON t1.id = t2.id` → Join with INNER type
- `FROM t1 LEFT JOIN t2 USING (id)` → Join with LEFT type and USING condition
- `FROM (SELECT ...) AS sub` → TableFactor::Subquery

### R5: Use shared type definitions

- Table references use `arneb-common`'s `TableReference`
- Literal values use `arneb-common`'s `ScalarValue`
- Data type references use `arneb-common`'s `DataType`
- Errors use `arneb-common`'s `ParseError`

Ensure consistency between the AST and common crate types; avoid duplicate definitions.
