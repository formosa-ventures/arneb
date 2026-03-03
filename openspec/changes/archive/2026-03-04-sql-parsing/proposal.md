## Why

trino-alt needs to convert user-supplied SQL strings into a structured AST (Abstract Syntax Tree) before proceeding to query planning and optimization. SQL parsing is the entry point of the entire query pipeline — without it, no queries can be processed. We use `sqlparser-rs` as the foundation and build a trino-alt-specific AST representation and conversion layer on top of it.

## What Changes

- Create `crates/sql-parser/` crate (package name: `trino-sql-parser`)
- Add `sqlparser-rs` dependency for low-level SQL grammar parsing
- Define trino-alt-specific AST types (Statement, Expr, SelectItem, TableFactor, etc.)
- Implement a conversion layer from `sqlparser-rs` AST to trino-alt AST
- Provide a top-level `parse()` function as the public API
- Handle unsupported SQL constructs by returning `ParseError`

## Capabilities

### New Capabilities

- `sql-ast`: trino-alt-specific AST type definitions covering the SQL grammar subset needed for MVP (SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT, basic expressions)
- `sql-parse-api`: SQL string parsing API that converts SQL text into trino-alt AST, including error handling and unsupported syntax detection

### Modified Capabilities

(No existing capabilities modified)

## Impact

- **New crate**: `crates/sql-parser/`
- **New dependency**: `sqlparser` (sqlparser-rs)
- **Depends on common**: Uses `ParseError` as the error type
- **Downstream impact**: The `planner` crate will directly consume the AST produced by this crate
