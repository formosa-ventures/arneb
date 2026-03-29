# sql-ast

**Status**: MODIFIED
**Crate**: sql-parser

## Overview

Handle CASE, COALESCE, and NULLIF expressions in the AST conversion layer (convert.rs). The sqlparser-rs crate already parses these constructs; our converter must translate them to our AST types.

## MODIFIED Requirements

### Requirement: Convert CASE expression from sqlparser AST
The AST converter SHALL handle `sqlparser::ast::Expr::Case` and convert it to our `Expr::Case` variant. Both searched CASE (no operand) and simple CASE (with operand) forms SHALL be supported.

#### Scenario: Searched CASE conversion
- **WHEN** `CASE WHEN a > 1 THEN 'big' WHEN a > 0 THEN 'small' ELSE 'zero' END` is parsed by sqlparser
- **THEN** our converter produces `Expr::Case { operand: None, conditions: [a > 1, a > 0], results: ['big', 'small'], else_result: Some('zero') }`

#### Scenario: Simple CASE conversion
- **WHEN** `CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' END` is parsed by sqlparser
- **THEN** our converter produces `Expr::Case { operand: Some(x), conditions: [1, 2], results: ['one', 'two'], else_result: None }`

### Requirement: Convert COALESCE to CASE
The AST converter SHALL recognize `COALESCE(args...)` as a function call in the sqlparser AST and desugar it into our `Expr::Case` with IS NOT NULL conditions.

#### Scenario: COALESCE function conversion
- **WHEN** `COALESCE(a, b, c)` is parsed by sqlparser
- **THEN** our converter produces `Expr::Case { operand: None, conditions: [a IS NOT NULL, b IS NOT NULL], results: [a, b], else_result: Some(c) }`

### Requirement: Convert NULLIF to CASE
The AST converter SHALL recognize `NULLIF(a, b)` as a function call in the sqlparser AST and desugar it into our `Expr::Case` with equality comparison.

#### Scenario: NULLIF function conversion
- **WHEN** `NULLIF(a, b)` is parsed by sqlparser
- **THEN** our converter produces `Expr::Case { operand: None, conditions: [a = b], results: [NULL], else_result: Some(a) }`

### Requirement: Add Case variant to Expr enum
The `Expr` enum in our AST SHALL include a `Case` variant with fields: `operand` (optional), `conditions` (list of condition expressions), `results` (list of result expressions), and `else_result` (optional).

#### Scenario: Case variant structure
- **WHEN** a CASE expression is represented in our AST
- **THEN** `conditions.len() == results.len()` is always true (one result per condition)
