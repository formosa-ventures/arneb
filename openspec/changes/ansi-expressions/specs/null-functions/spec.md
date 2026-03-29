# null-functions

**Status**: ADDED
**Crate**: sql-parser

## Overview

COALESCE and NULLIF expression support via desugaring to CASE WHEN expressions during AST conversion. These are standard ANSI SQL null-handling functions.

## ADDED Requirements

### Requirement: COALESCE desugaring
The AST converter SHALL transform `COALESCE(expr1, expr2, ..., exprN)` into a searched CASE expression equivalent to `CASE WHEN expr1 IS NOT NULL THEN expr1 WHEN expr2 IS NOT NULL THEN expr2 ... ELSE exprN END`.

#### Scenario: COALESCE with two arguments
- **WHEN** `COALESCE(a, b)` is parsed and converted
- **THEN** the resulting AST is equivalent to `CASE WHEN a IS NOT NULL THEN a ELSE b END`

#### Scenario: COALESCE with three arguments
- **WHEN** `COALESCE(a, b, c)` is parsed and converted
- **THEN** the resulting AST is equivalent to `CASE WHEN a IS NOT NULL THEN a WHEN b IS NOT NULL THEN b ELSE c END`

#### Scenario: COALESCE with single argument
- **WHEN** `COALESCE(a)` is parsed and converted
- **THEN** the resulting AST is equivalent to `a` (identity)

#### Scenario: COALESCE evaluation with NULLs
- **WHEN** `COALESCE(a, b, 0)` is evaluated where a is `[NULL, 2, NULL]` and b is `[NULL, NULL, 3]`
- **THEN** it returns `[0, 2, 3]`

### Requirement: NULLIF desugaring
The AST converter SHALL transform `NULLIF(expr1, expr2)` into a searched CASE expression equivalent to `CASE WHEN expr1 = expr2 THEN NULL ELSE expr1 END`.

#### Scenario: NULLIF with matching values
- **WHEN** `NULLIF(a, 0)` is evaluated where a is `[0, 1, 0, 2]`
- **THEN** it returns `[NULL, 1, NULL, 2]`

#### Scenario: NULLIF with no matching values
- **WHEN** `NULLIF(a, 99)` is evaluated where a is `[1, 2, 3]`
- **THEN** it returns `[1, 2, 3]`

#### Scenario: NULLIF with NULL first argument
- **WHEN** `NULLIF(a, 0)` is evaluated where a is `[NULL, 1]`
- **THEN** it returns `[NULL, 1]` because NULL = 0 is NULL (not true), so ELSE branch returns NULL (the original value)
