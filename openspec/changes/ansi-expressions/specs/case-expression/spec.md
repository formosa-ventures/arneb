# case-expression

**Status**: ADDED
**Crate**: execution

## Overview

CASE WHEN/THEN/ELSE conditional expression evaluation within the execution engine's expression evaluator. Supports both searched CASE (conditions are arbitrary boolean expressions) and simple CASE (operand compared to each WHEN value).

## ADDED Requirements

### Requirement: Searched CASE evaluation
The expression evaluator SHALL evaluate searched CASE expressions by iterating through when_clauses in order, evaluating each condition against the input RecordBatch, and returning the result of the first matching condition. If no condition matches and an ELSE is present, the ELSE result is returned. If no condition matches and no ELSE is present, NULL is returned.

#### Scenario: First matching condition wins
- **WHEN** `CASE WHEN a > 10 THEN 'high' WHEN a > 5 THEN 'mid' ELSE 'low' END` is evaluated where a is `[3, 7, 12]`
- **THEN** it returns `["low", "mid", "high"]`

#### Scenario: No ELSE clause returns NULL
- **WHEN** `CASE WHEN a > 10 THEN 'high' END` is evaluated where a is `[3, 7, 12]`
- **THEN** it returns `[NULL, NULL, "high"]`

#### Scenario: NULL condition is not matched
- **WHEN** a CASE condition evaluates to NULL for a given row
- **THEN** that condition is treated as not matched and the next condition is tried

### Requirement: Simple CASE evaluation
The expression evaluator SHALL evaluate simple CASE expressions (`CASE operand WHEN val1 THEN res1 ...`) by comparing the operand to each WHEN value using equality. The first matching value determines the result.

#### Scenario: Simple CASE with matching value
- **WHEN** `CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END` is evaluated where status is `[1, 2, 3]`
- **THEN** it returns `["active", "inactive", "unknown"]`

#### Scenario: Simple CASE with NULL operand
- **WHEN** `CASE status WHEN 1 THEN 'active' ELSE 'unknown' END` is evaluated where status is `[1, NULL, 2]`
- **THEN** it returns `["active", "unknown", "unknown"]` because NULL = 1 is NULL (not true)

### Requirement: CASE result type consistency
The expression evaluator SHALL ensure all THEN/ELSE branches produce compatible types. If types differ, the evaluator SHALL attempt to coerce to a common type.

#### Scenario: Mixed numeric result types
- **WHEN** a CASE expression has an Int32 THEN branch and a Float64 ELSE branch
- **THEN** both results are coerced to Float64

#### Scenario: Incompatible result types
- **WHEN** a CASE expression has a Utf8 THEN branch and an Int64 ELSE branch with no valid coercion
- **THEN** the evaluator returns an execution error

### Requirement: Nested CASE expressions
The evaluator SHALL support CASE expressions nested within CASE conditions or results.

#### Scenario: CASE inside CASE
- **WHEN** `CASE WHEN a > 0 THEN CASE WHEN a > 10 THEN 'high' ELSE 'low' END ELSE 'zero' END` is evaluated where a is `[-1, 5, 15]`
- **THEN** it returns `["zero", "low", "high"]`
