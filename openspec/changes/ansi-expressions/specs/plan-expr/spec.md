# plan-expr

**Status**: MODIFIED
**Crate**: planner

## Overview

Add CaseExpr variant to the PlanExpr enum to represent CASE WHEN/THEN/ELSE expressions in logical plans.

## MODIFIED Requirements

### Requirement: CaseExpr variant in PlanExpr
The `PlanExpr` enum SHALL include a `CaseExpr` variant with the following structure:
- `operand: Option<Box<PlanExpr>>` -- the expression being compared in simple CASE form; None for searched CASE
- `when_clauses: Vec<(PlanExpr, PlanExpr)>` -- pairs of (condition, result) for each WHEN/THEN branch
- `else_result: Option<Box<PlanExpr>>` -- the ELSE branch; None if omitted

#### Scenario: Searched CASE representation
- **WHEN** `CASE WHEN a > 1 THEN 'yes' ELSE 'no' END` is planned
- **THEN** it is represented as `PlanExpr::CaseExpr { operand: None, when_clauses: [(a > 1, 'yes')], else_result: Some('no') }`

#### Scenario: Simple CASE representation
- **WHEN** `CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' END` is planned
- **THEN** it is represented as `PlanExpr::CaseExpr { operand: Some(x), when_clauses: [(1, 'one'), (2, 'two')], else_result: None }`

### Requirement: Display for CaseExpr
The `Display` implementation for `PlanExpr::CaseExpr` SHALL produce human-readable output for EXPLAIN plans.

#### Scenario: Displaying searched CASE
- **WHEN** a searched CaseExpr with one WHEN clause and an ELSE is displayed
- **THEN** it outputs `CASE WHEN <cond> THEN <val> ELSE <default> END`

#### Scenario: Displaying simple CASE
- **WHEN** a simple CaseExpr with operand `x` is displayed
- **THEN** it outputs `CASE x WHEN <val1> THEN <res1> ... END`
