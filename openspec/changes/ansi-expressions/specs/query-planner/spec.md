# query-planner

**Status**: MODIFIED
**Crate**: planner

## Overview

Add HAVING clause execution by inserting a Filter node after the Aggregate node in the query planner. Convert AST Case expressions to PlanExpr CaseExpr during expression planning.

## MODIFIED Requirements

### Requirement: HAVING clause produces Filter after Aggregate
The query planner SHALL insert a `LogicalPlan::Filter` node between the Aggregate and Projection nodes when a HAVING clause is present. The HAVING expression is planned against the Aggregate node's output schema.

#### Scenario: GROUP BY with HAVING
- **WHEN** `SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status HAVING COUNT(*) > 10` is planned
- **THEN** the plan tree is `Projection(Filter(Aggregate(TableScan(orders), group_by=[status], aggs=[COUNT(*)]), COUNT(*) > 10), [status, cnt])`

#### Scenario: HAVING without GROUP BY
- **WHEN** `SELECT COUNT(*) AS cnt FROM orders HAVING COUNT(*) > 0` is planned
- **THEN** the plan tree is `Projection(Filter(Aggregate(TableScan(orders), group_by=[], aggs=[COUNT(*)]), COUNT(*) > 0), [cnt])`

#### Scenario: HAVING with column alias
- **WHEN** `SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status HAVING cnt > 10` is planned
- **THEN** the planner resolves `cnt` to the aggregate output column and produces the correct Filter

#### Scenario: No HAVING clause
- **WHEN** a query has GROUP BY but no HAVING clause
- **THEN** no additional Filter node is inserted (existing behavior unchanged)

### Requirement: Plan AST Case to PlanExpr CaseExpr
The planner's expression conversion SHALL handle `Expr::Case` from the AST and convert it to `PlanExpr::CaseExpr`, recursively planning operand, conditions, results, and else_result.

#### Scenario: Planning a CASE expression
- **WHEN** `CASE WHEN a > 1 THEN 'big' ELSE 'small' END` appears in a SELECT list
- **THEN** the planner produces a `PlanExpr::CaseExpr` with the condition and results planned against the input schema

#### Scenario: CASE in WHERE clause
- **WHEN** `SELECT * FROM t WHERE CASE WHEN a > 0 THEN true ELSE false END` is planned
- **THEN** the Filter node contains a CaseExpr that evaluates to a boolean
