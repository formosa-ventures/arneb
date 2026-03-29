# Proposal: Subquery Support

## Why

Subqueries are required for 9 out of 22 TPC-H benchmark queries. Without subquery support, the engine cannot execute queries that use IN, EXISTS, or scalar subqueries in WHERE clauses or SELECT lists. This is a fundamental SQL capability that blocks meaningful benchmark coverage and real-world query compatibility.

## What

Add support for the following subquery types:

- **IN subquery**: `WHERE x IN (SELECT y FROM ...)` — filters rows where a column value matches any value from a subquery result set.
- **EXISTS subquery**: `WHERE EXISTS (SELECT ... FROM ... WHERE ...)` — filters rows based on whether a correlated subquery returns any rows.
- **Scalar subquery**: `SELECT (SELECT MAX(x) FROM ...) AS max_val` — embeds a subquery that returns exactly one row and one column as a scalar value in expressions.
- **Correlated subqueries**: subqueries that reference columns from the outer query, requiring nested-loop evaluation.

## New Capabilities

- `in-subquery` — Parse, plan, and execute IN subqueries.
- `exists-subquery` — Parse, plan, and execute EXISTS subqueries.
- `scalar-subquery` — Parse, plan, and execute scalar subqueries.

## Modified Capabilities

- `query-planner` — Extended to detect subquery expressions in the AST and convert them into appropriate join or nested-loop plan nodes.
- `expression-evaluator` — Extended to evaluate scalar subquery results inline within expression evaluation.

## Success Criteria

- TPC-H queries Q2, Q4, Q17, Q18, Q20, Q21, Q22 (and others using subqueries) parse, plan, and execute correctly.
- IN subqueries with non-correlated subqueries are converted to semi-joins.
- EXISTS subqueries with correlated predicates execute correctly.
- Scalar subqueries that return more than one row produce a runtime error.
