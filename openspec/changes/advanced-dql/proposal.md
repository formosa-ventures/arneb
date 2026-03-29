# Proposal: Advanced DQL (CTEs, Set Operations, Window Functions)

## Why

Common Table Expressions (CTEs) and set operations are required for TPC-H queries Q13 and Q15, and are widely used in real-world analytical SQL. Without these capabilities, users cannot write modular queries using WITH clauses, combine result sets using UNION/INTERSECT/EXCEPT, or use window functions for ranking and running aggregates.

## What

Add support for the following SQL constructs:

- **WITH clause (CTEs)**: Named subqueries that can be referenced multiple times within a single query. Supports non-recursive CTEs.
- **UNION ALL**: Concatenates result sets from two or more SELECT statements without deduplication.
- **UNION**: Concatenates result sets and removes duplicate rows.
- **INTERSECT**: Returns rows that appear in both result sets.
- **EXCEPT**: Returns rows from the first result set that do not appear in the second.
- **Window functions (stretch goal)**: ROW_NUMBER(), RANK(), DENSE_RANK(), SUM() OVER, AVG() OVER with PARTITION BY and ORDER BY clauses.

## New Capabilities

- `cte-support` — Parse, plan, and execute WITH clause queries.
- `set-operations` — Parse, plan, and execute UNION ALL, UNION, INTERSECT, EXCEPT.
- `window-functions` — Parse, plan, and execute window function expressions.

## Modified Capabilities

- `sql-ast` — Extended to handle WITH clauses and set operation nodes during AST conversion.
- `query-planner` — Extended to plan CTEs as materialized inline views and set operations as new plan nodes.
- `execution-operators` — Extended with UnionAllExec, UnionExec, IntersectExec, ExceptExec, and WindowExec operators.

## Success Criteria

- TPC-H Q13 (using LEFT JOIN + GROUP BY, benefits from CTE) and Q15 (using CTE for revenue view) execute correctly.
- CTEs can be referenced multiple times in a single query without re-execution.
- UNION ALL preserves all rows including duplicates.
- UNION removes exact duplicate rows.
- Window functions compute correct results with PARTITION BY and ORDER BY.
