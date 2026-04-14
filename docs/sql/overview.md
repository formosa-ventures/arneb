# SQL Overview

Arneb supports a broad subset of ANSI SQL. This section documents all supported statements, expressions, and functions.

## Supported Statements

| Statement | Description |
|-----------|-------------|
| [`SELECT`](/sql/expressions) | Query data with filtering, grouping, ordering, and joins |
| `EXPLAIN` | Display the query execution plan |
| `CREATE TABLE` | Create a new table |
| `DROP TABLE` | Remove a table |
| `CREATE TABLE AS SELECT` | Create a table from a query result |
| `INSERT INTO` | Insert rows into a table |
| `DELETE FROM` | Delete rows from a table |
| `CREATE VIEW` | Create a named view |
| `DROP VIEW` | Remove a view |

## Query Capabilities

Arneb supports the following query features:

- **Joins**: INNER, LEFT, RIGHT, FULL OUTER, CROSS, and semi-joins
- **Aggregations**: GROUP BY with HAVING, aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- **Subqueries**: Scalar, IN, and EXISTS subqueries
- **CTEs**: Common Table Expressions via `WITH` clauses
- **Set Operations**: UNION ALL, UNION, INTERSECT, EXCEPT
- **Window Functions**: ROW_NUMBER, RANK, DENSE_RANK, and aggregate window functions with PARTITION BY and ORDER BY
- **Ordering and Limiting**: ORDER BY (including on aliases and aggregates), LIMIT, OFFSET

## Further Reading

- [Expressions](/sql/expressions) — operators, CASE, CAST, LIKE, and more
- [Functions](/sql/functions) — all 19 built-in scalar functions
- [Advanced](/sql/advanced) — CTEs, window functions, set operations
