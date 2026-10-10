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

## Session Settings

PostgreSQL clients can set the schema used for unqualified table names, per connection:

```sql
SET search_path = tpch;             -- a schema in the default catalog
SET search_path TO datalake.tpch;   -- catalog.schema
SHOW search_path;
RESET search_path;                  -- back to the server default
```

- Like Trino, Arneb has one current schema: the first entry that names an existing schema is used. If none does — for example PostgreSQL's default `"$user", public`, or `public`, which Arneb doesn't have — the server's default schema is kept, so tools that send these keep working.
- The setting lasts for the rest of the connection, including `SET LOCAL`.
- Fully qualified names (`catalog.schema.table`) are always resolved as written.

Trino clients set the catalog and schema with `USE` instead; see [Trino Clients](/guide/trino-clients#session-statements).

## Further Reading

- [Expressions](/sql/expressions) — operators, CASE, CAST, LIKE, and more
- [Functions](/sql/functions) — all 90 built-in scalar functions (Trino-compatible)
- [Advanced](/sql/advanced) — CTEs, window functions, set operations
