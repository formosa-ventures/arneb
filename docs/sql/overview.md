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
- **Aggregations**: GROUP BY with HAVING, `GROUP BY ROLLUP(...)` with `GROUPING(...)`, aggregate functions (COUNT, SUM, AVG, MIN, MAX, BOOL_AND, BOOL_OR, ARRAY_AGG with ORDER BY, and the variance family STDDEV, STDDEV_SAMP, STDDEV_POP, VARIANCE, VAR_SAMP, VAR_POP)
- **Subqueries**: Scalar, IN, and EXISTS subqueries
- **Arrays**: `ARRAY[...]` literals, `'{...}'::type[]` casts, `= ANY` / `<> ALL`, `UNNEST` and `GENERATE_SUBSCRIPTS` in the SELECT list, and array parameters and results over pgwire — see [Arrays](/sql/advanced#arrays)
- **CTEs**: Common Table Expressions via `WITH` clauses
- **Set Operations**: UNION ALL, UNION, INTERSECT, EXCEPT, and `VALUES` lists
- **Window Functions**: ROW_NUMBER, RANK, DENSE_RANK, and aggregate window functions with PARTITION BY and ORDER BY, plus the `ROWS UNBOUNDED PRECEDING` frame
- **Ordering and Limiting**: ORDER BY (including on aliases and aggregates), LIMIT, OFFSET (any constant integer expression, such as `LIMIT $1::INTEGER` from a driver; not a column)

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

### Statement timeout

```sql
SET statement_timeout = '30s';      -- also 30000 (ms), '2min', '1h'; 0 = off
SHOW statement_timeout;             -- 30s
RESET statement_timeout;            -- back to the server default
```

A statement still running when the timeout expires (counting any time spent
waiting in the admission queue, as in PostgreSQL) is cancelled on every
node and fails with `ERROR 57014 canceling statement due to statement
timeout`; the connection stays usable. An invalid value is `22023`. The
server default is `[admission] statement_timeout`
([Admission Control](/guide/configuration#admission-control)).

### Cancelling a statement

A client's cancel request (psql's Ctrl-C, JDBC's `Statement.cancel`, the
stop button in Metabase or DBeaver) cancels the statement running on that
connection on every node, the same way a timeout does. It fails with
`ERROR 57014 canceling statement due to user request`, and the connection
stays usable. Each connection gets its own random cancel key at startup,
so a request only reaches the connection that issued the key. A request
that arrives while the connection is idle does nothing.

### Resource errors

| SQLSTATE | Meaning |
|---|---|
| `53200` | The query exceeded its per-node memory limit (`query_max_memory_per_node`). |
| `53000` | The admission queue is full (`max_queued_queries`). |
| `57014` | The statement timed out (`statement_timeout`). |

### Data errors

A value the query computes that its type cannot hold is a data error, with
the code PostgreSQL (pgwire) or Trino (Trino protocol) gives the same
failure. It is the same whether the planner folds the expression or a worker
evaluates it. [`TRY`](/sql/functions#try) turns any of these into `NULL`.

| SQLSTATE | Trino `errorName` (`errorCode`) | Example |
|---|---|---|
| `22003` | `NUMERIC_VALUE_OUT_OF_RANGE` (19) | `CAST(300 AS TINYINT)`, `CAST(infinity() AS INTEGER)`, an INTEGER `+` that overflows, a DECIMAL result too wide for its type |
| `22003` | `INVALID_CAST_ARGUMENT` (9) | `CAST(nan() AS INTEGER)`: PostgreSQL calls it out of range, Trino an invalid cast argument (`Cannot cast double NaN to integer`) |
| `22P02` | `INVALID_CAST_ARGUMENT` (9) | `CAST('abc' AS INTEGER)` |
| `22007` | `INVALID_CAST_ARGUMENT` (9) | `CAST('2024-13-45' AS DATE)` |
| `22012` | `DIVISION_BY_ZERO` (8) | `x / 0`, `x % 0` on integers |

## Further Reading

- [Expressions](/sql/expressions) — operators, CASE, CAST, LIKE, and more
- [Functions](/sql/functions) — all 90 built-in scalar functions (Trino-compatible)
- [Advanced](/sql/advanced) — CTEs, window functions, set operations
