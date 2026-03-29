# Design: Advanced DQL (CTEs, Set Operations, Window Functions)

## Overview

This change extends the query engine with CTEs, set operations, and window functions. Each feature requires changes at the AST, planner, and execution layers.

## Common Table Expressions (CTEs)

### Strategy: Materialized Inline View

A CTE defined with `WITH name AS (SELECT ...)` is treated as a materialized inline view:

1. **During planning**: The planner registers each CTE name and its logical plan in a CTE registry.
2. **When referenced**: Each reference to the CTE name in the FROM clause resolves to the registered plan.
3. **Materialization**: On first execution, the CTE subplan is executed and its results are cached as `Vec<RecordBatch>`. Subsequent references read from the cache.

```sql
WITH regional_sales AS (
    SELECT region, SUM(amount) AS total
    FROM orders GROUP BY region
)
SELECT * FROM regional_sales WHERE total > 1000
```

Planning produces:

```
CTE("regional_sales", Aggregate(Scan(orders), group=[region], agg=[SUM(amount)]))
  → Filter(CTERef("regional_sales"), total > 1000)
```

### Multiple CTEs

Multiple CTEs in a single WITH clause are planned sequentially. Later CTEs can reference earlier ones.

### Non-Recursive Only

Recursive CTEs (WITH RECURSIVE) are out of scope for this change.

## Set Operations

### UNION ALL

UNION ALL concatenates the output of two subplans. No deduplication.

**Logical plan node**: `LogicalPlan::UnionAll { inputs: Vec<LogicalPlan> }`

**Physical operator**: `UnionAllExec` iterates over each child plan in order, emitting all batches.

**Schema requirement**: All inputs MUST have the same number of columns with compatible types. Column names are taken from the first input.

### UNION (DISTINCT)

UNION is implemented as UNION ALL followed by a deduplication step.

**Implementation**: `UnionAllExec` → `DistinctExec` (hash-based deduplication on all columns).

### INTERSECT

INTERSECT returns rows that appear in both inputs.

**Implementation**: Execute both sides, build a hash set from the right side, probe with the left side, emit matches. Deduplicates by default (INTERSECT ALL is not supported initially).

### EXCEPT

EXCEPT returns rows from the left side that do not appear in the right side.

**Implementation**: Execute both sides, build a hash set from the right side, probe with the left side, emit non-matches.

## Window Functions

### Strategy: WindowExec Operator

Window functions are computed by the `WindowExec` operator, which:

1. Receives the full input as `Vec<RecordBatch>`.
2. Sorts by PARTITION BY + ORDER BY keys.
3. Iterates through partitions (groups of rows with equal PARTITION BY values).
4. Computes the window function for each row within each partition.
5. Appends the window function result as a new column.

### Supported Window Functions

- **Ranking**: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`
- **Aggregate**: `SUM(...) OVER (...)`, `AVG(...) OVER (...)`, `COUNT(...) OVER (...)`, `MIN(...) OVER (...)`, `MAX(...) OVER (...)`

### Window Specification

```sql
function_name(...) OVER (
    PARTITION BY col1, col2
    ORDER BY col3 ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
```

Frame specification (ROWS BETWEEN) is a stretch goal. Initial implementation assumes the default frame (UNBOUNDED PRECEDING to CURRENT ROW for ranking, entire partition for aggregates without ORDER BY).

## AST Changes

The AST converter is extended to handle:

- `WITH` clause: Parse CTE definitions and store them alongside the main query.
- Set operations: Parse `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT` as binary operations between SELECT statements.
- Window functions: Parse `OVER (PARTITION BY ... ORDER BY ...)` clauses attached to function call expressions.

## Data Flow

```
SQL with CTE / UNION / Window
  → Parser → AST with CTE definitions, SetOperation nodes, WindowFunction expressions
  → Planner → LogicalPlan with CTE, UnionAll, Intersect, Except, Window nodes
  → ExecutionContext → UnionAllExec, IntersectExec, ExceptExec, WindowExec
  → Execute → Vec<RecordBatch>
```
