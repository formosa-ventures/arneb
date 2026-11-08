# Advanced SQL

## Common Table Expressions (CTEs)

CTEs provide a way to define temporary named result sets within a query using the `WITH` clause.

### Syntax

```sql
WITH cte_name AS (
    SELECT ...
)
SELECT ... FROM cte_name;
```

### Single CTE

```sql
WITH high_value_orders AS (
    SELECT customer_id, SUM(total) AS total_spent
    FROM orders
    GROUP BY customer_id
    HAVING SUM(total) > 10000
)
SELECT c.name, h.total_spent
FROM customers c
JOIN high_value_orders h ON c.id = h.customer_id;
```

### Multiple CTEs

```sql
WITH
    regional_sales AS (
        SELECT region, SUM(amount) AS total_sales
        FROM orders
        GROUP BY region
    ),
    top_regions AS (
        SELECT region
        FROM regional_sales
        WHERE total_sales > (SELECT SUM(total_sales) / 10 FROM regional_sales)
    )
SELECT region, total_sales
FROM regional_sales
WHERE region IN (SELECT region FROM top_regions);
```

## Window Functions

Window functions compute values across a set of rows related to the current row without collapsing the result set.

### Syntax

```sql
function_name() OVER (
    [PARTITION BY column1, column2, ...]
    [ORDER BY column3, column4, ...]
)
```

### Ranking Functions

#### ROW_NUMBER

Assigns a unique sequential number to each row within a partition.

```sql
SELECT
    name,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
FROM employees;
```

#### RANK

Assigns a rank with gaps for ties.

```sql
SELECT
    name,
    score,
    RANK() OVER (ORDER BY score DESC) AS rank
FROM students;
-- Scores: 95, 90, 90, 85 → Ranks: 1, 2, 2, 4
```

#### DENSE_RANK

Assigns a rank without gaps for ties.

```sql
SELECT
    name,
    score,
    DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank
FROM students;
-- Scores: 95, 90, 90, 85 → Ranks: 1, 2, 2, 3
```

### Aggregate Window Functions

Standard aggregate functions can be used as window functions:

```sql
SELECT
    name,
    department,
    salary,
    SUM(salary) OVER (PARTITION BY department) AS dept_total,
    AVG(salary) OVER (PARTITION BY department) AS dept_avg,
    COUNT(*) OVER (PARTITION BY department) AS dept_count,
    MIN(salary) OVER (PARTITION BY department) AS dept_min,
    MAX(salary) OVER (PARTITION BY department) AS dept_max
FROM employees;
```

### Window with ORDER BY

When ORDER BY is specified, aggregate window functions compute a running total:

```sql
SELECT
    order_date,
    amount,
    SUM(amount) OVER (ORDER BY order_date) AS running_total
FROM orders;
```

### ROWS Frame

The default frame above is a RANGE frame: rows that tie on the ORDER BY key
all get the same running total. A ROWS frame stops at the current row
instead, even when later rows tie:

```sql
SELECT
    order_date,
    amount,
    SUM(amount) OVER (ORDER BY order_date ROWS UNBOUNDED PRECEDING) AS running_total
FROM orders;
```

`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is the same frame. This is
the only ROWS frame supported; other frame bounds are rejected.

## Set Operations

Combine results from multiple queries.

### UNION ALL

Returns all rows from both queries, including duplicates:

```sql
SELECT name, email FROM customers
UNION ALL
SELECT name, email FROM prospects;
```

### UNION

Returns distinct rows from both queries:

```sql
SELECT city FROM customers
UNION
SELECT city FROM suppliers;
```

### INTERSECT

Returns rows that appear in both queries:

```sql
SELECT customer_id FROM orders_2023
INTERSECT
SELECT customer_id FROM orders_2024;
```

### EXCEPT

Returns rows from the first query that don't appear in the second:

```sql
SELECT customer_id FROM orders_2023
EXCEPT
SELECT customer_id FROM orders_2024;
```

## GROUP BY with HAVING

`HAVING` filters groups after aggregation, unlike `WHERE` which filters rows before aggregation.

```sql
SELECT
    department,
    COUNT(*) AS employee_count,
    AVG(salary) AS avg_salary
FROM employees
GROUP BY department
HAVING COUNT(*) > 5 AND AVG(salary) > 50000
ORDER BY avg_salary DESC;
```

### ORDER BY on Aggregates and Aliases

Arneb supports ordering by aggregate expressions and column aliases:

```sql
SELECT
    category,
    SUM(revenue) AS total_revenue
FROM sales
GROUP BY category
ORDER BY total_revenue DESC;

SELECT
    region,
    COUNT(*) AS order_count
FROM orders
GROUP BY region
ORDER BY COUNT(*) DESC;
```

## GROUP BY ROLLUP

`GROUP BY ROLLUP(a, b)` adds subtotal rows: it groups by `(a, b)`, then by
`(a)`, then over all rows, with the rolled-up keys set to NULL.
`GROUPING(x)` is 1 on rows where `x` was rolled up and 0 otherwise, which
tells a subtotal's NULL apart from a NULL key value.

```sql
SELECT
    region,
    category,
    SUM(revenue) AS total_revenue,
    GROUPING(category) AS is_region_total
FROM sales
GROUP BY ROLLUP(region, category)
ORDER BY region, is_region_total, category;
```

Limits:

- `ROLLUP(...)` must be the whole GROUP BY clause: one ROLLUP, not mixed
  with plain keys, and no composite elements such as `ROLLUP((a, b), c)`.
- `CUBE` and `GROUPING SETS` are not supported.
- `GROUPING` takes one argument and works only with `ROLLUP`; Trino's
  multi-argument form and `GROUPING` over a plain GROUP BY are rejected.

## Aggregate Functions

COUNT, SUM, AVG, MIN and MAX, plus Trino's variance family:

| Function | Result |
|----------|--------|
| `STDDEV_SAMP(x)`, `STDDEV(x)` | Sample standard deviation |
| `STDDEV_POP(x)` | Population standard deviation |
| `VAR_SAMP(x)`, `VARIANCE(x)` | Sample variance |
| `VAR_POP(x)` | Population variance |

The variance family returns DOUBLE and skips NULL inputs. It returns NULL
when there are too few non-NULL values: none for the population forms,
fewer than two for the sample forms.
