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
