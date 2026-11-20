# Expressions

Arneb supports a wide range of SQL expressions for filtering, transforming, and computing values.

## Arithmetic Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition | `price + tax` |
| `-` | Subtraction | `total - discount` |
| `*` | Multiplication | `quantity * price` |
| `/` | Division | `total / count` |
| `%` | Modulo | `id % 10` |

## Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal | `status = 'active'` |
| `<>` or `!=` | Not equal | `status <> 'deleted'` |
| `<` | Less than | `price < 100` |
| `>` | Greater than | `quantity > 0` |
| `<=` | Less than or equal | `age <= 65` |
| `>=` | Greater than or equal | `score >= 90` |

## Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `AND` | Logical AND | `a > 1 AND b < 10` |
| `OR` | Logical OR | `status = 'A' OR status = 'B'` |
| `NOT` | Logical NOT | `NOT is_deleted` |

## CASE Expression

```sql
CASE
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    ELSE default_result
END
```

Example:

```sql
SELECT
    name,
    CASE
        WHEN score >= 90 THEN 'A'
        WHEN score >= 80 THEN 'B'
        WHEN score >= 70 THEN 'C'
        ELSE 'F'
    END AS grade
FROM students;
```

## COALESCE

Returns the first non-NULL argument.

```sql
COALESCE(value1, value2, ...)
```

```sql
SELECT COALESCE(nickname, first_name, 'Unknown') AS display_name
FROM users;
```

## NULLIF

Returns NULL if the two arguments are equal; otherwise returns the first argument.

```sql
NULLIF(value1, value2)
```

```sql
SELECT NULLIF(discount, 0) AS safe_discount
FROM orders;
```

## CAST

Convert a value to a different data type.

```sql
CAST(expression AS type)
```

```sql
SELECT CAST(price AS INTEGER) FROM products;
SELECT CAST('2024-01-15' AS DATE) AS order_date;
```

Casting a DOUBLE, REAL or DECIMAL to an integer type (TINYINT, SMALLINT,
INTEGER, BIGINT) rounds half away from zero, as Trino does: `2.5` becomes
`3` and `-2.5` becomes `-3`. A value whose rounded result does not fit the
target type is an error, for example `CAST(2147483647.5e0 AS INTEGER)` or
`CAST(DECIMAL '9223372036854775807.5' AS BIGINT)`; wrap the cast in [`TRY`](/sql/functions#try) to get
`NULL` instead.

## BETWEEN

Test whether a value falls within a range (inclusive).

```sql
expression BETWEEN low AND high
```

```sql
SELECT * FROM orders WHERE total BETWEEN 100 AND 500;
```

## IN

Test whether a value matches any value in a list or subquery.

```sql
expression IN (value1, value2, ...)
expression IN (subquery)
```

```sql
SELECT * FROM orders WHERE status IN ('shipped', 'delivered');
SELECT * FROM users WHERE id IN (SELECT user_id FROM premium_users);
```

`expression = ANY (array)` is true when the value equals an element of the
array, and `expression <> ALL (array)` when it equals none. The array may be
a literal, a column, a cast or a bound parameter. See
[Arrays](/sql/advanced#arrays).

```sql
SELECT * FROM orders WHERE status = ANY (ARRAY['shipped', 'delivered']);
SELECT * FROM orders WHERE id = ANY ($1);   -- $1 bound as int8[]
```

## LIKE

Pattern matching on strings. `%` matches any sequence of characters, `_` matches a single character.

```sql
expression LIKE pattern
```

```sql
SELECT * FROM products WHERE name LIKE 'Widget%';
SELECT * FROM users WHERE email LIKE '%@example.com';
```

## IS NULL / IS NOT NULL

Test for NULL values.

```sql
SELECT * FROM orders WHERE shipped_at IS NULL;
SELECT * FROM users WHERE email IS NOT NULL;
```

## ARRAY constructor and ANY / ALL

`ARRAY[a, b, ...]` builds a one-dimensional array. The elements must have
one common type (integers widen to the widest integer or to `double`; text
stays text); `NULL` elements are allowed. Over pgwire an array is sent as a
PostgreSQL array literal (`{1,2,NULL}`), quoting elements that are empty,
contain a comma, quote, backslash, brace or whitespace, are `NULL` text, or
are nested. Over the Trino protocol it is an `array(...)` of its element
type, which the Python client returns as a `list`.

```sql
SELECT ARRAY[1, 2, 3], ARRAY['a,b', NULL];
```

`x = ANY (array)` and `x <> ALL (array)` work over an array expression or an
array bind parameter (for example `= ANY($1)` with `{}` or `{1,NULL}`):
`= ANY` over an empty array is false, and over an array with a `NULL` element
it is `NULL` unless another element matches. Array subscripts and array
operators other than these are not supported.

## Subquery Expressions

### Scalar Subquery

A subquery that returns a single value:

```sql
SELECT
    name,
    (SELECT AVG(score) FROM scores WHERE scores.user_id = users.id) AS avg_score
FROM users;
```

### IN Subquery

```sql
SELECT * FROM orders
WHERE customer_id IN (SELECT id FROM customers WHERE region = 'US');
```

`IN` and `NOT IN` subqueries can sit anywhere in a `WHERE` clause, under
`AND`, `OR` and `NOT`, for example
`WHERE x NOT IN (SELECT y FROM u) OR x = 2`. They follow SQL's three-valued
logic: `x NOT IN (...)` is NULL, so the row is dropped, when `x` is NULL
and the subquery has rows, or when the subquery returns a NULL and nothing
matches.

### EXISTS Subquery

```sql
SELECT * FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);
```

### Correlated scalar subquery in the SELECT list

A scalar subquery in the `SELECT` list (or under a `CASE` branch) may refer to
the outer row:

```sql
SELECT c_custkey,
       (SELECT count(*) FROM orders WHERE o_custkey = c_custkey),
       CASE WHEN c_acctbal < 0
            THEN (SELECT r_name FROM region WHERE r_regionkey = c_nationkey % 5)
       END
FROM customer;
```

- The subquery must have at least one equality correlation
  (`inner_col = outer_col`); it is planned as a left join on those keys, so
  an unmatched row gets `NULL`; an aggregate subquery gets its empty-set
  value (`count(*)` → 0), as in PostgreSQL.
- A subquery that yields more than one row for an outer row fails with
  `more than one row returned by a subquery used as an expression`, but
  only for rows where the expression is evaluated: a `CASE` branch that is not
  taken does not raise.
- A correlated subquery over a CTE, a derived table or `LATERAL`, one
  correlated through two levels, and one with no equality correlation are
  refused with a planning error. An outer reference that cannot be
  decorrelated is never silently ignored.
