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

### EXISTS Subquery

```sql
SELECT * FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);
```
