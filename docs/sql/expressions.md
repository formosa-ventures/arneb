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

## Regular Expression Match Operators

PostgreSQL's POSIX match operators. Each is the same as a
[`REGEXP_LIKE`](/sql/functions#regexp_like) call:

| Operator | Meaning | Same as |
|----------|---------|---------|
| `a ~ p` | `a` matches `p` | `regexp_like(a, p)` |
| `a ~* p` | matches, ignoring case | `regexp_like(a, '(?i)' \|\| p)` |
| `a !~ p` | does not match | `NOT regexp_like(a, p)` |
| `a !~* p` | does not match, ignoring case | `NOT regexp_like(a, '(?i)' \|\| p)` |

```sql
SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname !~ '^pg_';
SELECT * FROM users WHERE email ~* '@example\.com$';
```

- A match is found anywhere in the string unless the pattern is anchored
  (`^`, `$`), as in PostgreSQL.
- A `NULL` operand gives `NULL`.
- The patterns use the Rust `regex` syntax. PostgreSQL-only syntax such as
  back-references (`\1`) and look-around (`(?=...)`, `(?<=...)`) is refused.
  A constant pattern is checked at planning time, with an error that names
  the operator.

## Untyped String Literals

A quoted literal compared with, or combined with, a number takes the
number's type, as PostgreSQL's untyped literals do:

```sql
SELECT * FROM t WHERE int_col = '0';         -- same as int_col = 0
SELECT NULLIF(n_live_tup, '0') FROM ...;     -- BIGINT
SELECT COALESCE(x, '0') + 1 FROM t;
```

- Only a string *literal* is converted. A text column compared with a number
  column is still a type error.
- A literal that is not a number of that type is an error:
  `x = 'abc'` gives `invalid input syntax for type bigint: "abc"`.

## Dates, Timestamps and Intervals

An interval can be written in the SQL form `INTERVAL '3' MONTH` or in
PostgreSQL's string form `INTERVAL '3 months'` / `INTERVAL '-1 day'`. The
units are `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE` and
`SECOND`, singular or plural.

| Expression | Result |
|------------|--------|
| `date ± INTERVAL 'n' DAY` (or `WEEK`) | `DATE` |
| `date ± INTERVAL 'n' MONTH` (or `QUARTER`, `YEAR`) | `DATE`; the day is clamped to the end of a shorter month (`DATE '2000-03-31' - INTERVAL '1 month'` is `2000-02-29`) |
| `date ± INTERVAL 'n' HOUR` (or `MINUTE`, `SECOND`) | `TIMESTAMP` |
| `timestamp ± INTERVAL ...` | `TIMESTAMP` |
| `date - date` | `INTEGER`, the number of days (PostgreSQL; Trino has no such operator) |
| `date ± integer` | `DATE` |

```sql
SELECT o_orderdate + INTERVAL '1 month' FROM orders;
SELECT count(*) FROM orders WHERE o_orderdate >= date_trunc('year', now() + INTERVAL '-30 year');
SELECT DATE '2000-01-01' - o_orderdate FROM orders;   -- days
```

- An interval is not a value of its own: it can only be added to or
  subtracted from a `DATE` or `TIMESTAMP`. `SELECT INTERVAL '1 day'` and
  `INTERVAL '1 day' * 2` are refused.
- A string with several parts (`INTERVAL '1 day 2 hours'`) and
  `INTERVAL '...' DAY TO SECOND` are refused.
- A `DATE` compared with a `TIMESTAMP` (`=`, `<`, `BETWEEN`, `IN`, ...)
  compares as a `TIMESTAMP` at midnight, and a `DATE` widens to `TIMESTAMP`
  where the two meet in `CASE`, `COALESCE` or a `UNION`.

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

A `DATE` tested against `TIMESTAMP` bounds compares as a `TIMESTAMP` (see
[Dates, Timestamps and Intervals](#dates-timestamps-and-intervals)).

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
it is `NULL` unless another element matches. Array operators other than
these are not supported.

### Array subscripts

`array[i]` is the element at 1-based position `i`. Wrap any expression other
than a column in parentheses: `(ARRAY[10, 20, 30])[2]` is `20`.

```sql
SELECT nspname FROM pg_catalog.pg_namespace
WHERE nspname = (pg_catalog.current_schemas(true))[1];
```

- A position out of range (including `0` and negative positions) gives
  `NULL`, as does a `NULL` array or a `NULL` position.
- The position must be an integer. Slices (`a[1:2]`) are refused, and
  arrays have one dimension, so `a[1][1]` is a type error.

## Composite Field Access

`(expr).field` reads one field of a composite (row) value. In Arneb the
composite values come from `information_schema._pg_expandarray(array)`, a
set-returning function that yields one `(x, n)` row per element (the
element and its 1-based position), which PostgreSQL drivers use to list key
columns:

```sql
SELECT (information_schema._pg_expandarray(ARRAY[7, 8])).n;   -- 1, 2
SELECT (k.e).x, (k.e).n
FROM (SELECT information_schema._pg_expandarray(ARRAY[7, 8]) AS e) k;
```

- An unaliased `(expr).field` column is named after the field, as in
  PostgreSQL.
- An unknown field is an error naming it. A composite value itself cannot be
  sent to a client: selecting `_pg_expandarray(...)` without a field fails
  with `composite values are not supported over pgwire`.

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
