# Functions

Arneb provides 19 built-in scalar functions organized by category.

## String Functions

### UPPER

Converts a string to uppercase.

```sql
UPPER(string) → VARCHAR
```

```sql
SELECT UPPER('hello');  -- 'HELLO'
```

### LOWER

Converts a string to lowercase.

```sql
LOWER(string) → VARCHAR
```

```sql
SELECT LOWER('HELLO');  -- 'hello'
```

### SUBSTRING

Extracts a substring starting at a given position with an optional length.

```sql
SUBSTRING(string FROM start [FOR length]) → VARCHAR
```

```sql
SELECT SUBSTRING('Hello World' FROM 1 FOR 5);  -- 'Hello'
SELECT SUBSTRING('Hello World' FROM 7);         -- 'World'
```

### TRIM

Removes leading and trailing whitespace (or specified characters) from a string.

```sql
TRIM([LEADING | TRAILING | BOTH] [characters FROM] string) → VARCHAR
```

```sql
SELECT TRIM('  hello  ');           -- 'hello'
SELECT TRIM(LEADING ' ' FROM '  hello  ');  -- 'hello  '
```

### LTRIM

Removes leading whitespace from a string.

```sql
LTRIM(string) → VARCHAR
```

```sql
SELECT LTRIM('  hello');  -- 'hello'
```

### RTRIM

Removes trailing whitespace from a string.

```sql
RTRIM(string) → VARCHAR
```

```sql
SELECT RTRIM('hello  ');  -- 'hello'
```

### CONCAT

Concatenates two or more strings.

```sql
CONCAT(string1, string2, ...) → VARCHAR
```

```sql
SELECT CONCAT('Hello', ' ', 'World');  -- 'Hello World'
```

### LENGTH

Returns the number of characters in a string.

```sql
LENGTH(string) → INTEGER
```

```sql
SELECT LENGTH('hello');  -- 5
```

### REPLACE

Replaces all occurrences of a substring within a string.

```sql
REPLACE(string, from, to) → VARCHAR
```

```sql
SELECT REPLACE('hello world', 'world', 'there');  -- 'hello there'
```

### POSITION

Returns the position of a substring within a string (1-based).

```sql
POSITION(substring IN string) → INTEGER
```

```sql
SELECT POSITION('world' IN 'hello world');  -- 7
```

## Math Functions

### ABS

Returns the absolute value of a number.

```sql
ABS(number) → NUMBER
```

```sql
SELECT ABS(-42);  -- 42
```

### ROUND

Rounds a number to a specified number of decimal places.

```sql
ROUND(number [, decimal_places]) → NUMBER
```

```sql
SELECT ROUND(3.14159, 2);  -- 3.14
SELECT ROUND(3.5);          -- 4
```

### CEIL

Returns the smallest integer greater than or equal to the argument.

```sql
CEIL(number) → INTEGER
```

```sql
SELECT CEIL(3.2);   -- 4
SELECT CEIL(-3.2);  -- -3
```

### FLOOR

Returns the largest integer less than or equal to the argument.

```sql
FLOOR(number) → INTEGER
```

```sql
SELECT FLOOR(3.8);   -- 3
SELECT FLOOR(-3.2);  -- -4
```

### MOD

Returns the remainder of a division.

```sql
MOD(dividend, divisor) → NUMBER
```

```sql
SELECT MOD(10, 3);  -- 1
```

### POWER

Returns a number raised to a power.

```sql
POWER(base, exponent) → NUMBER
```

```sql
SELECT POWER(2, 10);  -- 1024
```

## Date Functions

### EXTRACT

Extracts a field from a date or timestamp.

```sql
EXTRACT(field FROM source) → INTEGER
```

Supported fields: `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`.

```sql
SELECT EXTRACT(YEAR FROM DATE '2024-06-15');   -- 2024
SELECT EXTRACT(MONTH FROM DATE '2024-06-15');  -- 6
```

### CURRENT_DATE

Returns the current date.

```sql
CURRENT_DATE → DATE
```

```sql
SELECT CURRENT_DATE;  -- 2024-06-15
```

### DATE_TRUNC

Truncates a date or timestamp to the specified precision.

```sql
DATE_TRUNC('precision', source) → DATE/TIMESTAMP
```

Supported precisions: `year`, `month`, `day`, `hour`, `minute`, `second`.

```sql
SELECT DATE_TRUNC('month', DATE '2024-06-15');  -- 2024-06-01
SELECT DATE_TRUNC('year', DATE '2024-06-15');   -- 2024-01-01
```
