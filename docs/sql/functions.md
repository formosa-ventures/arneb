# Functions

Arneb provides 90 built-in scalar functions (plus 10 alternate spellings), organized by category. Names, argument order, NULL handling and return types follow [Trino](https://trino.io/docs/current/functions.html) unless a difference is called out under [Differences from Trino](#differences-from-trino).

Unless stated otherwise, a function returns `NULL` when any argument is `NULL`. Function names are case-insensitive.

| Category | Functions |
|---|---|
| [Conditional](#conditional-functions) | `IF`, `TRY`, `GREATEST`, `LEAST` (plus `CASE`, `COALESCE`, `NULLIF` — see [Expressions](/sql/expressions)) |
| [String](#string-functions) | `UPPER`, `LOWER`, `SUBSTRING`/`SUBSTR`, `TRIM`, `LTRIM`, `RTRIM`, `CONCAT`, `\|\|`, `CONCAT_WS`, `LENGTH`, `REPLACE`, `POSITION`, `STRPOS`, `SPLIT_PART`, `STARTS_WITH`, `REVERSE`, `LPAD`, `RPAD`, `CHR`, `CODEPOINT`, `TRANSLATE`, `LEVENSHTEIN_DISTANCE`, `HAMMING_DISTANCE` |
| [Regular expression](#regular-expression-functions) | `REGEXP_LIKE`, `REGEXP_EXTRACT`, `REGEXP_REPLACE`, `REGEXP_COUNT` |
| [Math](#math-functions) | `ABS`, `ROUND`, `CEIL`/`CEILING`, `FLOOR`, `TRUNCATE`, `MOD`, `POWER`/`POW`, `SQRT`, `CBRT`, `EXP`, `LN`, `LOG2`, `LOG10`, `LOG`, `SIGN`, `PI`, `E`, `RANDOM`/`RAND`, `DEGREES`, `RADIANS`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `SINH`, `COSH`, `TANH`, `NAN`, `INFINITY`, `IS_NAN`, `IS_FINITE`, `IS_INFINITE` |
| [Date and time](#date-and-time-functions) | `CURRENT_DATE`, `NOW`/`CURRENT_TIMESTAMP`/`LOCALTIMESTAMP`, `EXTRACT`, `YEAR`, `QUARTER`, `MONTH`, `WEEK`/`WEEK_OF_YEAR`, `DAY`/`DAY_OF_MONTH`, `DAY_OF_WEEK`/`DOW`, `DAY_OF_YEAR`/`DOY`, `YEAR_OF_WEEK`/`YOW`, `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`, `DATE_TRUNC`, `DATE_ADD`, `DATE_DIFF`, `LAST_DAY_OF_MONTH`, `DATE`, `FROM_UNIXTIME`, `TO_UNIXTIME`, `DATE_FORMAT`, `DATE_PARSE`, `FORMAT_DATETIME` |

## Conditional Functions

### IF

Returns `true_value` when `condition` is true, otherwise `false_value` (or `NULL` when omitted). Only the selected branch is evaluated per row; a `NULL` condition selects the false branch.

```sql
IF(condition, true_value [, false_value]) → same type as the values
```

```sql
SELECT IF(amount > 100, 'large', 'small') FROM orders;
SELECT IF(status = 'F', 1);  -- 1 or NULL
```

### TRY

Evaluates `expression` and returns `NULL` instead of failing for rows whose evaluation raises an error (invalid argument, bad cast, division by zero, ...). Other rows keep their value.

```sql
TRY(expression) → type of expression
```

```sql
SELECT TRY(date_parse(raw_ts, '%Y-%m-%d'));   -- NULL for malformed rows
SELECT TRY(chr(code_point));                  -- NULL for invalid code points
```

### GREATEST / LEAST

Return the largest / smallest argument. Arguments are coerced to their common supertype, which is also the return type. Returns `NULL` if **any** argument is `NULL`.

```sql
GREATEST(value1, value2, ...) → common type
LEAST(value1, value2, ...) → common type
```

```sql
SELECT GREATEST(1, 5, 3);          -- 5
SELECT LEAST(2, 1.5);              -- 1.5
SELECT GREATEST('apple', 'kiwi');  -- 'kiwi'
SELECT GREATEST(1, NULL);          -- NULL
```

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

Extracts a substring starting at a 1-based position with an optional length. `SUBSTR` is an alias.

```sql
SUBSTRING(string FROM start [FOR length]) → VARCHAR
SUBSTRING(string, start [, length]) → VARCHAR
```

```sql
SELECT SUBSTRING('Hello World' FROM 1 FOR 5);  -- 'Hello'
SELECT SUBSTR('Hello World', 7);               -- 'World'
```

### TRIM

Removes leading and trailing whitespace, or any of the given characters, from a string.

```sql
TRIM([LEADING | TRAILING | BOTH] [characters FROM] string) → VARCHAR
TRIM(string [, characters]) → VARCHAR
```

```sql
SELECT TRIM('  hello  ');                    -- 'hello'
SELECT TRIM(BOTH '*' FROM '**hello**');      -- 'hello'
SELECT TRIM(LEADING '0' FROM '000123');      -- '123'
SELECT TRIM('xxhixyx', 'xy');                -- 'hi'
```

### LTRIM / RTRIM

Remove leading (`LTRIM`) or trailing (`RTRIM`) whitespace, or any of the given characters.

```sql
LTRIM(string [, characters]) → VARCHAR
RTRIM(string [, characters]) → VARCHAR
```

```sql
SELECT LTRIM('  hello');      -- 'hello'
SELECT RTRIM('hello!!', '!'); -- 'hello'
```

### CONCAT / `||`

Concatenates strings. Returns `NULL` if any argument is `NULL`.

```sql
CONCAT(string1, string2, ...) → VARCHAR
string1 || string2 → VARCHAR
```

```sql
SELECT CONCAT('Hello', ' ', 'World');  -- 'Hello World'
SELECT 'Hello' || '!';                 -- 'Hello!'
```

### CONCAT_WS

Joins strings with a separator, **skipping** `NULL` arguments. Returns `NULL` only when the separator is `NULL`.

```sql
CONCAT_WS(separator, string1, ...) → VARCHAR
```

```sql
SELECT CONCAT_WS(',', 'a', NULL, 'b');  -- 'a,b'
```

### LENGTH

Returns the number of characters in a string.

```sql
LENGTH(string) → BIGINT
```

```sql
SELECT LENGTH('hello');  -- 5
```

### REPLACE

Replaces all occurrences of `search` with `replacement`; the 2-argument form removes them.

```sql
REPLACE(string, search [, replacement]) → VARCHAR
```

```sql
SELECT REPLACE('hello world', 'world', 'there');  -- 'hello there'
SELECT REPLACE('a-b-c', '-');                     -- 'abc'
```

### POSITION / STRPOS

Return the 1-based character position of a substring, or `0` when absent. `STRPOS` takes an optional `instance` to find the n-th occurrence (must be positive).

```sql
POSITION(substring IN string) → BIGINT
STRPOS(string, substring [, instance]) → BIGINT
```

```sql
SELECT POSITION('world' IN 'hello world');  -- 7
SELECT STRPOS('high', 'ig');                -- 2
SELECT STRPOS('abcabc', 'bc', 2);           -- 5
```

### SPLIT_PART

Splits `string` on `delimiter` and returns field `index` (1-based). Returns `NULL` when `index` exceeds the number of fields; `index` must be positive.

```sql
SPLIT_PART(string, delimiter, index) → VARCHAR
```

```sql
SELECT SPLIT_PART('a,b,c', ',', 2);  -- 'b'
SELECT SPLIT_PART('a,b,c', ',', 4);  -- NULL
```

### STARTS_WITH

Tests whether `string` starts with `prefix`.

```sql
STARTS_WITH(string, prefix) → BOOLEAN
```

```sql
SELECT STARTS_WITH('hello', 'he');  -- true
```

### REVERSE

Reverses the characters of a string.

```sql
REVERSE(string) → VARCHAR
```

```sql
SELECT REVERSE('abc');  -- 'cba'
```

### LPAD / RPAD

Pad `string` on the left / right to `size` characters with `padstring` (repeated as needed). If `string` is longer than `size` it is truncated to `size` characters. `size` must be non-negative and `padstring` non-empty.

```sql
LPAD(string, size, padstring) → VARCHAR
RPAD(string, size, padstring) → VARCHAR
```

```sql
SELECT LPAD('42', 5, '0');     -- '00042'
SELECT RPAD('ab', 5, 'xy');    -- 'abxyx'
SELECT LPAD('hello', 3, '*');  -- 'hel'
```

### CHR / CODEPOINT

`CHR` returns the character for a Unicode code point; `CODEPOINT` returns the code point of a single-character string. Invalid input is an error.

```sql
CHR(n) → VARCHAR
CODEPOINT(string) → INTEGER
```

```sql
SELECT CHR(65);         -- 'A'
SELECT CODEPOINT('A');  -- 65
```

### TRANSLATE

Replaces each character of `source` that appears in `from` with the character at the same position in `to`. Characters of `from` without a counterpart in `to` are deleted.

```sql
TRANSLATE(source, from, to) → VARCHAR
```

```sql
SELECT TRANSLATE('abcd', 'abc', 'x');  -- 'xd'
```

### LEVENSHTEIN_DISTANCE / HAMMING_DISTANCE

Edit distance between two strings. `HAMMING_DISTANCE` requires strings of equal length.

```sql
LEVENSHTEIN_DISTANCE(string1, string2) → BIGINT
HAMMING_DISTANCE(string1, string2) → BIGINT
```

```sql
SELECT LEVENSHTEIN_DISTANCE('kitten', 'sitting');  -- 3
SELECT HAMMING_DISTANCE('abcde', 'abxdz');         -- 2
```

## Regular Expression Functions

Patterns use the Rust [`regex`](https://docs.rs/regex) syntax, which matches Trino's Java-style syntax for character classes, quantifiers, anchors, (named) groups and inline flags such as `(?i)`. Look-around and back-references are not supported.

### REGEXP_LIKE

Tests whether `pattern` matches anywhere in `string` (anchor with `^`/`$` for a full match).

```sql
REGEXP_LIKE(string, pattern) → BOOLEAN
```

```sql
SELECT * FROM users WHERE REGEXP_LIKE(email, '@example\.com$');
```

### REGEXP_EXTRACT

Returns the first match of `pattern` (or of capturing `group`); `NULL` when nothing matches.

```sql
REGEXP_EXTRACT(string, pattern [, group]) → VARCHAR
```

```sql
SELECT REGEXP_EXTRACT('1a 2b 14m', '\d+');               -- '1'
SELECT REGEXP_EXTRACT('1a 2b 14m', '(\d+)([a-z]+)', 2);  -- 'a'
```

### REGEXP_REPLACE

Replaces every match of `pattern` with `replacement` (default: remove matches). The replacement can reference groups as `$1` or `${name}`; write `\$` for a literal dollar sign.

```sql
REGEXP_REPLACE(string, pattern [, replacement]) → VARCHAR
```

```sql
SELECT REGEXP_REPLACE('1a 2b 14m', '\d+[ab] ');                 -- '14m'
SELECT REGEXP_REPLACE('1a 2b 14m', '(\d+)([ab]) ', '3c$2 ');    -- '3ca 3cb 14m'
```

### REGEXP_COUNT

Counts the matches of `pattern` in `string`.

```sql
REGEXP_COUNT(string, pattern) → BIGINT
```

```sql
SELECT REGEXP_COUNT('1a 2b 14m', '\d+');  -- 3
```

## Math Functions

### ABS

Returns the absolute value of a number.

```sql
ABS(number) → same type
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

### CEIL / CEILING, FLOOR

Round up / down to the nearest integer.

```sql
CEIL(number) → NUMBER
FLOOR(number) → NUMBER
```

```sql
SELECT CEIL(3.2);    -- 4
SELECT FLOOR(-3.2);  -- -4
```

### TRUNCATE

Rounds toward zero, optionally keeping `n` decimal places (negative `n` zeroes digits left of the decimal point).

```sql
TRUNCATE(number [, n]) → BIGINT for integer input, otherwise DOUBLE
```

```sql
SELECT TRUNCATE(-3.7);         -- -3.0
SELECT TRUNCATE(3.14159, 2);   -- 3.14
```

### MOD

Returns the remainder of a division.

```sql
MOD(dividend, divisor) → NUMBER
```

```sql
SELECT MOD(10, 3);  -- 1
```

### POWER / POW

Returns a number raised to a power.

```sql
POWER(base, exponent) → DOUBLE
```

```sql
SELECT POWER(2, 10);  -- 1024
```

### SQRT, CBRT, EXP, LN, LOG2, LOG10, LOG

Square / cube root, `e^x`, and logarithms. `LOG(b, x)` is the base-`b` logarithm of `x`. Out-of-domain input follows IEEE-754 like Trino: `SQRT(-1)` is `NaN`, `LN(0)` is `-Infinity`.

```sql
SQRT(x) → DOUBLE      CBRT(x) → DOUBLE      EXP(x) → DOUBLE
LN(x) → DOUBLE        LOG2(x) → DOUBLE      LOG10(x) → DOUBLE
LOG(b, x) → DOUBLE
```

```sql
SELECT SQRT(16), LOG10(1000), LOG(2, 8);  -- 4.0, 3.0, 3.0
```

### SIGN

Returns -1, 0 or 1 (`NaN` for `NaN`).

```sql
SIGN(x) → BIGINT for integer input, otherwise DOUBLE
```

### PI, E, NAN, INFINITY

Constants.

```sql
PI() → DOUBLE    E() → DOUBLE    NAN() → DOUBLE    INFINITY() → DOUBLE
```

### IS_NAN, IS_FINITE, IS_INFINITE

Floating-point classification.

```sql
IS_NAN(x) → BOOLEAN    IS_FINITE(x) → BOOLEAN    IS_INFINITE(x) → BOOLEAN
```

### RANDOM / RAND

`RANDOM()` returns a pseudo-random `DOUBLE` in `[0, 1)`, a new value per row. `RANDOM(n)` returns a pseudo-random `BIGINT` in `[0, n)`; `n` must be positive. Not cryptographically secure.

```sql
SELECT * FROM events WHERE RANDOM() < 0.01;  -- ~1% sample
```

### Trigonometric functions

All take and return `DOUBLE`, in radians.

```sql
SIN(x)  COS(x)  TAN(x)  ASIN(x)  ACOS(x)  ATAN(x)  ATAN2(y, x)
SINH(x) COSH(x) TANH(x)  DEGREES(x)  RADIANS(x)
```

## Date and Time Functions

DATE values are days since 1970-01-01. TIMESTAMP values returned by these functions have microsecond precision and no time zone. Arneb does not yet have a session time zone: all functions behave as if the Trino session time zone were UTC.

### CURRENT_DATE / NOW

`CURRENT_DATE` returns today's date; `NOW()` (aliases `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`) returns the current timestamp.

```sql
CURRENT_DATE → DATE
NOW() → TIMESTAMP
```

### EXTRACT

Extracts a field from a date or timestamp.

```sql
EXTRACT(field FROM source) → BIGINT
```

Supported fields: `YEAR`, `QUARTER`, `MONTH`, `WEEK` (ISO week), `DAY`, `DOW`/`DAY_OF_WEEK` (ISO: Monday = 1 … Sunday = 7), `DOY`/`DAY_OF_YEAR`, `YOW`/`YEAR_OF_WEEK` (ISO week-year), `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`. Time fields of a DATE are 0.

```sql
SELECT EXTRACT(YEAR FROM DATE '2024-06-15');   -- 2024
SELECT EXTRACT(DOW FROM DATE '2024-07-04');    -- 4 (Thursday)
SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-06-15 10:30:00');  -- 10
```

### YEAR, QUARTER, MONTH, WEEK, DAY, DAY_OF_WEEK, DAY_OF_YEAR, YEAR_OF_WEEK, HOUR, MINUTE, SECOND, MILLISECOND

Shorthand for the matching `EXTRACT` field. Aliases: `WEEK_OF_YEAR`, `DAY_OF_MONTH`, `DOW`, `DOY`, `YOW`.

```sql
YEAR(x) → BIGINT   ...   MILLISECOND(x) → BIGINT
```

```sql
SELECT YEAR(o_orderdate), COUNT(*) FROM orders GROUP BY YEAR(o_orderdate);
```

### DATE_TRUNC

Truncates a date or timestamp to the given unit. Returns the same type as its input.

```sql
DATE_TRUNC(unit, x) → same type as x
```

Units: `millisecond`, `second`, `minute`, `hour` (timestamps only), `day`, `week` (ISO, Monday), `month`, `quarter`, `year`.

```sql
SELECT DATE_TRUNC('month', DATE '2024-06-15');                  -- 2024-06-01
SELECT DATE_TRUNC('hour', TIMESTAMP '2024-06-15 10:30:45');     -- 2024-06-15 10:00:00
```

### DATE_ADD

Adds `value` units to a date or timestamp (negative values subtract). Month-based units clamp to the end of the month. DATE inputs accept only `day` and larger units.

```sql
DATE_ADD(unit, value, x) → same type as x
```

```sql
SELECT DATE_ADD('month', 1, DATE '2024-01-31');   -- 2024-02-29
SELECT DATE_ADD('day', -7, CURRENT_DATE);
SELECT DATE_ADD('hour', 3, TIMESTAMP '2024-01-31 22:00:00');  -- 2024-02-01 01:00:00
```

### DATE_DIFF

Number of whole `unit`s from `a` to `b` (negative when `b` is earlier), truncated toward zero. Month-based units count calendar months (`2024-01-31` → `2024-02-29` is 1 month).

```sql
DATE_DIFF(unit, a, b) → BIGINT
```

```sql
SELECT DATE_DIFF('day', DATE '2024-01-01', DATE '2024-03-01');   -- 60
SELECT DATE_DIFF('month', DATE '2024-01-31', DATE '2024-02-29'); -- 1
```

### LAST_DAY_OF_MONTH

```sql
LAST_DAY_OF_MONTH(x) → DATE
```

```sql
SELECT LAST_DAY_OF_MONTH(DATE '2024-02-10');  -- 2024-02-29
```

### DATE

Converts a VARCHAR (`'YYYY-MM-DD'`) or TIMESTAMP to a DATE; same as `CAST(x AS DATE)`.

```sql
DATE(x) → DATE
```

### FROM_UNIXTIME / TO_UNIXTIME

Convert between Unix epoch seconds (with fraction) and timestamps.

```sql
FROM_UNIXTIME(seconds) → TIMESTAMP
TO_UNIXTIME(timestamp) → DOUBLE
```

```sql
SELECT FROM_UNIXTIME(0);                            -- 1970-01-01 00:00:00
SELECT TO_UNIXTIME(TIMESTAMP '1970-01-02 00:00:00');  -- 86400.0
```

### DATE_FORMAT / DATE_PARSE

Format and parse timestamps with MySQL-style specifiers.

```sql
DATE_FORMAT(timestamp, format) → VARCHAR
DATE_PARSE(string, format) → TIMESTAMP
```

| Specifier | Meaning | Specifier | Meaning |
|---|---|---|---|
| `%Y` | Year, 4 digits | `%y` | Year, 2 digits |
| `%m` | Month `01`–`12` | `%c` | Month `1`–`12` |
| `%M` | Month name (`January`) | `%b` | Abbreviated month (`Jan`) |
| `%d` | Day `01`–`31` | `%e` | Day `1`–`31` |
| `%D` | Day with suffix (`1st`; format only) | `%j` | Day of year `001`–`366` |
| `%H` | Hour `00`–`23` | `%k` | Hour `0`–`23` |
| `%h`, `%I` | Hour `01`–`12` | `%l` | Hour `1`–`12` |
| `%i` | Minutes `00`–`59` | `%S`, `%s` | Seconds `00`–`59` |
| `%f` | Microseconds `000000`–`999999` | `%p` | `AM` / `PM` |
| `%T` | `%H:%i:%s` | `%r` | `%h:%i:%s %p` |
| `%W` | Weekday name (`Sunday`) | `%a` | Abbreviated weekday (`Sun`) |
| `%v` | ISO week `01`–`53` | `%x` | ISO week-year |
| `%%` | Literal `%` | | |

`%U`, `%u`, `%V`, `%w`, `%X` are not supported (Trino rejects them too). For `DATE_PARSE`, fields missing from the format default to `1970-01-01 00:00:00`; input that does not match is an error (wrap in `TRY` to get `NULL`).

```sql
SELECT DATE_FORMAT(TIMESTAMP '2024-03-01 15:04:05', '%Y-%m-%d %H:%i:%s');  -- '2024-03-01 15:04:05'
SELECT DATE_FORMAT(TIMESTAMP '2024-03-01 15:04:05', '%W, %M %D %Y');       -- 'Friday, March 1st 2024'
SELECT DATE_PARSE('03/15/2024 07:05 PM', '%m/%d/%Y %h:%i %p');              -- 2024-03-15 19:05:00
```

### FORMAT_DATETIME

Formats a timestamp with a [Joda-Time](https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html) pattern.

```sql
FORMAT_DATETIME(timestamp, format) → VARCHAR
```

Supported letters: `yyyy`/`yy` (year), `xxxx` (ISO week-year), `M`/`MM`/`MMM`/`MMMM` (month), `d`/`dd`, `D`/`DDD` (day of year), `H`/`HH`, `h`/`hh`, `m`/`mm`, `s`/`ss`, `S`…`SSSSSSSSS` (fraction of second), `a` (AM/PM), `E`/`EEE` and `EEEE` (weekday), `e` (ISO day of week), `w`/`ww` (ISO week), `Z` (`+0000`), `z` (`UTC`), and `'quoted literals'`.

```sql
SELECT FORMAT_DATETIME(TIMESTAMP '2024-03-01 15:04:05', 'yyyy-MM-dd''T''HH:mm:ss');  -- '2024-03-01T15:04:05'
SELECT FORMAT_DATETIME(TIMESTAMP '2024-03-01 15:04:05', 'EEE, d MMM yyyy');          -- 'Fri, 1 Mar 2024'
```

## Differences from Trino

- **Time zones**: there is no session time zone. `NOW()`/`CURRENT_TIMESTAMP` and `FROM_UNIXTIME` return a zone-less `TIMESTAMP` holding UTC time, where Trino returns `TIMESTAMP WITH TIME ZONE` in the session zone. `NOW()` is read once per batch rather than once per query.
- **Regular expressions** use the Rust `regex` engine: no look-around or back-references, and matching is always linear-time.
- **Decimal results**: `SIGN`, `TRUNCATE` and the transcendental functions return `DOUBLE` for `DECIMAL` input (Trino returns `DECIMAL` for `SIGN`/`TRUNCATE`). `RANDOM(n)` always returns `BIGINT`.
- **Time fields of a DATE** (`HOUR(date)`, ...) return 0 instead of failing analysis.
- **`TRY`** turns every evaluation error into `NULL`; Trino only catches a fixed set of error codes (in practice the same ones scalar evaluation raises).
- **`GREATEST`/`LEAST` with `NaN`** use IEEE total order (`NaN` is the largest value).
- **`FORMAT_DATETIME`** fraction widths other than 3, 6 or 9 `S` letters round up to the next of those widths.
- Not yet available: `LEFT`/`RIGHT` (not Trino functions either), `TO_HEX`/`FROM_HEX` and other `VARBINARY` functions, `SPLIT` / `REGEXP_EXTRACT_ALL` and other array-returning functions, `AT TIME ZONE`, `INTERVAL` arithmetic beyond day units, JSON and URL functions.
