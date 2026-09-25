## ADDED Requirements

### Requirement: Date-part functions
The system SHALL implement `YEAR`, `QUARTER`, `MONTH`, `WEEK` (ISO week), `DAY`, `DAY_OF_WEEK` (ISO, Monday = 1), `DAY_OF_YEAR`, `YEAR_OF_WEEK` (ISO week-year), `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`, each returning BIGINT via Arrow's `date_part` kernel over DATE or TIMESTAMP input, with aliases `WEEK_OF_YEAR`, `DAY_OF_MONTH`, `DOW`, `DOY`, `YOW`.

#### Scenario: ISO week edge
- **WHEN** `WEEK(DATE '2021-01-01')` and `YEAR_OF_WEEK(DATE '2021-01-01')` are evaluated
- **THEN** they return `53` and `2020`

#### Scenario: Sunday
- **WHEN** `DAY_OF_WEEK(DATE '2024-01-07')` is evaluated
- **THEN** it returns `7`

### Requirement: DATE_ADD
The system SHALL implement `DATE_ADD(unit, value, x)` returning the type of `x`. Units: millisecond, second, minute, hour, day, week, month, quarter, year. Month-based units SHALL clamp to the end of the target month. DATE input SHALL reject sub-day units.

#### Scenario: Month-end clamping
- **WHEN** `DATE_ADD('month', 1, DATE '2024-01-31')` is evaluated
- **THEN** it returns `2024-02-29`

### Requirement: DATE_DIFF
The system SHALL implement `DATE_DIFF(unit, a, b) → BIGINT`, the whole number of units from `a` to `b` truncated toward zero; month-based units SHALL use Joda-Time month arithmetic.

#### Scenario: End-of-month month difference
- **WHEN** `DATE_DIFF('month', DATE '2024-01-31', DATE '2024-02-29')` is evaluated
- **THEN** it returns `1`

### Requirement: Current time and unix time
The system SHALL implement `NOW()` (aliases `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`) returning a microsecond TIMESTAMP (UTC) per row, `FROM_UNIXTIME(seconds) → TIMESTAMP`, `TO_UNIXTIME(x) → DOUBLE`, `LAST_DAY_OF_MONTH(x) → DATE`, and `DATE(x) → DATE`.

#### Scenario: Unix time round trip
- **WHEN** `TO_UNIXTIME(FROM_UNIXTIME(1700000000.5))` is evaluated
- **THEN** it returns `1700000000.5`

### Requirement: Formatting and parsing
The system SHALL implement `DATE_FORMAT(x, format) → VARCHAR` and `DATE_PARSE(string, format) → TIMESTAMP` with MySQL specifiers (`%Y %y %m %c %M %b %d %e %D %j %H %k %h %I %l %i %S %s %f %p %T %r %W %a %v %x %%`), and `FORMAT_DATETIME(x, pattern) → VARCHAR` with a Joda-Time pattern subset. `DATE_PARSE` SHALL default missing fields to `1970-01-01 00:00:00` and SHALL error on non-matching input. Unsupported specifiers SHALL error.

#### Scenario: Day with suffix
- **WHEN** `DATE_FORMAT(TIMESTAMP '2024-03-01 15:04:05', '%W, %M %D %Y')` is evaluated
- **THEN** it returns `'Friday, March 1st 2024'`

#### Scenario: 12-hour parse
- **WHEN** `DATE_PARSE('03/15/2024 07:05 PM', '%m/%d/%Y %h:%i %p')` is evaluated
- **THEN** it returns `2024-03-15 19:05:00`

#### Scenario: Joda pattern with literal
- **WHEN** `FORMAT_DATETIME(TIMESTAMP '2024-03-01 15:04:05', 'yyyy-MM-dd''T''HH:mm:ss')` is evaluated
- **THEN** it returns `'2024-03-01T15:04:05'`

## MODIFIED Requirements

### Requirement: EXTRACT
The system SHALL implement `ExtractFunction` over DATE and TIMESTAMP input returning BIGINT, supporting fields YEAR, QUARTER, MONTH, WEEK, DAY, DOW/DAY_OF_WEEK (ISO), DOY/DAY_OF_YEAR, YOW/YEAR_OF_WEEK, HOUR, MINUTE, SECOND, MILLISECOND. Unknown fields SHALL error. Null values SHALL be propagated.

#### Scenario: EXTRACT HOUR from timestamp
- **WHEN** `EXTRACT(HOUR FROM TIMESTAMP '2024-06-15 10:30:00')` is evaluated
- **THEN** it returns `10`

#### Scenario: EXTRACT DOW
- **WHEN** `EXTRACT(DOW FROM DATE '2024-07-04')` is evaluated
- **THEN** it returns `4`

### Requirement: DATE_TRUNC
The system SHALL implement `DateTruncFunction` returning the type of its input. DATE input SHALL support day, week (ISO Monday), month, quarter, year and SHALL reject smaller units; TIMESTAMP input SHALL additionally support millisecond, second, minute, hour. Truncation of pre-epoch timestamps SHALL round down.

#### Scenario: Truncate timestamp to hour
- **WHEN** `DATE_TRUNC('hour', TIMESTAMP '2024-06-15 10:30:45')` is evaluated
- **THEN** it returns `2024-06-15 10:00:00`

#### Scenario: Truncate date to week
- **WHEN** `DATE_TRUNC('week', DATE '2024-05-15')` is evaluated
- **THEN** it returns `2024-05-13`

### Requirement: CURRENT_DATE
`CURRENT_DATE` SHALL return one DATE value per row of the batch it is evaluated against.

#### Scenario: CURRENT_DATE over a multi-row table
- **WHEN** `SELECT current_date FROM t` is executed against a 3-row table
- **THEN** it returns 3 rows
