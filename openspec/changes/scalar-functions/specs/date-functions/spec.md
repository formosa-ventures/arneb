## ADDED Requirements

### Requirement: EXTRACT
The system SHALL implement `ExtractFunction` that extracts a date/time field from a Date32 or Timestamp array. It SHALL support the fields YEAR, MONTH, and DAY. The first argument SHALL be a Utf8 literal specifying the field name. The second argument SHALL be a Date32 or Timestamp array. It SHALL return an Int32 array. Null values SHALL be propagated.

#### Scenario: EXTRACT YEAR from date
- **WHEN** `EXTRACT(YEAR FROM date_column)` is evaluated where date_column contains `[2024-01-15, 2023-06-30, NULL]` (Date32)
- **THEN** it returns `[2024, 2023, NULL]` (Int32)

#### Scenario: EXTRACT MONTH from date
- **WHEN** `EXTRACT(MONTH FROM date_column)` is evaluated where date_column contains `[2024-01-15, 2024-12-01]` (Date32)
- **THEN** it returns `[1, 12]` (Int32)

#### Scenario: EXTRACT DAY from date
- **WHEN** `EXTRACT(DAY FROM date_column)` is evaluated where date_column contains `[2024-01-15, 2024-12-31]` (Date32)
- **THEN** it returns `[15, 31]` (Int32)

#### Scenario: EXTRACT with unsupported field
- **WHEN** `EXTRACT(HOUR FROM date_column)` is evaluated on a Date32 column
- **THEN** it returns `Err(ExecutionError::InvalidOperation(...))` indicating the field is not supported for Date32

### Requirement: CURRENT_DATE
The system SHALL implement `CurrentDateFunction` that returns the current date. It SHALL accept zero arguments. It SHALL return a Date32 array where every element is today's date, with length matching the batch row count. The date SHALL be determined at evaluation time.

#### Scenario: CURRENT_DATE returns today
- **WHEN** `CURRENT_DATE` is evaluated against a batch with 3 rows
- **THEN** it returns a Date32 array of `[today, today, today]` where `today` is the current date as days since epoch (1970-01-01)

#### Scenario: CURRENT_DATE with no arguments
- **WHEN** `CURRENT_DATE()` is called with zero arguments
- **THEN** it returns successfully (does not error on zero arguments)

### Requirement: DATE_TRUNC
The system SHALL implement `DateTruncFunction` that truncates a date to the specified precision. It SHALL accept two arguments: a Utf8 literal specifying the truncation level ('year', 'month', 'day') and a Date32 array. It SHALL return a Date32 array. Null values SHALL be propagated.

#### Scenario: DATE_TRUNC to year
- **WHEN** `DATE_TRUNC('year', date_column)` is evaluated where date_column contains `[2024-03-15, 2023-11-30]` (Date32)
- **THEN** it returns `[2024-01-01, 2023-01-01]` (Date32)

#### Scenario: DATE_TRUNC to month
- **WHEN** `DATE_TRUNC('month', date_column)` is evaluated where date_column contains `[2024-03-15, 2023-11-30]` (Date32)
- **THEN** it returns `[2024-03-01, 2023-11-01]` (Date32)

#### Scenario: DATE_TRUNC to day
- **WHEN** `DATE_TRUNC('day', date_column)` is evaluated where date_column contains `[2024-03-15]` (Date32)
- **THEN** it returns `[2024-03-15]` (Date32) (no-op for Date32 since it already has day precision)

#### Scenario: DATE_TRUNC with null
- **WHEN** `DATE_TRUNC('year', date_column)` is evaluated where date_column contains `[2024-03-15, NULL]` (Date32)
- **THEN** it returns `[2024-01-01, NULL]` (Date32)

#### Scenario: DATE_TRUNC with unsupported precision
- **WHEN** `DATE_TRUNC('hour', date_column)` is evaluated on a Date32 column
- **THEN** it returns `Err(ExecutionError::InvalidOperation(...))` indicating the precision is not supported for Date32
