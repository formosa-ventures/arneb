## ADDED Requirements

### Requirement: ABS
The system SHALL implement `AbsFunction` that returns the absolute value of a numeric array. It SHALL accept exactly one numeric argument (Int32, Int64, Float32, Float64) and return an array of the same type. It SHALL use Arrow's `abs` compute kernel. Null values SHALL be propagated.

#### Scenario: ABS of negative integers
- **WHEN** `ABS` is evaluated with input `[-1, 0, 3, -5]` (Int64)
- **THEN** it returns `[1, 0, 3, 5]` (Int64)

#### Scenario: ABS of negative floats
- **WHEN** `ABS` is evaluated with input `[-1.5, 2.0, NULL]` (Float64)
- **THEN** it returns `[1.5, 2.0, NULL]` (Float64)

### Requirement: ROUND, CEIL, FLOOR
The system SHALL implement `RoundFunction`, `CeilFunction`, and `FloorFunction` for numeric arrays. `ROUND` SHALL accept one or two arguments: `ROUND(value)` rounds to the nearest integer, `ROUND(value, decimals)` rounds to the specified number of decimal places. `CEIL` and `FLOOR` SHALL each accept exactly one numeric argument. All three SHALL return Float64. Null values SHALL be propagated.

#### Scenario: ROUND to nearest integer
- **WHEN** `ROUND` is evaluated with input `[1.4, 1.5, 2.6, NULL]` (Float64)
- **THEN** it returns `[1.0, 2.0, 3.0, NULL]` (Float64)

#### Scenario: ROUND with decimal places
- **WHEN** `ROUND(3.14159, 2)` is evaluated
- **THEN** it returns `3.14`

#### Scenario: CEIL rounds up
- **WHEN** `CEIL` is evaluated with input `[1.1, 2.0, -1.5, NULL]` (Float64)
- **THEN** it returns `[2.0, 2.0, -1.0, NULL]` (Float64)

#### Scenario: FLOOR rounds down
- **WHEN** `FLOOR` is evaluated with input `[1.9, 2.0, -1.5, NULL]` (Float64)
- **THEN** it returns `[1.0, 2.0, -2.0, NULL]` (Float64)

#### Scenario: CEIL and FLOOR with integer input
- **WHEN** `CEIL` or `FLOOR` is evaluated with input `[1, 2, 3]` (Int32)
- **THEN** the input is cast to Float64 and returned as `[1.0, 2.0, 3.0]` (Float64)

### Requirement: MOD
The system SHALL implement `ModFunction` that computes the modulo (remainder) of two numeric values. It SHALL accept exactly two numeric arguments: `MOD(dividend, divisor)`. It SHALL return the same type as the inputs (after coercion). Null values SHALL be propagated. Division by zero SHALL return an error.

#### Scenario: MOD of integers
- **WHEN** `MOD(10, 3)` is evaluated
- **THEN** it returns `1`

#### Scenario: MOD of floats
- **WHEN** `MOD(10.5, 3.0)` is evaluated
- **THEN** it returns `1.5`

#### Scenario: MOD with null
- **WHEN** `MOD(NULL, 3)` is evaluated
- **THEN** it returns `NULL`

#### Scenario: MOD by zero
- **WHEN** `MOD(10, 0)` is evaluated
- **THEN** it returns `Err(ExecutionError::InvalidOperation(...))` indicating division by zero

### Requirement: POWER
The system SHALL implement `PowerFunction` that raises a base to an exponent. It SHALL accept exactly two numeric arguments: `POWER(base, exponent)`. It SHALL return Float64. Null values SHALL be propagated.

#### Scenario: POWER of integers
- **WHEN** `POWER(2, 10)` is evaluated
- **THEN** it returns `1024.0` (Float64)

#### Scenario: POWER with fractional exponent
- **WHEN** `POWER(9.0, 0.5)` is evaluated
- **THEN** it returns `3.0` (Float64)

#### Scenario: POWER with null
- **WHEN** `POWER(NULL, 2)` is evaluated
- **THEN** it returns `NULL`
