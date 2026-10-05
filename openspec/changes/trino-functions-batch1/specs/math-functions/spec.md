## ADDED Requirements

### Requirement: Transcendental and trigonometric functions
The system SHALL implement `SQRT`, `CBRT`, `EXP`, `LN`, `LOG2`, `LOG10`, `DEGREES`, `RADIANS`, `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `SINH`, `COSH`, `TANH` (one argument), and `LOG(b, x)`, `ATAN2(y, x)` (two arguments), all returning DOUBLE via Arrow's vectorised `unary`/`binary` kernels. Integer and decimal arguments SHALL be cast to DOUBLE. Out-of-domain inputs SHALL follow IEEE-754 (NaN / ±Infinity) as Trino does.

#### Scenario: Domain behaviour
- **WHEN** `SQRT(-1)` and `LN(0)` are evaluated
- **THEN** they return NaN and -Infinity

#### Scenario: Logarithm with base
- **WHEN** `LOG(2, 8)` is evaluated
- **THEN** it returns `3.0`

### Requirement: Constants and classification
The system SHALL implement nullary `PI()`, `E()`, `NAN()`, `INFINITY()` returning one DOUBLE per input row, and `IS_NAN`, `IS_FINITE`, `IS_INFINITE` returning BOOLEAN.

#### Scenario: Constant fills the batch
- **WHEN** `PI()` is evaluated against a 3-row batch
- **THEN** it returns 3 copies of π

### Requirement: SIGN and TRUNCATE
The system SHALL implement `SIGN(x)` (−1/0/1, NaN for NaN) and `TRUNCATE(x [, n])` (round toward zero, optionally keeping `n` decimals). Both SHALL return BIGINT for integer input and DOUBLE otherwise.

#### Scenario: SIGN of zero
- **WHEN** `SIGN(0.0)` is evaluated
- **THEN** it returns `0.0`

#### Scenario: TRUNCATE with decimals
- **WHEN** `TRUNCATE(3.14159, 2)` is evaluated
- **THEN** it returns `3.14`

### Requirement: RANDOM
The system SHALL implement `RANDOM()` (alias `RAND`) returning a pseudo-random DOUBLE in `[0, 1)` per row, and `RANDOM(n)` returning a BIGINT in `[0, n)`; `n <= 0` SHALL error.

#### Scenario: Per-row values
- **WHEN** `RANDOM()` is evaluated over 1000 rows
- **THEN** all values lie in `[0, 1)` and nearly all are distinct

### Requirement: Alternate spellings
`CEILING` SHALL resolve to `CEIL` and `POW` to `POWER`.

#### Scenario: CEILING alias
- **WHEN** `CEILING(3.2)` is evaluated
- **THEN** it returns `4.0`
