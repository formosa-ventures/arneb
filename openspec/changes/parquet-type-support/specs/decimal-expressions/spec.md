## ADDED Requirements

### Requirement: Decimal128 literal arrays
The expression evaluator SHALL convert Decimal128 scalar values to Arrow Decimal128Array for use in expressions. Currently `crates/execution/src/expression.rs` (line ~193) returns "decimal literal arrays not yet supported".

#### Scenario: Decimal literal in SELECT
- **WHEN** a query contains `SELECT CAST(100.50 AS DECIMAL(10,2))`
- **THEN** the expression evaluator produces a Decimal128Array with the correct value

#### Scenario: Decimal literal in CASE
- **WHEN** a CASE expression returns a Decimal128 literal
- **THEN** the literal is correctly converted to a Decimal128Array

### Requirement: Decimal128 comparisons
The `compare_op` function in `expression.rs` SHALL support Decimal128 comparisons (=, !=, <, <=, >, >=) using Arrow compute kernels.

#### Scenario: WHERE on Decimal column
- **WHEN** `SELECT * FROM t WHERE price > 100.00` is executed and `price` is Decimal128(10,2)
- **THEN** the comparison produces correct boolean results

#### Scenario: Decimal equality
- **WHEN** two Decimal128 values with different scales are compared
- **THEN** Arrow handles scale alignment and comparison is correct

### Requirement: Decimal128 arithmetic
The `arithmetic_op` function in `expression.rs` SHALL support Decimal128 arithmetic (+, -, *, /) using Arrow compute kernels.

#### Scenario: TPC-H revenue calculation
- **WHEN** `SELECT l_extendedprice * (1 - l_discount)` is executed with Decimal128 columns
- **THEN** the multiplication and subtraction produce correct Decimal128 results

#### Scenario: Decimal-Integer mixed arithmetic
- **WHEN** a Decimal128 column is multiplied by an Int64 value
- **THEN** type coercion produces a Decimal128 result

### Requirement: Decimal128 aggregates
The SUM, AVG, MIN, and MAX accumulators in `aggregate.rs` SHALL support Decimal128 input.

#### Scenario: SUM of Decimal column
- **WHEN** `SELECT SUM(amount) FROM orders` is executed with Decimal128 `amount` column
- **THEN** the sum is computed correctly as Decimal128 preserving precision

#### Scenario: AVG of Decimal column
- **WHEN** `SELECT AVG(price) FROM items` is executed with Decimal128 `price` column
- **THEN** the average is computed as Decimal128

#### Scenario: MIN/MAX of Decimal column
- **WHEN** `SELECT MIN(price), MAX(price) FROM items` is executed
- **THEN** correct minimum and maximum Decimal128 values are returned
