# Spec: Window Functions

## ADDED Requirements

### Requirement: Parse window function syntax
The SQL parser SHALL recognize window function expressions with OVER clause containing PARTITION BY and ORDER BY.

#### Scenario: ROW_NUMBER with PARTITION BY
- **WHEN** a query contains `ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)`
- **THEN** the parser produces a `WindowFunction` AST node with function `ROW_NUMBER`, partition key `dept`, and order key `salary DESC`.

#### Scenario: Aggregate window function
- **WHEN** a query contains `SUM(amount) OVER (PARTITION BY region)`
- **THEN** the parser produces a `WindowFunction` AST node with function `SUM`, argument `amount`, and partition key `region`.

### Requirement: Plan window functions
The query planner SHALL add a `Window` logical plan node for queries containing window function expressions.

#### Scenario: Window function in SELECT
- **WHEN** the SELECT list contains a window function expression
- **THEN** the logical plan includes a `Window` node above the input plan, specifying the window function, partition keys, and order keys.

#### Scenario: Multiple window functions
- **WHEN** the SELECT list contains multiple window functions with the same OVER clause
- **THEN** the planner groups them into a single `Window` node.

### Requirement: Execute ROW_NUMBER
The window executor SHALL compute ROW_NUMBER() as a sequential integer starting at 1 within each partition.

#### Scenario: ROW_NUMBER within partitions
- **WHEN** data is partitioned by `dept` with 3 rows in dept A and 2 rows in dept B, ordered by salary
- **THEN** dept A rows get ROW_NUMBER 1, 2, 3 and dept B rows get 1, 2.

### Requirement: Execute RANK
The window executor SHALL compute RANK() as the position within the partition, with ties receiving the same rank and gaps after ties.

#### Scenario: RANK with ties
- **WHEN** three rows have ORDER BY values 10, 10, 20
- **THEN** RANK() returns 1, 1, 3.

### Requirement: Execute DENSE_RANK
The window executor SHALL compute DENSE_RANK() as the position within the partition, with ties receiving the same rank and no gaps.

#### Scenario: DENSE_RANK with ties
- **WHEN** three rows have ORDER BY values 10, 10, 20
- **THEN** DENSE_RANK() returns 1, 1, 2.

### Requirement: Execute aggregate window functions
The window executor SHALL compute aggregate window functions (SUM, AVG, COUNT, MIN, MAX) over the partition.

#### Scenario: SUM OVER partition
- **WHEN** data is partitioned by `region` and `SUM(amount) OVER (PARTITION BY region)` is computed
- **THEN** each row's window SUM equals the total amount for its partition.

#### Scenario: SUM OVER with ORDER BY (running total)
- **WHEN** `SUM(amount) OVER (PARTITION BY region ORDER BY date)` is computed
- **THEN** each row's window SUM equals the running total of amount up to and including that row within its partition.

### Requirement: Window function result as additional column
Window function results SHALL be appended as new columns to the output schema.

#### Scenario: Original columns preserved
- **WHEN** a query selects `name, dept, ROW_NUMBER() OVER (...) AS rn`
- **THEN** the output contains columns `name`, `dept`, and `rn`.
