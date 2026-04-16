## ADDED Requirements

### Requirement: Binary literal array conversion
The expression evaluator SHALL convert Binary scalar values to Arrow BinaryArray. Currently `crates/execution/src/expression.rs` (line ~190) returns "binary literal arrays not yet supported".

#### Scenario: Binary literal in CASE expression
- **WHEN** a CASE expression returns a Binary literal value
- **THEN** the literal is correctly converted to a BinaryArray

#### Scenario: Binary column in SELECT
- **WHEN** a Parquet file contains a Binary column and it is included in SELECT
- **THEN** the column is read and returned without errors
