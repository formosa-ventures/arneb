# Spec: Set Operations

## ADDED Requirements

### Requirement: Parse set operation syntax
The SQL parser SHALL recognize UNION ALL, UNION, INTERSECT, and EXCEPT between SELECT statements.

#### Scenario: UNION ALL
- **WHEN** a query contains `SELECT ... UNION ALL SELECT ...`
- **THEN** the parser produces a `SetOperation` AST node with operator `UnionAll`.

#### Scenario: UNION DISTINCT
- **WHEN** a query contains `SELECT ... UNION SELECT ...` (or `UNION DISTINCT`)
- **THEN** the parser produces a `SetOperation` AST node with operator `Union`.

#### Scenario: INTERSECT
- **WHEN** a query contains `SELECT ... INTERSECT SELECT ...`
- **THEN** the parser produces a `SetOperation` AST node with operator `Intersect`.

#### Scenario: EXCEPT
- **WHEN** a query contains `SELECT ... EXCEPT SELECT ...`
- **THEN** the parser produces a `SetOperation` AST node with operator `Except`.

### Requirement: Schema compatibility for set operations
All inputs to a set operation MUST have the same number of columns with compatible types.

#### Scenario: Matching schemas
- **WHEN** both sides of a UNION have 3 columns with matching types
- **THEN** the operation proceeds and the output schema uses column names from the first input.

#### Scenario: Mismatched column count
- **WHEN** the left side has 3 columns and the right side has 2 columns
- **THEN** a planning error SHALL be raised.

#### Scenario: Incompatible types
- **WHEN** the left side has a string column and the right side has an integer column in the same position
- **THEN** a planning error SHALL be raised (or implicit casting is applied if supported).

### Requirement: UNION ALL preserves all rows
UNION ALL SHALL concatenate all rows from all inputs without removing duplicates.

#### Scenario: Duplicate rows
- **WHEN** both inputs contain the row `(1, "alice")`
- **THEN** the UNION ALL result contains that row twice.

### Requirement: UNION removes duplicates
UNION SHALL concatenate all rows and then remove exact duplicate rows.

#### Scenario: Duplicate removal
- **WHEN** both inputs contain the row `(1, "alice")`
- **THEN** the UNION result contains that row exactly once.

### Requirement: INTERSECT returns common rows
INTERSECT SHALL return only rows that appear in both inputs, deduplicated.

#### Scenario: Common rows
- **WHEN** left contains `{(1), (2), (3)}` and right contains `{(2), (3), (4)}`
- **THEN** the INTERSECT result is `{(2), (3)}`.

### Requirement: EXCEPT returns difference rows
EXCEPT SHALL return rows from the left input that do not appear in the right input, deduplicated.

#### Scenario: Difference
- **WHEN** left contains `{(1), (2), (3)}` and right contains `{(2), (4)}`
- **THEN** the EXCEPT result is `{(1), (3)}`.

### Requirement: Chained set operations
Multiple set operations SHALL be supported in a single query with left-to-right evaluation.

#### Scenario: Three-way UNION ALL
- **WHEN** a query contains `SELECT ... UNION ALL SELECT ... UNION ALL SELECT ...`
- **THEN** all three result sets are concatenated.
