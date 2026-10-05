## ADDED Requirements

### Requirement: Lowering of function special syntax
The SQL-to-AST conversion SHALL lower sqlparser's dedicated expression nodes to ordinary function calls:
- `CEIL(x)` / `FLOOR(x)` → `CEIL(x)` / `FLOOR(x)` (the `TO <unit>` and scale forms stay unsupported),
- `POSITION(a IN b)` → `POSITION(a, b)`,
- `TRIM([BOTH|LEADING|TRAILING] [chars FROM] s)` and `TRIM(s, chars)` → `TRIM` / `LTRIM` / `RTRIM(s [, chars])`,
- `a || b` → `CONCAT(a, b)`.

#### Scenario: CEIL syntax
- **WHEN** `SELECT CEIL(x) FROM t` is parsed
- **THEN** the projection is `Function { name: "CEIL", args: [x] }`

#### Scenario: TRIM LEADING
- **WHEN** `SELECT TRIM(LEADING 'x' FROM s) FROM t` is parsed
- **THEN** the projection is `Function { name: "LTRIM", args: [s, 'x'] }`

#### Scenario: String concatenation operator
- **WHEN** `SELECT a || 'b' FROM t` is parsed
- **THEN** the projection is `Function { name: "CONCAT", args: [a, 'b'] }`

### Requirement: IF desugaring
The conversion SHALL desugar `IF(cond, a [, b])` to `CASE WHEN cond THEN a [ELSE b] END` and SHALL reject other arities with `ParseError::InvalidSyntax`.

#### Scenario: IF with one argument
- **WHEN** `SELECT IF(a) FROM t` is parsed
- **THEN** parsing fails
