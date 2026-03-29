# Spec: CTE Support

## ADDED Requirements

### Requirement: Parse WITH clause
The SQL parser SHALL recognize WITH clause syntax and produce CTE definition nodes in the AST.

#### Scenario: Single CTE
- **WHEN** a query begins with `WITH sales AS (SELECT region, SUM(amount) FROM orders GROUP BY region)`
- **THEN** the parser produces a CTE definition with name "sales" and the subquery AST.

#### Scenario: Multiple CTEs
- **WHEN** a query contains `WITH a AS (SELECT ...), b AS (SELECT ... FROM a)`
- **THEN** the parser produces two CTE definitions in order, where "b" may reference "a".

### Requirement: Plan CTE as materialized inline view
The query planner SHALL register each CTE and resolve references to CTE names in FROM clauses to the registered plan.

#### Scenario: CTE referenced once
- **WHEN** a CTE "regional_sales" is defined and referenced once in the main query
- **THEN** the planner resolves the reference to the CTE's logical plan.

#### Scenario: CTE referenced multiple times
- **WHEN** a CTE "totals" is referenced twice in the main query (e.g., in a self-join)
- **THEN** both references resolve to the same CTE plan, and execution materializes it once.

#### Scenario: CTE referencing another CTE
- **WHEN** CTE "b" references CTE "a" in its definition
- **THEN** the planner resolves "a" within "b"'s plan correctly.

### Requirement: Execute CTE with materialization
The execution engine SHALL execute a CTE subplan once and cache the result for subsequent references.

#### Scenario: CTE used in self-join
- **WHEN** a query joins a CTE with itself (`FROM t1 JOIN t1 ON ...`)
- **THEN** the CTE subplan executes once, and both sides of the join read from the cached result.

### Requirement: CTE scope
A CTE SHALL only be visible within the query in which it is defined.

#### Scenario: CTE not visible in separate query
- **WHEN** a CTE "tmp" is defined in one query and a subsequent query references "tmp"
- **THEN** the second query SHALL fail with a "table not found" error.

### Requirement: CTE column naming
The CTE SHALL support optional column aliasing.

#### Scenario: CTE with column aliases
- **WHEN** a CTE is defined as `WITH t(x, y) AS (SELECT a, b FROM ...)`
- **THEN** the CTE's output schema uses column names "x" and "y".
