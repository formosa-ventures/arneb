# Spec: Query Planner (Advanced DQL)

## MODIFIED Requirements

### Requirement: Plan CTEs as materialized inline views
The query planner SHALL register CTE definitions and resolve CTE name references in FROM clauses to the corresponding logical plan.

#### Scenario: CTE in FROM clause
- **WHEN** the planner encounters `FROM regional_sales` where "regional_sales" is a CTE
- **THEN** the planner resolves the table reference to the CTE's logical plan.

#### Scenario: CTE takes priority over catalog tables
- **WHEN** a CTE name matches an existing catalog table name
- **THEN** the CTE definition takes priority within the scope of that query.

### Requirement: Plan UNION ALL as concatenation
The query planner SHALL transform a UNION ALL set operation into a `UnionAll` logical plan node.

#### Scenario: Two-input UNION ALL
- **WHEN** the planner encounters `SELECT ... UNION ALL SELECT ...`
- **THEN** the logical plan contains a `UnionAll` node with two child plans.

### Requirement: Plan UNION as UNION ALL plus deduplication
The query planner SHALL transform a UNION set operation into a `UnionAll` node followed by a `Distinct` node.

#### Scenario: UNION deduplication
- **WHEN** the planner encounters `SELECT ... UNION SELECT ...`
- **THEN** the logical plan contains a `Distinct` node wrapping a `UnionAll` node.

### Requirement: Plan INTERSECT
The query planner SHALL transform an INTERSECT set operation into an `Intersect` logical plan node.

#### Scenario: INTERSECT plan
- **WHEN** the planner encounters `SELECT ... INTERSECT SELECT ...`
- **THEN** the logical plan contains an `Intersect` node with two child plans.

### Requirement: Plan EXCEPT
The query planner SHALL transform an EXCEPT set operation into an `Except` logical plan node.

#### Scenario: EXCEPT plan
- **WHEN** the planner encounters `SELECT ... EXCEPT SELECT ...`
- **THEN** the logical plan contains an `Except` node with two child plans.

### Requirement: Plan window functions
The query planner SHALL detect window function expressions in the SELECT list and add a `Window` logical plan node.

#### Scenario: Window function planning
- **WHEN** the SELECT list contains `ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary)`
- **THEN** the logical plan includes a `Window` node specifying the function, partition keys, and order keys.

### Requirement: Preserve existing planning behavior
All existing query planning functionality MUST continue to work unchanged.

#### Scenario: Query without advanced DQL features
- **WHEN** a query contains no CTEs, set operations, or window functions
- **THEN** the planner produces the same plan as before this change.
