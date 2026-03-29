# Spec: SQL AST (Advanced DQL)

## MODIFIED Requirements

### Requirement: Handle WITH clause in AST conversion
The AST converter SHALL process WITH clauses and produce CTE definition structures alongside the main query AST.

#### Scenario: WITH clause present
- **WHEN** the sqlparser produces a Query with a WITH clause containing CTE definitions
- **THEN** the AST converter maps each CTE to a named subquery structure in the internal AST.

#### Scenario: No WITH clause
- **WHEN** the sqlparser produces a Query without a WITH clause
- **THEN** the AST converter proceeds as before with no CTE structures.

### Requirement: Handle set operations in AST conversion
The AST converter SHALL recognize UNION, UNION ALL, INTERSECT, and EXCEPT in the sqlparser output and produce corresponding SetOperation AST nodes.

#### Scenario: UNION ALL in AST
- **WHEN** the sqlparser produces a SetExpr with UNION ALL operator
- **THEN** the AST converter produces a `SetOperation { op: UnionAll, left, right }` node.

#### Scenario: Nested set operations
- **WHEN** the sqlparser produces nested set operations (e.g., UNION ALL within EXCEPT)
- **THEN** the AST converter preserves the nesting structure.

### Requirement: Handle window function expressions in AST conversion
The AST converter SHALL recognize window function call syntax and produce WindowFunction expression nodes.

#### Scenario: Window function in SELECT item
- **WHEN** the sqlparser produces a function call with an OVER clause
- **THEN** the AST converter produces a `WindowFunction` expression node with the function name, arguments, partition-by keys, and order-by keys.

### Requirement: Preserve backward compatibility
All existing AST conversion functionality MUST continue to work unchanged.

#### Scenario: Simple SELECT without new features
- **WHEN** a query uses none of the new syntax (no WITH, no UNION, no window functions)
- **THEN** the AST converter produces the same output as before this change.
