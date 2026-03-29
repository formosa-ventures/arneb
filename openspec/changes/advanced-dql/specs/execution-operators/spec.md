# Spec: Execution Operators (Advanced DQL)

## MODIFIED Requirements

### Requirement: UnionAllExec operator
The execution engine SHALL provide a `UnionAllExec` operator that concatenates record batches from all child operators in order.

#### Scenario: Two inputs
- **WHEN** UnionAllExec has two child operators producing 3 and 2 batches respectively
- **THEN** the output contains all 5 batches in order (left first, then right).

#### Scenario: Empty input
- **WHEN** one child operator produces zero batches
- **THEN** the output contains only the batches from the other child.

### Requirement: IntersectExec operator
The execution engine SHALL provide an `IntersectExec` operator that returns rows appearing in both inputs.

#### Scenario: Common rows returned
- **WHEN** left produces `{(1,"a"), (2,"b")}` and right produces `{(2,"b"), (3,"c")}`
- **THEN** the output is `{(2,"b")}`.

#### Scenario: No common rows
- **WHEN** left and right have no rows in common
- **THEN** the output is empty.

### Requirement: ExceptExec operator
The execution engine SHALL provide an `ExceptExec` operator that returns rows from the left input not present in the right input.

#### Scenario: Difference computed
- **WHEN** left produces `{(1,"a"), (2,"b"), (3,"c")}` and right produces `{(2,"b")}`
- **THEN** the output is `{(1,"a"), (3,"c")}`.

### Requirement: WindowExec operator
The execution engine SHALL provide a `WindowExec` operator that computes window function results.

#### Scenario: ROW_NUMBER computation
- **WHEN** WindowExec is configured with ROW_NUMBER() partitioned by `dept` ordered by `salary`
- **THEN** each output row has an additional column with the correct row number within its partition.

#### Scenario: Running SUM computation
- **WHEN** WindowExec is configured with `SUM(amount) OVER (PARTITION BY region ORDER BY date)`
- **THEN** each output row has an additional column with the running sum up to that row within its partition.

### Requirement: CTE materialization in execution
The execution engine SHALL materialize a CTE subplan on first access and serve subsequent accesses from the cached result.

#### Scenario: CTE accessed twice
- **WHEN** a CTE is referenced twice in a query (e.g., in a self-join)
- **THEN** the subplan executes once and both references read from the cached `Vec<RecordBatch>`.

### Requirement: Preserve existing execution behavior
All existing operators (Scan, Filter, Project, Join, Aggregate, Sort, Limit, Explain) MUST continue to work unchanged.

#### Scenario: Query without new operators
- **WHEN** a query uses none of the new operators
- **THEN** execution produces the same results as before this change.
