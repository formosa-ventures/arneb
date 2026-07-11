## ADDED Requirements

### Requirement: Smaller side is built for reorderable INNER-join chains

For a reorderable INNER-join chain, the planner SHALL arrange the physical plan so that each
`HashJoinExec` builds its hash table from the input with the smaller estimated cardinality (the
join's right/build child), per the Selinger cost model.

#### Scenario: Two-table INNER join builds the smaller side
- **WHEN** an INNER join combines a large fact input and a small filtered dimension input
- **THEN** the resulting `HashJoinExec` builds from the small input and probes with the large input

#### Scenario: Multi-table left-deep chain keeps the fact input on the probe spine
- **WHEN** a multi-table INNER chain joins one large fact input against several smaller inputs
- **THEN** the large input stays on the probe (left) spine and each smaller input is a build (right)
  child, so no hash table is built from the large input

### Requirement: Build-side selection is robust to self-joins

Build-side selection SHALL apply even when the join chain contains a self-join (two leaves sharing
the same column names). The planner SHALL NOT fall back to the original SQL join order solely
because two leaves have duplicate column names.

#### Scenario: Self-join chain still builds the smaller side
- **WHEN** a reorderable INNER chain contains two leaves with identical column names (e.g. a table
  joined to itself under two aliases) alongside a large fact input
- **THEN** the planner still places the large fact input on the probe spine and builds from the
  smaller inputs — it does not keep the SQL order that would build the large input

#### Scenario: Column references resolve to the correct leaf after a self-join reorder
- **WHEN** a chain containing a self-join is reordered and column indices are rebuilt
- **THEN** every column reference in join conditions, filters, and projections resolves to the
  intended leaf's column (no cross-leaf index mismatch)

### Requirement: Reordering preserves query results

Any build-side / ordering rewrite SHALL be result-preserving: the query output SHALL be identical to
the un-rewritten plan's output.

#### Scenario: Self-join query is cell-correct after the rewrite
- **WHEN** a query whose chain contains a self-join is executed with build-side selection applied
- **THEN** its result is cell-for-cell identical (within float tolerance) to the reference engine's
  result

#### Scenario: Non-self-join behavior is unchanged
- **WHEN** a chain with no duplicate leaf column names is planned
- **THEN** its build-side selection and column indices are identical to the prior behavior
