## MODIFIED Requirements

### Requirement: Execution operators include HashJoinExec
The execution engine SHALL include HashJoinExec as a physical operator for performing equi-joins using hash-based build and probe phases, in addition to the existing NestedLoopJoinExec.

#### Scenario: Physical plan with hash join
- **WHEN** a query contains an equi-join
- **THEN** the physical plan SHALL use HashJoinExec instead of NestedLoopJoinExec
