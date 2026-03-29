## MODIFIED Requirements

### Requirement: Physical planner selects join strategy
The physical planner SHALL analyze join conditions and select the appropriate join operator: HashJoinExec for equi-join conditions, NestedLoopJoinExec for non-equi conditions.

#### Scenario: Planning equi-join
- **WHEN** a logical Join node has an equi-join condition
- **THEN** the physical planner SHALL produce a HashJoinExec

#### Scenario: Planning non-equi join
- **WHEN** a logical Join node has a non-equality condition
- **THEN** the physical planner SHALL produce a NestedLoopJoinExec
