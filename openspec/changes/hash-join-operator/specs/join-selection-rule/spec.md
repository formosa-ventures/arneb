## ADDED Requirements

### Requirement: JoinSelection rule chooses optimal join strategy
The system SHALL implement a JoinSelection optimization rule that analyzes join conditions and selects HashJoinExec for equi-joins or NestedLoopJoinExec for non-equi joins.

#### Scenario: Equi-join detected
- **WHEN** a join condition consists of one or more equality comparisons between left and right columns
- **THEN** JoinSelection SHALL replace NestedLoopJoinExec with HashJoinExec

#### Scenario: Non-equi join
- **WHEN** a join condition contains non-equality operators (>, <, LIKE, etc.)
- **THEN** JoinSelection SHALL keep the NestedLoopJoinExec unchanged

#### Scenario: Mixed conditions
- **WHEN** a join condition has both equi and non-equi parts
- **THEN** JoinSelection SHALL use HashJoinExec for the equi parts and apply remaining conditions as a post-filter
