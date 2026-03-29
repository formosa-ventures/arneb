## ADDED Requirements

### Requirement: HashJoinExec performs equi-joins using build and probe phases
The system SHALL implement a HashJoinExec operator that performs equi-joins by building a hash table from the build (right) side and probing with the probe (left) side.

#### Scenario: INNER hash join
- **WHEN** HashJoinExec executes an INNER join on key columns
- **THEN** it SHALL return only rows where the join keys match between left and right sides

#### Scenario: LEFT hash join
- **WHEN** HashJoinExec executes a LEFT join
- **THEN** it SHALL return all left rows, with matching right columns filled for matched rows and NULL-filled for unmatched rows

#### Scenario: RIGHT hash join
- **WHEN** HashJoinExec executes a RIGHT join
- **THEN** it SHALL return all right rows, with matching left columns filled for matched rows and NULL-filled for unmatched rows

#### Scenario: FULL hash join
- **WHEN** HashJoinExec executes a FULL join
- **THEN** it SHALL return all rows from both sides, with NULL-filled columns where no match exists

#### Scenario: Multi-column join keys
- **WHEN** the join condition involves multiple equality comparisons (a.col1 = b.col1 AND a.col2 = b.col2)
- **THEN** HashJoinExec SHALL hash composite keys and match on all key columns

#### Scenario: NULL join keys
- **WHEN** a row has NULL in any join key column
- **THEN** that row SHALL NOT match any other row (SQL null semantics)
