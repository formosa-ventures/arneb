## MODIFIED Requirements

### Requirement: Memory connector supports filter pushdown
The memory connector SHALL support basic filter pushdown by applying simple comparison predicates during scan.

#### Scenario: Memory scan with equality filter
- **WHEN** a memory DataSource receives a ScanContext with a filter predicate column_0 = 42
- **THEN** it SHALL return only rows where column_0 equals 42
