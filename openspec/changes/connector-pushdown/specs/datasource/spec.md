## MODIFIED Requirements

### Requirement: DataSource scan accepts ScanContext
The DataSource trait's scan method SHALL accept a ScanContext parameter containing optional filter predicates, projection column indices, and row limit. Connectors that support pushdown SHALL use these hints to reduce I/O.

#### Scenario: Scan with ScanContext containing projection
- **WHEN** scan is called with a ScanContext that has projection indices set
- **THEN** the DataSource SHALL return a stream containing only the projected columns

#### Scenario: Scan with empty ScanContext
- **WHEN** scan is called with a ScanContext that has all fields empty/None
- **THEN** the DataSource SHALL perform a full scan returning all columns and rows

#### Scenario: Scan with unsupported pushdown
- **WHEN** scan is called with a ScanContext containing filter predicates but the DataSource does not support filter pushdown
- **THEN** the DataSource SHALL ignore the filters and perform a full scan (the caller is responsible for applying filters)
