## ADDED Requirements

### Requirement: ScanContext carries pushdown information
The system SHALL define a ScanContext struct in the common crate that carries optional filter predicates, projection column indices, and row limit for pushdown into connectors.

#### Scenario: ScanContext with projection only
- **WHEN** a query selects specific columns from a table
- **THEN** ScanContext SHALL contain a projection field with the indices of the selected columns

#### Scenario: ScanContext with filter
- **WHEN** a query has a WHERE clause with simple comparison predicates
- **THEN** ScanContext SHALL contain a filters field with the filter expressions

#### Scenario: ScanContext with limit
- **WHEN** a query has a LIMIT clause
- **THEN** ScanContext SHALL contain a limit field with the maximum number of rows

#### Scenario: Empty ScanContext
- **WHEN** no pushdown is applicable
- **THEN** ScanContext SHALL have all fields set to None/empty, and the connector SHALL perform a full scan
