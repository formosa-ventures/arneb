## ADDED Requirements

### Requirement: ProjectionPushdown rule pushes column selections into scans
The system SHALL implement a ProjectionPushdown optimization rule that analyzes ProjectionExec operators and pushes the required column indices into the ScanExec's ScanContext.

#### Scenario: Simple projection pushdown
- **WHEN** a ProjectionExec sits directly above a ScanExec and selects a subset of columns
- **THEN** the rule SHALL set ScanContext.projection to the required column indices and remove the ProjectionExec if it becomes a no-op

#### Scenario: Connector does not support projection pushdown
- **WHEN** the connector's capabilities indicate no projection pushdown support
- **THEN** the rule SHALL leave the plan unchanged

### Requirement: FilterPushdown rule pushes predicates into scans
The system SHALL implement a FilterPushdown optimization rule that analyzes FilterExec operators and pushes eligible predicates into the ScanExec's ScanContext.

#### Scenario: Simple filter pushdown
- **WHEN** a FilterExec sits above a ScanExec with a supported predicate (comparison operators)
- **THEN** the rule SHALL move the predicate into ScanContext.filters

#### Scenario: Partial pushdown
- **WHEN** a FilterExec has multiple ANDed predicates and only some are pushdown-eligible
- **THEN** the rule SHALL push eligible predicates to ScanContext and keep remaining predicates in FilterExec
