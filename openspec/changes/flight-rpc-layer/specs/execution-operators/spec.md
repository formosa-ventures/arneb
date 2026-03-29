## MODIFIED Requirements

### Requirement: ExchangeOperator in operator set
The system SHALL add `ExchangeOperator` to the set of available physical operators in the execution engine. The `ExchangeOperator` SHALL implement the existing `ExecutionPlan` trait and be usable wherever other operators (ScanExec, FilterExec, etc.) are used.

#### Scenario: ExchangeOperator as plan node
- **WHEN** a physical plan contains an `ExchangeOperator` as a leaf node
- **AND** the plan is executed
- **THEN** the `ExchangeOperator` fetches data from remote sources and feeds it to upstream operators

### Requirement: ExecutionContext awareness
The system SHALL ensure `ExecutionContext` can construct and execute plans containing `ExchangeOperator` nodes. No changes to `ExecutionContext` internals are required — the operator works through the existing `ExecutionPlan` trait interface.

#### Scenario: Mixed local and remote operators
- **WHEN** a plan has `FilterExec` → `ExchangeOperator` (filter on top of exchange)
- **AND** the plan is executed
- **THEN** `ExchangeOperator` fetches remote data, and `FilterExec` applies filtering on the received batches
