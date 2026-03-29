## MODIFIED Requirements

### Requirement: Query handler routes to distributed execution
The protocol handler's `execute_query` SHALL check for alive workers in the NodeRegistry after planning. If workers are available and the plan fragments into multiple stages, it SHALL delegate to `QueryCoordinator::execute()` for distributed execution. Single-fragment queries and queries with no available workers SHALL continue to use the local execution path.

#### Scenario: Distributed query routing
- **WHEN** a query arrives and 2 workers are registered and the plan has 3 fragments
- **THEN** execute_query invokes QueryCoordinator instead of local execution

#### Scenario: Fallback to local
- **WHEN** a query arrives and no workers are registered
- **THEN** execute_query uses the existing local path (no regression)
