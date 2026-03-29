## ADDED Requirements

### Requirement: TaskManager receives and executes tasks
The system SHALL provide a `TaskManager` on the worker side that handles `submit_task` RPC calls. It SHALL create an ExecutionContext, register data sources, create a physical plan from the received LogicalPlan fragment, execute it, and write output to an OutputBuffer registered with FlightState.

#### Scenario: Execute scan task
- **WHEN** worker receives a `submit_task` with a TableScan fragment
- **THEN** TaskManager creates ExecutionContext, registers the table's DataSource, executes the scan, writes output batches to OutputBuffer, and marks the task as finished

#### Scenario: Execute task with exchange input
- **WHEN** worker receives a task that includes ExchangeNode sources
- **THEN** TaskManager creates ExchangeExec operators for each source, connecting them to remote workers via ExchangeClient

#### Scenario: Task execution failure
- **WHEN** a task fails during execution (e.g., data source error)
- **THEN** TaskManager marks the task as failed and the error is propagated to the coordinator
