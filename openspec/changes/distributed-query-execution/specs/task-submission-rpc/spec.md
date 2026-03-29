## ADDED Requirements

### Requirement: submit_task Flight RPC action
The system SHALL implement a `submit_task` Flight action that the coordinator uses to send plan fragments to workers. The payload SHALL be a JSON-serialized `TaskDescriptor` containing: task_id, stage_id, query_id, serialized LogicalPlan fragment, output_partitioning scheme, and source exchange addresses.

#### Scenario: Submit scan task to worker
- **WHEN** coordinator sends `submit_task` action with a TableScan fragment to a worker
- **THEN** worker acknowledges the task and begins execution asynchronously

#### Scenario: Submit task with exchange sources
- **WHEN** coordinator sends a join fragment that depends on two source stages
- **THEN** the TaskDescriptor includes exchange source addresses so the worker can fetch input via ExchangeClient

### Requirement: LogicalPlan serialization
The system SHALL support JSON serialization and deserialization of LogicalPlan, PlanExpr, and related types for task submission. The serialized plan SHALL be a complete, self-contained description of the work to execute.

#### Scenario: Round-trip serialization
- **WHEN** a LogicalPlan is serialized to JSON and deserialized back
- **THEN** the resulting plan is functionally equivalent to the original
