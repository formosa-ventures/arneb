## ADDED Requirements

### Requirement: Task submission
The system SHALL implement a `TaskManager` that accepts task submissions containing a query_id, stage_id, task_id, and a serialized plan fragment. Each submitted task SHALL be tracked with a `TaskHandle` containing the task status and an OutputBuffer for results.

#### Scenario: Submit a new task
- **WHEN** a TaskRequest is submitted with a valid plan fragment
- **THEN** a TaskHandle is created with status Running
- **AND** the task begins execution asynchronously

#### Scenario: Duplicate task_id
- **WHEN** a TaskRequest is submitted with a task_id that already exists
- **THEN** the submission returns the existing TaskHandle without creating a duplicate

### Requirement: Task execution
The system SHALL execute each submitted task by creating an ExecutionContext, building a physical plan from the plan fragment, calling `execute()` on the plan, and collecting the resulting RecordBatch stream into an OutputBuffer. Execution SHALL run on a tokio task to avoid blocking the RPC handler.

#### Scenario: Successful execution
- **WHEN** a task executes a valid plan fragment that produces 100 rows across 3 batches
- **THEN** the OutputBuffer contains 3 RecordBatches totaling 100 rows
- **AND** the task status transitions to Completed

#### Scenario: Execution error
- **WHEN** a task executes a plan fragment that fails (e.g., missing data source)
- **THEN** the task status transitions to Failed with a descriptive error message
- **AND** the OutputBuffer remains empty

### Requirement: Output buffer access
The system SHALL provide access to a task's OutputBuffer by task_id. The OutputBuffer SHALL be readable by the Flight RPC `do_get` handler to stream results back to the coordinator. The buffer SHALL include the output schema and the collected RecordBatches.

#### Scenario: Read completed task output
- **WHEN** a task has completed and the coordinator requests its output
- **THEN** the OutputBuffer returns the schema and all collected RecordBatches

#### Scenario: Read running task output
- **WHEN** a task is still running and the coordinator requests its output
- **THEN** the system returns an error indicating the task has not completed yet

### Requirement: Task status query
The system SHALL provide a method to query the status of a task by task_id. The status SHALL be one of: Running, Completed, or Failed.

#### Scenario: Query running task
- **WHEN** `get_task_status(task_id)` is called for a running task
- **THEN** it returns `TaskStatus::Running`

#### Scenario: Query unknown task
- **WHEN** `get_task_status(task_id)` is called for a task_id that does not exist
- **THEN** it returns an error indicating the task was not found

### Requirement: Task cleanup
The system SHALL provide a method to remove completed or failed tasks from the TaskManager. This is called after the coordinator has fetched the output or when a query is cancelled.

#### Scenario: Remove completed task
- **WHEN** `remove_task(task_id)` is called for a completed task
- **THEN** the TaskHandle and OutputBuffer are dropped, freeing memory
