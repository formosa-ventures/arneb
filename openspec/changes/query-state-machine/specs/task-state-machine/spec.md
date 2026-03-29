## ADDED Requirements

### Requirement: TaskState enum
The system SHALL define a `TaskState` enum with variants: `Planned`, `Running`, `Flushing`, `Finished`, `Failed`, `Cancelled`. The enum SHALL derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`.

### Requirement: TaskStateMachine struct
The system SHALL implement a `TaskStateMachine` struct containing a task identifier, current `TaskState`, transition timestamps, and an optional error message.

### Requirement: Valid task transitions
The system SHALL enforce the following valid transitions:
- `Planned` → `Running`
- `Running` → `Flushing`
- `Flushing` → `Finished`
Any transition not in this list (excluding `fail()` and `cancel()`) SHALL return `Err`.

#### Scenario: Valid running transition
- **WHEN** a `TaskStateMachine` in `Planned` state transitions to `Running`
- **THEN** the transition succeeds and the current state is `Running`

#### Scenario: Invalid backward transition
- **WHEN** a `TaskStateMachine` in `Running` state attempts to transition to `Planned`
- **THEN** the transition returns `Err` and the state remains `Running`

### Requirement: Fail from any active state
The system SHALL implement a `fail(error: String)` method that transitions to `Failed` from any state except `Finished`, `Failed`, or `Cancelled`.

#### Scenario: Failing a flushing task
- **WHEN** `fail("io error")` is called on a `Flushing` task
- **THEN** the state becomes `Failed` and the error message is "io error"

### Requirement: Cancel from any active state
The system SHALL implement a `cancel()` method that transitions to `Cancelled` from any state except `Finished`, `Failed`, or `Cancelled`.

### Requirement: Terminal state detection
The system SHALL implement an `is_terminal()` method returning `true` for `Finished`, `Failed`, and `Cancelled` states.
