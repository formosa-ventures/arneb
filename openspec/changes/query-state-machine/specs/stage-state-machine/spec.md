## ADDED Requirements

### Requirement: StageState enum
The system SHALL define a `StageState` enum with variants: `Planned`, `Scheduling`, `Running`, `Flushing`, `Finished`, `Failed`, `Cancelled`. The enum SHALL derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`.

### Requirement: StageStateMachine struct
The system SHALL implement a `StageStateMachine` struct containing a stage identifier, current `StageState`, transition timestamps, and an optional error message.

### Requirement: Valid stage transitions
The system SHALL enforce the following valid transitions:
- `Planned` → `Scheduling`
- `Scheduling` → `Running`
- `Running` → `Flushing`
- `Flushing` → `Finished`
Any transition not in this list (excluding `fail()` and `cancel()`) SHALL return `Err`.

#### Scenario: Valid scheduling transition
- **WHEN** a `StageStateMachine` in `Planned` state transitions to `Scheduling`
- **THEN** the transition succeeds and the current state is `Scheduling`

#### Scenario: Invalid skip transition
- **WHEN** a `StageStateMachine` in `Planned` state attempts to transition to `Running`
- **THEN** the transition returns `Err` and the state remains `Planned`

### Requirement: Fail from any active state
The system SHALL implement a `fail(error: String)` method that transitions to `Failed` from any state except `Finished`, `Failed`, or `Cancelled`.

#### Scenario: Failing a running stage
- **WHEN** `fail("task failure")` is called on a `Running` stage
- **THEN** the state becomes `Failed`

### Requirement: Cancel from any active state
The system SHALL implement a `cancel()` method that transitions to `Cancelled` from any state except `Finished`, `Failed`, or `Cancelled`.

### Requirement: Terminal state detection
The system SHALL implement an `is_terminal()` method returning `true` for `Finished`, `Failed`, and `Cancelled` states.
