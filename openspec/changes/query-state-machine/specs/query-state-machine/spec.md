## ADDED Requirements

### Requirement: QueryState enum
The system SHALL define a `QueryState` enum with variants: `Queued`, `Planning`, `Starting`, `Running`, `Finishing`, `Finished`, `Failed`, `Cancelled`. The enum SHALL derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`.

### Requirement: QueryStateMachine struct
The system SHALL implement a `QueryStateMachine` struct containing the `QueryId`, current `QueryState`, a `HashMap<QueryState, Instant>` for transition timestamps, an optional error message (for `Failed` state), and a list of associated `StageId`s.

### Requirement: Valid state transitions
The system SHALL enforce the following valid transitions:
- `Queued` → `Planning`
- `Planning` → `Starting`
- `Starting` → `Running`
- `Running` → `Finishing`
- `Finishing` → `Finished`
Any transition not in this list (excluding `fail()` and `cancel()`) SHALL return `Err`.

#### Scenario: Valid forward transition
- **WHEN** a `QueryStateMachine` in `Queued` state transitions to `Planning`
- **THEN** the transition succeeds and the current state is `Planning`
- **AND** the transition timestamp for `Planning` is recorded

#### Scenario: Invalid transition
- **WHEN** a `QueryStateMachine` in `Queued` state attempts to transition to `Running`
- **THEN** the transition returns `Err` with an appropriate error message
- **AND** the state remains `Queued`

### Requirement: Fail from any active state
The system SHALL implement a `fail(error: String)` method that transitions to `Failed` from any state except `Finished`, `Failed`, or `Cancelled`. The error message SHALL be stored in the state machine.

#### Scenario: Failing a running query
- **WHEN** `fail("out of memory")` is called on a `Running` query
- **THEN** the state becomes `Failed` and the error message is "out of memory"

#### Scenario: Failing an already finished query
- **WHEN** `fail("error")` is called on a `Finished` query
- **THEN** the call returns `Err` and the state remains `Finished`

### Requirement: Cancel from any active state
The system SHALL implement a `cancel()` method that transitions to `Cancelled` from any state except `Finished`, `Failed`, or `Cancelled`.

#### Scenario: Cancelling a queued query
- **WHEN** `cancel()` is called on a `Queued` query
- **THEN** the state becomes `Cancelled`

### Requirement: Terminal state detection
The system SHALL implement an `is_terminal()` method returning `true` for `Finished`, `Failed`, and `Cancelled` states.
