## ADDED Requirements

### Requirement: Query list view
The system SHALL provide a query list view displaying all queries in a table format. Columns SHALL include: Query ID, SQL preview (first 100 characters), State, Duration, and Started timestamp. The list SHALL support filtering by query state.

#### Scenario: Viewing all queries
- **WHEN** a user navigates to the Queries view
- **THEN** a table lists all queries with ID, SQL preview, state, duration, and start time

#### Scenario: Filtering by state
- **WHEN** a user selects the "RUNNING" filter
- **THEN** only queries in the RUNNING state are displayed

### Requirement: Query detail view
The system SHALL provide a query detail view accessible by clicking a query ID. The detail view SHALL display: full SQL text, current query state, execution plan (formatted text), stage progress bars (if available), task table (worker, state, rows processed, duration), and timing breakdown (queued, planning, running durations).

#### Scenario: Viewing query detail
- **WHEN** a user clicks on query ID `20260325_001`
- **THEN** the detail page shows the full SQL text, state, plan, and per-stage progress

#### Scenario: Running query progress
- **WHEN** viewing a running query with 3 stages
- **THEN** each stage shows a progress bar indicating percentage complete

### Requirement: Query cancellation
The system SHALL provide a cancel button on the query detail view for queries in RUNNING or QUEUED state. Clicking cancel SHALL send `DELETE /api/v1/queries/{id}` and update the UI to reflect the cancelled state.

#### Scenario: Cancel a running query
- **WHEN** a user clicks the cancel button on a running query
- **THEN** the system sends a DELETE request to cancel the query
- **AND** the query state updates to CANCELLED in the UI

#### Scenario: Cancel button visibility
- **WHEN** viewing a completed or failed query
- **THEN** the cancel button is not displayed
