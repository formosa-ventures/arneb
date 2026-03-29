## ADDED Requirements

### Requirement: Dashboard page
The system SHALL serve a dashboard page at the root URL (`/`) that displays a summary of query activity and cluster health. The page SHALL include navigation links to Dashboard, Queries, and Cluster views.

#### Scenario: Dashboard loads
- **WHEN** a browser navigates to `/`
- **THEN** the page displays running, completed, and failed query counts
- **AND** a cluster health summary showing the number of active workers

#### Scenario: Navigation links
- **WHEN** the dashboard page is loaded
- **THEN** it contains navigation links to the Dashboard, Queries, and Cluster views

### Requirement: Auto-refresh
The dashboard SHALL automatically refresh query data by polling `GET /api/v1/queries` every 2 seconds using JavaScript `fetch()`. The UI SHALL update in place without full page reloads.

#### Scenario: Live query count update
- **WHEN** a new query starts while the dashboard is open
- **THEN** the running query count updates within 2 seconds without manual page refresh

#### Scenario: Query completion reflected
- **WHEN** a running query completes while the dashboard is open
- **THEN** the running count decreases and the completed count increases within 2 seconds

### Requirement: Query summary cards
The dashboard SHALL display summary cards showing: total queries, running queries, completed queries, and failed queries. Each card SHALL show the count and be visually distinct (color-coded).

#### Scenario: Summary with mixed states
- **WHEN** there are 3 running, 10 completed, and 2 failed queries
- **THEN** the dashboard shows cards: Total=15, Running=3, Completed=10, Failed=2
