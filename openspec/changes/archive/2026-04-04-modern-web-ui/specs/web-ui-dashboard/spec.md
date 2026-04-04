## ADDED Requirements

### Requirement: Dashboard statistics cards
The system SHALL display four statistics cards at the top of the Dashboard page: Running Queries (count of queries in RUNNING state), Completed Queries (count in FINISHED state), Failed Queries (count in FAILED state), and Active Workers (count of alive workers). Each card SHALL show a numeric value and a descriptive label. Cards SHALL use distinct visual indicators (color or icon) per metric.

#### Scenario: Dashboard with active queries
- **WHEN** the API returns 3 running, 50 finished, 2 failed queries and 4 alive workers
- **THEN** the dashboard displays cards showing "3" Running, "50" Completed, "2" Failed, and "4" Workers

#### Scenario: Dashboard with no data
- **WHEN** the API returns 0 queries and 0 workers
- **THEN** all stat cards display "0" with their respective labels

#### Scenario: Standalone mode workers card
- **WHEN** the server role is "standalone"
- **THEN** the Workers card displays "Standalone" instead of a count

### Requirement: Recent queries table
The system SHALL display a table of the 10 most recent queries on the Dashboard page. The table SHALL show columns: Query ID (truncated to first 8 characters), SQL (truncated to 80 characters), State (as a colored badge), and Duration. Each row SHALL be clickable to navigate to the query detail view at `/queries/:id`.

#### Scenario: Recent queries display
- **WHEN** the API returns 25 queries
- **THEN** the table displays the 10 most recent queries sorted by recency

#### Scenario: Query state badge colors
- **WHEN** a query has state "Running"
- **THEN** its state badge displays with a blue background
- **WHEN** a query has state "Finished"
- **THEN** its state badge displays with a green background
- **WHEN** a query has state "Failed"
- **THEN** its state badge displays with a red background

#### Scenario: Click query row
- **WHEN** the user clicks a row in the recent queries table
- **THEN** the application navigates to `/queries/{queryId}` showing the query detail view

#### Scenario: Empty queries state
- **WHEN** there are no queries
- **THEN** the table area displays an empty state message: "No queries yet"

### Requirement: Cluster overview summary
The system SHALL display a cluster summary section on the Dashboard showing the server role (Standalone, Coordinator, or Worker) and the number of connected workers. In coordinator mode, the section SHALL show worker count with a health summary (e.g., "4 workers, 3 healthy").

#### Scenario: Coordinator mode summary
- **WHEN** the server role is "coordinator" with 4 workers (3 alive, 1 not alive)
- **THEN** the cluster summary shows "Coordinator" role with "4 workers (3 healthy)"

#### Scenario: Standalone mode summary
- **WHEN** the server role is "standalone"
- **THEN** the cluster summary shows "Standalone" role with no worker count
