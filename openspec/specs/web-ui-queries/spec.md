## Requirements

### Requirement: Query list page with filtering
The system SHALL display a paginated table of all queries on the Queries page. The page SHALL provide a state filter dropdown with options: All, Running, Queued, Finished, Failed, and Cancelled. Selecting a filter SHALL update the displayed queries by passing the `state` query parameter to `GET /api/v1/queries`. The table SHALL show columns: Query ID (truncated), SQL (truncated), State (badge), and a Cancel action button for running/queued queries.

#### Scenario: View all queries
- **WHEN** the user navigates to the Queries page with "All" filter selected
- **THEN** the table displays all queries from the API

#### Scenario: Filter by running state
- **WHEN** the user selects "Running" from the state filter dropdown
- **THEN** the table displays only queries with state "Running"

#### Scenario: Cancel a running query
- **WHEN** the user clicks the Cancel button on a running query
- **THEN** the system sends `DELETE /api/v1/queries/{id}` and refreshes the query list

#### Scenario: Cancel button visibility
- **WHEN** a query has state "Finished", "Failed", or "Cancelled"
- **THEN** no Cancel button is displayed for that row

#### Scenario: Empty filtered results
- **WHEN** the user selects a state filter that matches no queries
- **THEN** the table displays an empty state message: "No queries match this filter"

### Requirement: Query detail view
The system SHALL display a detail view at `/queries/:id` showing full information for a single query. The view SHALL display: full Query ID, current state (as a badge), full SQL text with syntax highlighting, and error message (if state is Failed). The SQL display SHALL use a monospace font with a copy-to-clipboard button.

#### Scenario: View running query detail
- **WHEN** the user navigates to `/queries/{id}` for a running query
- **THEN** the view displays the full query ID, "Running" state badge, and the complete SQL text with syntax highlighting

#### Scenario: View failed query detail
- **WHEN** the user navigates to `/queries/{id}` for a failed query
- **THEN** the view displays the state badge as "Failed" and shows the error message in a distinct error panel

#### Scenario: Copy SQL to clipboard
- **WHEN** the user clicks the copy button next to the SQL display
- **THEN** the full SQL text is copied to the clipboard and a brief "Copied!" confirmation appears

#### Scenario: Query not found
- **WHEN** the user navigates to `/queries/{id}` with an ID that does not exist
- **THEN** the view displays a "Query not found" message with a link back to the queries list

### Requirement: SQL syntax highlighting
The system SHALL render SQL text with syntax highlighting using a lightweight client-side highlighter. SQL keywords (SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, etc.) SHALL be visually distinct from identifiers, string literals, and numeric literals.

#### Scenario: SQL with keywords and identifiers
- **WHEN** the SQL text is `SELECT l_orderkey, SUM(l_extendedprice) FROM lineitem WHERE l_shipdate > '1995-01-01' GROUP BY l_orderkey`
- **THEN** keywords (SELECT, SUM, FROM, WHERE, GROUP BY) render in a distinct color from table/column names and string literals
