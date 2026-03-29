## ADDED Requirements

### Requirement: List queries endpoint
The system SHALL implement `GET /api/v1/queries` returning a JSON array of query summaries. Each summary SHALL include: `query_id`, `state`, `sql` (truncated to 200 characters), `started_at` (ISO 8601), `duration_ms`, and `error` (if failed). The endpoint SHALL accept an optional `state` query parameter to filter by query state.

#### Scenario: List all queries
- **WHEN** `GET /api/v1/queries` is called with no parameters
- **THEN** the response is a JSON array of all tracked queries sorted by start time descending

#### Scenario: Filter by state
- **WHEN** `GET /api/v1/queries?state=RUNNING` is called
- **THEN** the response contains only queries in the RUNNING state

#### Scenario: Empty query list
- **WHEN** `GET /api/v1/queries` is called and no queries have been executed
- **THEN** the response is an empty JSON array `[]`

### Requirement: Query detail endpoint
The system SHALL implement `GET /api/v1/queries/{id}` returning a JSON object with full query details: `query_id`, `state`, `sql` (full text), `started_at`, `duration_ms`, `plan` (formatted execution plan text), `stages` (array of stage info), `error` (if failed).

#### Scenario: Get existing query detail
- **WHEN** `GET /api/v1/queries/20260325_001` is called for an existing query
- **THEN** the response is a JSON object with full query details including the execution plan

#### Scenario: Query not found
- **WHEN** `GET /api/v1/queries/nonexistent` is called
- **THEN** the response is HTTP 404 with a JSON error message `{"error": "query not found"}`

### Requirement: Cancel query endpoint
The system SHALL implement `DELETE /api/v1/queries/{id}` to cancel a running or queued query. Successful cancellation SHALL return HTTP 200 with `{"status": "cancelled"}`. Cancelling a non-cancellable query (completed, failed, already cancelled) SHALL return HTTP 409 with an error message.

#### Scenario: Cancel running query
- **WHEN** `DELETE /api/v1/queries/20260325_001` is called for a running query
- **THEN** the query is cancelled and the response is HTTP 200 `{"status": "cancelled"}`

#### Scenario: Cancel completed query
- **WHEN** `DELETE /api/v1/queries/20260325_002` is called for a completed query
- **THEN** the response is HTTP 409 `{"error": "query is not cancellable in state COMPLETED"}`

### Requirement: Cluster overview endpoint
The system SHALL implement `GET /api/v1/cluster` returning a JSON object with: `coordinator` (id, address, role, uptime_seconds), `worker_count`, `total_active_tasks`.

#### Scenario: Cluster with workers
- **WHEN** `GET /api/v1/cluster` is called with 3 registered workers running 7 total tasks
- **THEN** the response includes `"worker_count": 3` and `"total_active_tasks": 7`

### Requirement: Worker list endpoint
The system SHALL implement `GET /api/v1/cluster/workers` returning a JSON array of worker details: `worker_id`, `address`, `status` (active/inactive), `active_tasks`, `last_heartbeat` (ISO 8601).

#### Scenario: List workers
- **WHEN** `GET /api/v1/cluster/workers` is called
- **THEN** the response is a JSON array with one entry per registered worker

### Requirement: Server info endpoint
The system SHALL implement `GET /api/v1/info` returning a JSON object with: `name` ("trino-alt"), `version`, `uptime_seconds`, `role` (coordinator/worker/standalone).

#### Scenario: Server info
- **WHEN** `GET /api/v1/info` is called
- **THEN** the response includes `"name": "trino-alt"` and the current server role and uptime
