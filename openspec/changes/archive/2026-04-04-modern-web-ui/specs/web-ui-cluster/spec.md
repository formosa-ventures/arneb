## ADDED Requirements

### Requirement: Server information display
The system SHALL display server information at the top of the Cluster page including: server version (from `GET /api/v1/info`), uptime formatted as a human-readable duration (e.g., "2h 15m 30s"), and the server role as a badge (Standalone, Coordinator, or Worker).

#### Scenario: Coordinator server info
- **WHEN** the API returns `version: "0.1.0"`, `uptime_secs: 8130`, `role: "coordinator"`
- **THEN** the Cluster page displays version "0.1.0", uptime "2h 15m 30s", and a "Coordinator" role badge

#### Scenario: Uptime formatting
- **WHEN** the uptime is 90 seconds
- **THEN** the display shows "1m 30s"
- **WHEN** the uptime is 86400 seconds
- **THEN** the display shows "1d 0h 0m"

### Requirement: Worker list with health indicators
The system SHALL display a list of workers as cards on the Cluster page, fetched from `GET /api/v1/cluster/workers`. Each worker card SHALL show: worker ID, RPC address, alive/dead status indicator (green dot for alive, red dot for dead), max splits capacity, and time since last heartbeat. Workers SHALL be sorted with alive workers first, then by worker ID.

#### Scenario: Display healthy workers
- **WHEN** the API returns 3 workers all with `alive: true`
- **THEN** the page displays 3 worker cards each with a green status indicator

#### Scenario: Worker with stale heartbeat
- **WHEN** a worker has `alive: false` and `last_heartbeat_secs_ago: 120`
- **THEN** the worker card shows a red status indicator and "Last seen: 2m ago"

#### Scenario: Worker card details
- **WHEN** a worker has `worker_id: "worker-1"`, `address: "10.0.0.2:9091"`, `max_splits: 8`
- **THEN** the card displays "worker-1", address "10.0.0.2:9091", and "8 splits" capacity

### Requirement: Standalone mode handling
The system SHALL handle standalone mode gracefully on the Cluster page. When the server role is "standalone", the worker list section SHALL display an informational message instead of an empty worker list, indicating that the server is running in single-node mode.

#### Scenario: Standalone mode display
- **WHEN** the server role is "standalone" and the workers endpoint returns an empty array
- **THEN** the Cluster page shows server info and an informational panel: "Running in standalone mode. No workers to display."

### Requirement: Cluster health summary
The system SHALL display a summary bar above the worker list showing: total worker count, alive worker count, dead worker count, and total split capacity across all alive workers.

#### Scenario: Cluster health with mixed status
- **WHEN** there are 4 workers (3 alive with max_splits 8 each, 1 dead)
- **THEN** the summary bar shows "4 total, 3 healthy, 1 unhealthy, 24 total splits"

#### Scenario: All workers healthy
- **WHEN** all 3 workers are alive
- **THEN** the summary bar shows "3 total, 3 healthy, 0 unhealthy"
