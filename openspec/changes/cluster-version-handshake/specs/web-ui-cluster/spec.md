## MODIFIED Requirements

### Requirement: Worker list with health indicators
The system SHALL display a list of workers as cards on the Cluster page, fetched from `GET /api/v1/cluster/workers`. Each worker card SHALL show: worker ID, RPC address, release version, alive/dead status indicator (green dot for alive, red dot for dead), max splits capacity, and time since last heartbeat. Each entry returned by the API SHALL include the worker's `version`. Workers SHALL be sorted with alive workers first, then by worker ID.

#### Scenario: Display healthy workers
- **WHEN** the API returns 3 workers all with `alive: true`
- **THEN** the page displays 3 worker cards each with a green status indicator

#### Scenario: Worker with stale heartbeat
- **WHEN** a worker has `alive: false` and `last_heartbeat_secs_ago: 120`
- **THEN** the worker card shows a red status indicator and "Last seen: 2m ago"

#### Scenario: Worker card details
- **WHEN** a worker has `worker_id: "worker-1"`, `address: "10.0.0.2:9091"`, `max_splits: 8`, `version: "0.1.6"`
- **THEN** the card displays "worker-1", address "10.0.0.2:9091", "8 splits" capacity and version "0.1.6"
