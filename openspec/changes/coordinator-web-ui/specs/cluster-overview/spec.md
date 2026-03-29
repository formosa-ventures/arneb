## ADDED Requirements

### Requirement: Cluster overview page
The system SHALL provide a cluster overview page displaying coordinator information and worker status. The page SHALL show the coordinator's version, uptime, role, and listening ports.

#### Scenario: Viewing cluster overview
- **WHEN** a user navigates to the Cluster view
- **THEN** the page displays coordinator info (version, uptime, role) and a list of workers

### Requirement: Worker cards
The system SHALL display each registered worker as a card showing: worker ID, address, status (active/inactive), number of active tasks, and time since last heartbeat. Workers that have not sent a heartbeat within the timeout period SHALL be displayed with an inactive/warning status.

#### Scenario: All workers healthy
- **WHEN** 3 workers are registered and all have recent heartbeats
- **THEN** 3 worker cards are displayed, all showing active status

#### Scenario: Worker missing heartbeat
- **WHEN** a worker has not sent a heartbeat for longer than the timeout
- **THEN** that worker's card shows an inactive/warning status indicator

#### Scenario: Worker task count
- **WHEN** a worker is executing 5 tasks
- **THEN** the worker card shows "Active Tasks: 5"

### Requirement: Coordinator info section
The cluster page SHALL display a coordinator information section showing: server version, uptime (human-readable duration), server role (coordinator/standalone), pgwire port, Flight port, and HTTP port.

#### Scenario: Coordinator info display
- **WHEN** the cluster page loads and the server has been running for 2 hours 15 minutes
- **THEN** the coordinator section shows uptime as "2h 15m" and all configured ports
