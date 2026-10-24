## MODIFIED Requirements

### Requirement: Lost workers are not scheduled
The coordinator SHALL mark a worker dead immediately when a task submission
or Flight stream to it fails with a transport error. A worker past its heartbeat deadline
SHALL be treated as dead. Queries that start afterwards SHALL be scheduled
only on live workers that are not draining.

#### Scenario: Next query after a kill
- **WHEN** a worker was killed and a query failed because of it
- **THEN** the next query succeeds on the remaining workers without waiting for the heartbeat timeout

#### Scenario: Next query during a drain
- **WHEN** a worker is draining
- **THEN** the next query is scheduled on the other workers only
