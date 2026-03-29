## ADDED Requirements

### Requirement: Round-robin stage assignment
The system SHALL implement a `NodeScheduler` that assigns stages to available workers using a round-robin strategy. The scheduler SHALL use an atomic counter to cycle through active workers returned by the NodeRegistry. The scheduler SHALL skip Dead and Draining workers.

#### Scenario: Single stage, multiple workers
- **WHEN** 3 active workers are registered and a single stage needs assignment
- **THEN** the scheduler assigns the stage to the next worker in the round-robin sequence

#### Scenario: Multiple stages distributed across workers
- **WHEN** 2 active workers are registered and 4 stages need assignment
- **THEN** each worker receives 2 stages (stages are evenly distributed)

#### Scenario: No active workers
- **WHEN** no active workers are registered and a stage needs assignment
- **THEN** the scheduler returns a `SchedulingError::NoActiveWorkers` error

### Requirement: NodeScheduler thread safety
The system SHALL ensure that `NodeScheduler` is safe for concurrent use from multiple query handlers. The round-robin counter SHALL use `AtomicUsize` for lock-free increment.

#### Scenario: Concurrent scheduling
- **WHEN** two queries simultaneously request stage assignments
- **THEN** each gets a valid worker assignment without data races or duplicate assignments to the same stage

### Requirement: Schedule plan for query
The system SHALL provide a method `schedule_query(fragments: &[PlanFragment]) -> Result<Vec<(PlanFragment, WorkerId)>>` that assigns each fragment to a worker and returns the assignment mapping.

#### Scenario: Schedule a 3-stage query
- **WHEN** a query has 3 plan fragments and 2 active workers
- **THEN** `schedule_query` returns 3 assignments with workers distributed round-robin (worker1, worker2, worker1)

#### Scenario: Single worker cluster
- **WHEN** a query has 5 plan fragments and only 1 active worker
- **THEN** all 5 fragments are assigned to the single active worker
