## ADDED Requirements

### Requirement: QueryCoordinator orchestrates distributed execution
The system SHALL provide a `QueryCoordinator` that takes a fragmented plan and executes it across available workers. It SHALL execute stages bottom-up (leaf stages first), submit tasks to workers via RPC, wait for task completion, and collect final results via ExchangeClient.

#### Scenario: Two-stage query (scan + project)
- **WHEN** a query `SELECT * FROM t WHERE x > 1` is fragmented into a source stage (TableScan) and a root stage (Filter+Projection)
- **AND** one worker is available
- **THEN** the coordinator submits the source stage as a task to the worker, waits for completion, then executes the root stage locally using ExchangeExec to read from the worker's OutputBuffer

#### Scenario: Three-stage join query
- **WHEN** a query `SELECT * FROM a JOIN b ON a.id = b.id` is fragmented into scan(a), scan(b), and join stages
- **AND** two workers are available
- **THEN** the coordinator submits scan(a) to worker-1 and scan(b) to worker-2, waits for both to complete, then executes the join stage using ExchangeExec to read from both workers

#### Scenario: No workers available
- **WHEN** a query is submitted and no workers are registered
- **THEN** the coordinator falls back to local execution (same as standalone mode)

### Requirement: include-coordinator configuration
The system SHALL support a `cluster.include_coordinator` config option (default: true). When true, the coordinator registers itself in the NodeRegistry and can receive task assignments. When false, the coordinator only coordinates.

#### Scenario: include-coordinator true
- **WHEN** coordinator starts with `include_coordinator = true`
- **THEN** the coordinator appears in NodeRegistry.alive_workers() and can be assigned tasks

#### Scenario: include-coordinator false
- **WHEN** coordinator starts with `include_coordinator = false` and no workers are registered
- **THEN** queries fall back to local execution (coordinator still executes locally but is not assigned tasks by the scheduler)
