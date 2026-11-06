## ADDED Requirements

### Requirement: Cordoned workers get no new queries
The coordinator SHALL leave a cordoned worker out of the workers it schedules new distributed queries on, while the queries it already runs continue. A cordon SHALL be independent of the worker-reported lifecycle `state`: a cordoned worker SHALL keep reporting `active` or `draining` as the worker sends it, and the workers API SHALL add a `scheduling` field with `schedulable`, `cordoned`, `draining` or `drained`. Selecting workers for a query and counting it against them SHALL happen under the same lock as cordon changes, so no query starts on a worker after it is cordoned. An uncordon SHALL make the worker schedulable again. When no schedulable worker is left, a distributed query SHALL fail with `no workers available for distributed execution` and the number of cordoned workers.

#### Scenario: Cordon while a query runs
- **WHEN** worker-1 is cordoned while a query runs on it and a second query starts
- **THEN** the first query finishes, and the second runs on the other workers only

#### Scenario: Drain completes
- **WHEN** a worker is drained and the last distributed query using it ends
- **THEN** `scheduling` changes from `draining` to `drained`

#### Scenario: Uncordon
- **WHEN** a cordoned worker is uncordoned
- **THEN** the next distributed query can run on it
