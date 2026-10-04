## ADDED Requirements

### Requirement: Scale scenarios are declared

The benchmark harness SHALL define each measurable scale as a named **scenario** stored under
`benchmarks/tpch/scenarios/<name>`, and SHALL declare at least the scenarios `sf1` and `sf10`.

A scenario declaration MUST state, for that scale:
- the TPC-H scale factor to seed,
- the run plan — total runs per query and the number of leading runs discarded as warmup,
- the per-node CPU allocation and the engine topology the run assumes,
- the host resource floors the scenario requires (container-runtime memory, CPU count, free disk).

A scenario declaration MUST NOT contain conditional logic; it declares values only. Selecting a
scenario by name MUST be sufficient to fix every parameter above — a run that names a scenario MUST
NOT additionally require the operator to set scale, run plan, or topology by hand.

#### Scenario: Naming a scenario fixes the run parameters

- **WHEN** the harness is invoked naming the `sf10` scenario and no other tuning arguments
- **THEN** the scale factor, run plan, per-node CPU allocation, and host floors all come from the
  `sf10` declaration, and the operator sets none of them

#### Scenario: An undeclared scenario is rejected

- **WHEN** the harness is invoked naming a scenario that has no declaration file
- **THEN** the run aborts before starting any container, naming the scenario that was requested and
  listing the scenarios that are declared

### Requirement: Host preconditions are verified before any data is seeded

Before seeding, and before any engine container is started for measurement, the harness SHALL
compare the current host's container-runtime memory, CPU count, and free disk against the floors
declared by the selected scenario.

If any floor is unmet, the run MUST abort, and the abort message MUST name the unmet precondition,
the floor the scenario declares, and the value observed on this host. The harness MUST NOT proceed
with a warning: the failure a missing floor predicts arrives later as an out-of-memory kill whose
cause is not evident from the output.

An operator MAY bypass the check explicitly. A bypassed run MUST record that fact in every result
document it produces, so that a bypassed run is distinguishable from one whose preconditions were
met.

#### Scenario: A host below the memory floor is refused

- **WHEN** the `sf10` scenario is run on a host whose container runtime reports less memory than the
  scenario's declared floor
- **THEN** the run aborts before seeding, and the message names the memory precondition, the
  declared floor, and the observed value

#### Scenario: The check runs before seeding, not after

- **WHEN** a scenario's precondition is unmet
- **THEN** no TPC-H data has been written to object storage and no engine container has been started
  for measurement

#### Scenario: A bypassed run is marked as such

- **WHEN** an operator bypasses the precondition check and the run completes
- **THEN** each result document records that the preconditions were bypassed

### Requirement: A scenario runs end to end on one local Docker host

Each declared scenario SHALL run to completion — infrastructure startup, seeding, per-engine
measurement, report generation, and shutdown — from a single command on one developer machine using
the project's Docker Compose stack, with no dependency on any host outside that machine.

Engines MUST continue to be measured one at a time, with the engines not being measured stopped
rather than left idle, so that a measured engine is not competing for memory with a rival's
resident heap.

On completion, the harness MUST leave no engine container running.

#### Scenario: One command produces a comparison report

- **WHEN** an operator on a host meeting the `sf10` floors runs the harness naming that scenario
- **THEN** the stack is brought up, the data is seeded, each engine is measured in turn, a
  comparison report is written, and no engine container is left running

#### Scenario: No external host participates

- **WHEN** any declared scenario runs
- **THEN** every engine, the runner, the metastore, and the object store execute on the invoking
  machine

### Requirement: An interrupted scenario leaves no partially seeded dataset

Seeding SHALL be idempotent: a scenario that is interrupted during seeding and then re-run MUST
produce the same dataset as an uninterrupted run, without requiring the operator to clean up object
storage or metastore state by hand.

A re-run that is explicitly told to reuse already-seeded data MUST verify that the resident dataset
matches the scale factor the selected scenario declares, and MUST abort if it does not.

#### Scenario: Seeding is re-runnable after an interruption

- **WHEN** seeding is interrupted partway through and the scenario is run again
- **THEN** the resulting dataset is the complete dataset for that scenario's scale factor

#### Scenario: Reused data of the wrong scale is refused

- **WHEN** a run reuses already-seeded data whose scale factor differs from the selected scenario's
- **THEN** the run aborts rather than measuring one scale under another scale's label

### Requirement: Scenario repeatability is measured, not asserted

For each declared scenario, the project SHALL record the run-to-run spread observed by executing
that scenario repeatedly on one unchanged host, and SHALL state that spread alongside the
scenario's figures.

A performance difference smaller than the recorded spread MUST NOT be reported as an improvement or
a regression for that scenario.

Repeatability is claimed **within** a host. Nothing in this capability asserts that two different
hosts produce comparable absolute figures.

#### Scenario: Repeats establish the spread

- **WHEN** a scenario is executed several times on one unchanged host
- **THEN** the spread across those repeats is recorded with the scenario's captured figures

#### Scenario: A difference inside the noise is not a result

- **WHEN** a change moves a scenario's suite aggregate by less than the recorded run-to-run spread
- **THEN** the change is not reported as having improved or regressed that scenario
