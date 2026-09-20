## ADDED Requirements

### Requirement: A measured scale factor that is withheld is disclosed as withheld

A release that has measured a scale factor and decides not to publish it SHALL state, on every
reader-facing surface that scopes its performance claim, that the scale factor was **measured and
withheld**, why it was withheld, and where the withheld run can be read.

Stating only that a scale factor is "not published" is insufficient: a reader parses that as *not
measured*, which is a different and more favourable claim than the truth. Scoping a claim to the
scale it covers is legitimate; leaving a reader unable to discover that an unfavourable run exists
is not.

A withheld run MUST remain recoverable — in version control if it is not in the working tree — and
the disclosure MUST name how to reach it.

#### Scenario: A release scopes its claim to one scale factor

- **WHEN** a release publishes SF1 figures and withholds a SF10 run it produced
- **THEN** the README and documentation homepage state that SF10 was measured and withheld, give the
  reason, and name where the withheld run can be read

#### Scenario: Wording that implies the scale was never measured

- **WHEN** a reader-facing surface says only that larger scale factors are "not published"
- **THEN** the wording is corrected, because a measured-and-withheld result and an unmeasured scale
  are different claims

#### Scenario: A scale factor that was genuinely never measured

- **WHEN** a release has produced no run at a given scale factor
- **THEN** no disclosure is required for that scale factor, and stating that it is unpublished is
  accurate

### Requirement: Republication of a withheld scale factor is gated on work landing, not on the result

A withheld scale factor SHALL be republished under `benchmarks/tpch/official/v<version>/` when both:

- every change identified as owning a regression in the withheld run has been completed, or has been
  explicitly deferred with that deferral recorded, and
- a fresh run of that scale factor's declared scenario exists.

The resulting figures SHALL be published whatever they show. Republication MUST NOT be conditioned
on the outcome reaching any threshold — a gate of that form makes publication contingent on the
result and converts a one-time withholding into a standing one.

A release MAY be slower than a compared engine at a given scale. A reader MUST NOT be unable to find
that out.

#### Scenario: The owning work lands and the number is still unfavourable

- **WHEN** every change owning a regression in the withheld run has landed, the scenario is re-run,
  and the result still trails the compared engine
- **THEN** the run is published as the official result for that scale factor, and the release's
  claims are scoped to what it shows

#### Scenario: Publication is proposed as conditional on the outcome

- **WHEN** a republication rule is proposed that publishes a scale factor only if it reaches a
  performance threshold
- **THEN** the rule is rejected, because it makes the published record a function of the result

#### Scenario: An owning change is deferred rather than completed

- **WHEN** a change identified as owning a regression is deferred out of the release
- **THEN** the deferral is recorded, and it does not by itself block republication

### Requirement: Every regression in a withheld run is attributed

Before a withheld scale factor is republished, the release SHALL record, per query, every query in
the withheld run that trailed the compared engine, the plan shape responsible, and the change that
owns it.

An attribution MUST cite the evidence it rests on — a profiling record, a plan, or the owning
change's own measurement. `unowned` MUST be a permitted value: a regression no in-flight change
addresses is the honest statement of what the release does not reach, and MUST be visible rather
than absent.

An attribution that is falsified — the owning change lands and its query does not move — MUST be
corrected in the record rather than removed.

#### Scenario: Each trailing query is accounted for

- **WHEN** a withheld run contains queries that trailed the compared engine
- **THEN** each of them appears in the attribution record with its plan shape, its owning change,
  and the evidence for that attribution

#### Scenario: A regression nothing addresses

- **WHEN** a trailing query is not addressed by any in-flight change
- **THEN** it is recorded as unowned rather than omitted from the record

#### Scenario: An attribution turns out to be wrong

- **WHEN** an owning change lands and the query attributed to it does not improve
- **THEN** the attribution record is corrected to reflect that the cause is still unidentified
