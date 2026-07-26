## ADDED Requirements

### Requirement: Official benchmark results are published per release

Each release that makes a performance claim SHALL publish the benchmark run backing that claim under a tracked, version-scoped directory `benchmarks/tpch/official/v<version>/`. The directory MUST NOT be covered by `.gitignore`, and MUST contain:

- one result document per engine that participated in the run, under a stable filename derived from the engine name (not the runner's timestamped output name),
- the generated comparison report exactly as the reporting tool emitted it, without hand-editing,
- a machine-readable provenance record (see the provenance requirement).

Directories for prior releases MUST be retained when a new release publishes its own, so that claims made by an earlier release stay verifiable.

#### Scenario: A release publishes its run

- **WHEN** version `1.0.0` publishes an official TPC-H run covering Arneb, Trino, and DataFusion
- **THEN** `benchmarks/tpch/official/v1.0.0/` is tracked in git and contains one result document per engine, the generated comparison report, and a provenance record

#### Scenario: A later release does not overwrite an earlier one

- **WHEN** a subsequent release publishes its own official run
- **THEN** it writes to its own `benchmarks/tpch/official/v<version>/` directory and the earlier release's directory is left unchanged

#### Scenario: Scratch results stay untracked

- **WHEN** a developer runs the benchmark harness locally
- **THEN** the runner's output lands in the gitignored scratch results directory and does not appear in `git status`

### Requirement: Provenance accompanies every official run

The provenance record SHALL capture what actually ran, in a machine-readable form, so a reader can determine whether their own run is comparable. It MUST include, at minimum:

- the Arneb release version and the git commit the run was produced from,
- the Rust compiler version used to build Arneb,
- the resolved version of every other engine in the run,
- the scale factor, the total number of queries attempted, the warmup run count, and the measurement run count,
- the host specification: CPU model, physical core count, RAM, operating system and version, and CPU architecture,
- the complete engine configuration under which each engine was measured, including every non-default tuning option in effect,
- the isolation and resource allocation each engine ran under: whether it ran natively or containerized, and its CPU and memory allocation,
- the run date as an RFC 3339 timestamp.

Engine versions MUST be resolved from what was actually running — the running container's image digest or the engine's own reported version — and MUST NOT be transcribed from a tag in a compose file.

#### Scenario: Floating image tags do not become the recorded version

- **WHEN** the benchmark stack starts Trino from an image tagged `latest`
- **THEN** the provenance record names the concrete version or image digest that the running container resolved to, not the string `latest`

#### Scenario: A reader assesses comparability

- **WHEN** a reader compares their own benchmark output against a published official run
- **THEN** the provenance record tells them the scale factor, host specification, warmup and measurement counts, and every engine version used, without consulting any other file

### Requirement: Compared engines are measured under controlled conditions

An official run SHALL place the compared engines under equivalent execution conditions, and SHALL disclose any inequality it could not eliminate. Specifically:

- engines being compared MUST run under the same form of isolation — it is not acceptable to run one engine natively on the host while its comparison target runs in a container,
- where an engine cannot be placed under the same isolation as the others, that asymmetry MUST be disclosed alongside the published figures rather than left to the provenance record alone.

Any non-default configuration applied to an engine for the run MUST be reproducible by a reader through the designated reproduction entry point. It is not acceptable to publish a figure produced with tuning options that the documented reproduction path does not apply.

#### Scenario: One engine would otherwise run natively

- **WHEN** the benchmark harness would start one engine natively on the host and its comparison target inside a container
- **THEN** the official run instead places both under the same isolation before the published figures are produced

#### Scenario: Non-default tuning is applied

- **WHEN** an official run measures an engine with tuning options that are disabled by default in a stock build
- **THEN** those options are recorded in the provenance record AND the designated reproduction entry point applies the same options, so a reader following it measures the same configuration

#### Scenario: An asymmetry cannot be removed

- **WHEN** one engine in the run cannot be placed under the same isolation as the others
- **THEN** the published figures carry that limitation as a stated caveat

### Requirement: Published results are unedited

The published result documents and comparison report SHALL be the verbatim output of the benchmark harness for the official run. Queries that failed MUST be published as failed, queries that were skipped MUST be published as skipped with their recorded reason, and cross-engine result divergences MUST be published in the report's divergence section.

Removing, rerunning, or substituting individual query results to improve the published figures is prohibited. If a run is rejected, the entire run is discarded and a new complete run is published in its place.

#### Scenario: A query fails during the official run

- **WHEN** one query fails on one engine during the official run
- **THEN** the published results record that failure with its error, and the published report reflects the reduced success count for that engine

#### Scenario: Engines disagree on a result

- **WHEN** two engines produce different canonical result hashes for the same query in the official run
- **THEN** the published comparison report contains that query in its correctness-divergence section

#### Scenario: A run is rejected

- **WHEN** an official run is judged unusable
- **THEN** the whole run is discarded and replaced by a complete rerun, rather than by patching individual query results

### Requirement: Reader-facing performance claims are traceable

Every performance claim stated in the project README or on the documentation site SHALL be sourced from the published official run for the current release. Each such claim MUST appear alongside the run date and a reference to the published directory the figures came from, and MUST NOT assert a scale factor, a query count, or a comparison that the published run does not contain.

Headline comparisons SHALL use a suite-level aggregate over the queries where both engines succeeded, not a best-case single-query result.

#### Scenario: README states a speedup

- **WHEN** the README claims Arneb is faster than Trino
- **THEN** the claim names the scale factor, the run date, and the path to the published official run, and the figure quoted appears in that run's comparison report

#### Scenario: A claim outruns the published evidence

- **WHEN** a performance claim references a scale factor for which no official run is published
- **THEN** the claim is not published in reader-facing prose

#### Scenario: Headline uses a suite aggregate

- **WHEN** the README or documentation homepage leads with a single comparison figure
- **THEN** that figure is a suite-level aggregate across the queries both engines completed, and per-query detail is reachable through a link to the published comparison report

### Requirement: One entry point for reproducing the comparison

The project SHALL designate exactly one document as the entry point for reproducing the published latency comparison, and every reader-facing surface that cites the numbers MUST link to it. Any other benchmark document in the repository MUST state its distinct purpose and link to the designated entry point, so that a reader is never presented with two competing sets of instructions for the same measurement.

#### Scenario: A reader follows the numbers back to a procedure

- **WHEN** a reader encounters the published figures in the README or on the documentation homepage
- **THEN** each surface links to the single designated reproduction document

#### Scenario: A second benchmark document exists

- **WHEN** the repository contains another benchmark document measuring something else, such as peak memory under container isolation
- **THEN** that document states what it measures and links to the designated entry point for the latency comparison
