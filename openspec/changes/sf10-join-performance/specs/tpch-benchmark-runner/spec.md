## ADDED Requirements

### Requirement: The run's scale is an explicit input

The runner SHALL accept the scale factor of the data it is measuring, and the name of the scenario
that selected it, as explicit inputs to the run.

The runner MUST NOT infer scale from the output directory, the file name, or the surrounding
convention. A run whose scale was not supplied MUST record the scale as unknown rather than assume
a default, so that an unlabelled document is visibly unlabelled instead of silently attributed to
the most common scale.

#### Scenario: Scale and scenario are supplied

- **WHEN** the harness invokes the runner for the `sf10` scenario
- **THEN** the runner receives both the scenario name and the scale factor as arguments, and does
  not derive either from the output path

#### Scenario: Scale is not supplied

- **WHEN** the runner is invoked without a scale factor
- **THEN** the run proceeds and the resulting document records the scale as unknown

## MODIFIED Requirements

### Requirement: Result persistence

The runner SHALL write one JSON file per engine per invocation under the configured output directory, named `{engine}_{YYYYMMDD_HHMMSS}.json`. The JSON document MUST include: engine name, host, port (or `null` for in-process engines), an RFC 3339 timestamp, the warmup count and measurement count used, the scale factor the run measured, the name of the scenario that selected it (or `null` when no scenario was named), whether the scenario's host preconditions were bypassed, and the array of per-query results (each containing all runs, statistics, status, optional error, and optional skip reason).

The scale factor MUST be recorded in the document itself. The directory a document is filed in is a filing convention, not evidence of what was measured, and a document that carries its own scale cannot be read as belonging to a scale it does not describe.

The schema MUST remain backwards-compatible with the existing `engine`, `host`, `port`, `timestamp`, `queries[].query_id`, `queries[].status`, and `queries[].runs[]` shape so that previously written result files can still be parsed by the report. The scale factor, scenario name, and precondition-bypass fields MUST be optional on read: documents written before this change do not carry them, MUST still parse, and MUST be rendered as having an unrecorded scale rather than an assumed one.

#### Scenario: Output filenames carry engine and timestamp

- **WHEN** the runner finishes a multi-engine run on 2026-04-30 at 12:34:56
- **THEN** the output directory contains files named `arneb_20260430_123456.json`, `trino_20260430_123456.json`, and `datafusion_20260430_123456.json`

#### Scenario: Existing JSON shape still parses

- **WHEN** an older `arneb_*.json` produced before this change is fed to the new report
- **THEN** the report parses it, ignores absent statistics fields, and renders a partial row using only median

#### Scenario: A document carries the scale it measured

- **WHEN** a run of the `sf10` scenario completes
- **THEN** each engine's result document records the scale factor and the scenario name, independently of the directory it was written to

#### Scenario: A document written before this change has no scale

- **WHEN** a result document produced before this change is read
- **THEN** it parses successfully and its scale is rendered as unrecorded, not as the default scale
