# tpch-benchmark-runner Specification

## Purpose
Orchestrate a reproducible multi-engine TPC-H run: load the query suite, drive every
selected engine through an identical run plan, separate warmup from measurement,
compute real per-query statistics rather than a bare median, hash each result set so
cross-engine correctness divergence is detectable, treat unsupported queries as
first-class skips rather than failures, and persist all of it to a JSON document the
report layer can consume — including documents written by earlier versions.

## Requirements

### Requirement: Multi-engine run plan

The runner SHALL execute the same run plan against every selected engine. A run plan consists of: an ordered list of queries (loaded from `.sql` files in the queries directory), a number of warmup runs, and a number of measurement runs. Within a single invocation, the warmup count, the measurement count, and the SQL string for each query MUST be identical across engines.

#### Scenario: Same query plan on every engine

- **WHEN** the runner is invoked with `--engines arneb,trino,datafusion --num-runs 8 --warm-up 3`
- **THEN** every engine executes the same query files in the same order, each with 3 warmup runs followed by 5 measurement runs, and the SQL passed to each engine is byte-identical

### Requirement: Per-query statistics

For each (engine, query) pair, the runner SHALL compute and persist the following statistics, derived from the measurement runs only (warmup runs MUST be excluded):
- minimum wall-clock duration,
- p50 (median) wall-clock duration,
- p95 wall-clock duration,
- p99 wall-clock duration,
- standard deviation of wall-clock durations,
- the row count returned by the first measurement run,
- the canonical-result hash (see correctness requirement).

When fewer than two measurement runs exist (e.g., a query failed mid-run), stddev MUST be reported as `null` and the percentiles MUST be reported as the single observed value or `null` if no measurement runs completed.

#### Scenario: Statistics are computed from measurement runs only

- **WHEN** the runner completes 3 warmup and 5 measurement runs of a query
- **THEN** the persisted statistics for that query are derived from exactly the 5 measurement runs, ignoring all warmup timings

#### Scenario: Single-sample query

- **WHEN** only one measurement run completed before a transient failure
- **THEN** min/p50/p95/p99 are all reported as that single duration and stddev is `null`

### Requirement: Cross-engine correctness hash

The runner SHALL compute a canonical hash of each query's result set on every engine and surface any divergence to the report. The canonical form MUST:
- represent NULL as the literal sentinel `\N`,
- format floating-point values to a fixed number of **significant digits**, chosen to sit inside the precision f64 actually carries,
- format timestamps as RFC 3339 in UTC,
- sort rows lexicographically by their canonical row representation before hashing,
- produce the hash via SHA-256 over the joined canonical rows.

Floating-point comparison MUST be scale-invariant: the same relative difference between two values MUST be judged the same way regardless of their magnitude. Fixed-decimal formatting does not satisfy this — six fractional digits on a value near `5.7e10` demands seventeen significant digits, more than an f64 holds, so the comparison becomes strictly impossible to satisfy at large magnitudes while staying lax at small ones.

The chosen precision MUST absorb summation-order noise. Two engines that partition an aggregate differently sum the same column in a different order and, by the non-associativity of floating-point addition, produce results differing in the final unit in the last place. That is not a correctness difference and MUST NOT be reported as one. The precision MUST still expose genuine computation errors, which are orders of magnitude larger than one part in the last representable digit.

Rounding alone cannot decide this. Any comparison built on "round, then compare the text" has rounding boundaries, and two values straddling one are reported as different however closely they agree — TPC-H q09 produced `309901366.4294996` against `309901366.4295008`, agreeing to fifteen significant digits, yet the twelfth digit rounds up on one side and down on the other. Reducing the precision only relocates the boundary.

The runner MUST therefore persist a second hash at a coarser precision, and the report MUST use it to adjudicate: when the strict hashes differ but the coarse hashes agree, the query MUST be reported as a rounding-boundary artifact with an explanation, NOT as a correctness divergence. When both differ, it MUST be reported as a correctness divergence.

The runner MUST compute both hashes from the first measurement run of each query (not warmup) and persist them in the result JSON.

#### Scenario: Two engines agree on a query

- **WHEN** Arneb and DataFusion both run `q06` and produce equivalent result sets up to canonicalization
- **THEN** the persisted SHA-256 digests for `q06` are identical for both engines

#### Scenario: Two engines disagree on a query

- **WHEN** Arneb and Trino produce result sets that differ after canonicalization for `q11`
- **THEN** the persisted SHA-256 digests differ and the divergence is exposed to the report layer

#### Scenario: Summation-order noise is not a divergence

- **WHEN** two engines compute the same `SUM` over a large column and their results differ only in the final unit in the last place, because they summed the rows in a different order
- **THEN** their canonical forms are identical and no divergence is reported

#### Scenario: Comparison strictness does not depend on magnitude

- **WHEN** two values differing by a given relative amount are compared, and another pair differing by the same relative amount at a magnitude ten orders larger is compared
- **THEN** both pairs are judged the same way — either both divergent or both equal

#### Scenario: A real computation error still diverges

- **WHEN** two engines produce results differing by more than the canonical precision retains
- **THEN** their canonical forms differ and the divergence is reported

#### Scenario: A rounding boundary is not reported as a divergence

- **WHEN** two engines produce values that agree well beyond the strict precision but straddle a rounding boundary in its last retained digit, so the strict hashes differ and the coarse hashes agree
- **THEN** the report describes the query as a floating-point boundary artifact and does not list it as a correctness divergence

#### Scenario: A real difference survives adjudication

- **WHEN** two engines produce genuinely different values, so both the strict and the coarse hashes differ
- **THEN** the report lists the query as a correctness divergence

### Requirement: Skipped queries are first-class

The runner SHALL accept a structured per-engine skip list (e.g., declaring that an engine cannot run a query today because it requires an unsupported SQL feature). A skipped query MUST be recorded in the result JSON with `status: "skipped"`, an explanatory `reason`, and no run timings, and MUST NOT block other engines from executing the same query.

Every skip entry MUST be justified by an observed failure of that query on that engine, and the recorded reason MUST describe that observed failure. Entries derived from reading the SQL rather than executing it are prohibited: an unverified entry silently removes a query from the comparison and publishes a fabricated reason for doing so, which is worse than having no skip list at all.

#### Scenario: An engine skips a query another engine runs

- **WHEN** the skip list declares a verified skip for one engine on `q21`
- **THEN** that engine's result for `q21` is recorded as skipped with the observed reason, while the other engines still execute `q21` normally and record their statistics

#### Scenario: A skip entry is not backed by an observed failure

- **WHEN** a query declared skipped for an engine is executed against that engine and succeeds
- **THEN** the entry is removed, because the comparison was silently excluding a query the engine can run

### Requirement: Result persistence

The runner SHALL write one JSON file per engine per invocation under the configured output directory, named `{engine}_{YYYYMMDD_HHMMSS}.json`. The JSON document MUST include: engine name, host, port (or `null` for in-process engines), an RFC 3339 timestamp, the warmup count and measurement count used, and the array of per-query results (each containing all runs, statistics, status, optional error, and optional skip reason).

The schema MUST remain backwards-compatible with the existing `engine`, `host`, `port`, `timestamp`, `queries[].query_id`, `queries[].status`, and `queries[].runs[]` shape so that previously written result files can still be parsed by the report.

#### Scenario: Output filenames carry engine and timestamp

- **WHEN** the runner finishes a multi-engine run on 2026-04-30 at 12:34:56
- **THEN** the output directory contains files named `arneb_20260430_123456.json`, `trino_20260430_123456.json`, and `datafusion_20260430_123456.json`

#### Scenario: Existing JSON shape still parses

- **WHEN** an older `arneb_*.json` produced before this change is fed to the new report
- **THEN** the report parses it, ignores absent statistics fields, and renders a partial row using only median
