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
- format floating-point values with six fractional digits,
- format timestamps as RFC 3339 in UTC,
- sort rows lexicographically by their canonical row representation before hashing,
- produce the hash via SHA-256 over the joined canonical rows.

The runner MUST compute the hash from the first measurement run of each query (not warmup) and persist it in the result JSON.

#### Scenario: Two engines agree on a query

- **WHEN** Arneb and DataFusion both run `q06` and produce equivalent result sets up to canonicalization
- **THEN** the persisted SHA-256 digests for `q06` are identical for both engines

#### Scenario: Two engines disagree on a query

- **WHEN** Arneb and Trino produce result sets that differ after canonicalization for `q11`
- **THEN** the persisted SHA-256 digests differ and the divergence is exposed to the report layer

### Requirement: Skipped queries are first-class

The runner SHALL accept a structured per-engine skip list (e.g., declaring that Arneb cannot run `q21` today because it requires correlated subqueries). A skipped query MUST be recorded in the result JSON with `status: "skipped"`, an explanatory `reason`, and no run timings, and MUST NOT block other engines from executing the same query.

#### Scenario: Arneb skips a query, Trino runs it

- **WHEN** the skip list declares `arneb: q21 — correlated subqueries unsupported`
- **THEN** Arneb's result for `q21` is recorded as skipped with that reason, while Trino still executes `q21` normally and records its statistics

### Requirement: Result persistence

The runner SHALL write one JSON file per engine per invocation under the configured output directory, named `{engine}_{YYYYMMDD_HHMMSS}.json`. The JSON document MUST include: engine name, host, port (or `null` for in-process engines), an RFC 3339 timestamp, the warmup count and measurement count used, and the array of per-query results (each containing all runs, statistics, status, optional error, and optional skip reason).

The schema MUST remain backwards-compatible with the existing `engine`, `host`, `port`, `timestamp`, `queries[].query_id`, `queries[].status`, and `queries[].runs[]` shape so that previously written result files can still be parsed by the report.

#### Scenario: Output filenames carry engine and timestamp

- **WHEN** the runner finishes a multi-engine run on 2026-04-30 at 12:34:56
- **THEN** the output directory contains files named `arneb_20260430_123456.json`, `trino_20260430_123456.json`, and `datafusion_20260430_123456.json`

#### Scenario: Existing JSON shape still parses

- **WHEN** an older `arneb_*.json` produced before this change is fed to the new report
- **THEN** the report parses it, ignores absent statistics fields, and renders a partial row using only median
