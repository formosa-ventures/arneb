# tpch-benchmark-reporting Specification

## Purpose
Turn one or more per-engine result JSON documents into a single Markdown comparison
report that a reader can act on: per-query side-by-side latencies with pairwise
speedups, a suite-level geomean, an explicit correctness-divergence section, and an
honest caveat about what p95/p99 mean at low sample counts. The report is a Rust
subcommand so the whole harness builds and runs with `cargo` alone — no Python.

## Requirements

### Requirement: `tpch-bench report` subcommand

The harness SHALL expose a `report` subcommand that consumes one or more result JSON files (or a directory, in which case the most recent file per engine is selected) and emits a Markdown comparison report to stdout. The subcommand MUST accept an `--output <path>` flag to write the report to a file in addition to stdout.

#### Scenario: Report from a directory of results

- **WHEN** the user runs `tpch-bench report --dir benchmarks/tpch/results`
- **THEN** the subcommand picks the most recent JSON file for each engine present in the directory and emits a single Markdown comparison report

#### Scenario: Report from explicit files

- **WHEN** the user runs `tpch-bench report results/arneb_a.json results/trino_b.json results/datafusion_c.json`
- **THEN** the subcommand uses exactly those three files

### Requirement: Three-way comparison table

For runs covering Arneb, Trino, and DataFusion, the report SHALL include a per-query table with one row per query containing, at minimum: query id, p50 (median) wall-clock duration for each engine, an Arneb-vs-Trino speedup, an Arneb-vs-DataFusion speedup, and the query status if not `ok`. Speedups MUST be expressed as a numeric multiplier with two decimals (e.g., `1.42x`), and MUST be omitted (`-`) when either side failed or was skipped.

When fewer than three engines are present, the table MUST gracefully degrade to the engines available and still emit pairwise speedups for any two engines that both ran a given query.

#### Scenario: All three engines ran the same query

- **WHEN** Arneb, Trino, and DataFusion each have a successful `q01` measurement
- **THEN** the report's `q01` row shows three p50 columns and two speedup columns, all populated

#### Scenario: Trino is missing from the inputs

- **WHEN** only Arneb and DataFusion result files are passed
- **THEN** the report renders two p50 columns and a single Arneb-vs-DataFusion speedup column, with no Trino column at all

### Requirement: Suite-level summary

The report SHALL include a summary section showing, for each engine: the count of `ok`, `failed`, and `skipped` queries, and the geometric mean of measurement-run p50 durations across all queries that succeeded on that engine. For pairs of engines, the summary MUST also report the geometric-mean speedup across queries where both engines succeeded.

#### Scenario: Geomean is computed only over commonly-successful queries

- **WHEN** Arneb succeeds on 20 queries and Trino succeeds on 19 (one mutually disjoint failure each)
- **THEN** the Arneb-vs-Trino geomean speedup is computed across the 18 queries where both engines succeeded

### Requirement: Correctness divergence section

When two or more engines ran the same query and their canonical-result hashes differ, the report SHALL include a divergence section listing each affected query and the engines whose hashes differ. The section MUST include the row counts each engine returned for that query.

#### Scenario: All engines agree on a query

- **WHEN** every engine present in the report produces the same canonical hash for `q06`
- **THEN** `q06` does not appear in the divergence section

#### Scenario: Two engines disagree on a query

- **WHEN** Arneb's hash for `q11` differs from Trino's hash for `q11`
- **THEN** the divergence section lists `q11` with both engines, both hashes (truncated to 8 hex chars), and both row counts

### Requirement: Statistics caveat header

The report SHALL include a header note disclosing the warmup count and measurement count from the input files, and MUST warn the reader that p95 and p99 are not statistically meaningful at low sample counts (specifically: when measurement count is below 20).

#### Scenario: Low sample count warning

- **WHEN** the inputs report 5 measurement runs per query
- **THEN** the report header notes "p95/p99 are heuristic at this sample count" or equivalent wording

### Requirement: Removal of `report.py`

The harness SHALL NOT ship `benchmarks/tpch/scripts/report.py`; the `tpch-bench report` subcommand replaces it.

The shell script `benchmarks/tpch/scripts/run_benchmark.sh` MUST invoke `tpch-bench report` rather than `python3 scripts/report.py`.

#### Scenario: run_benchmark.sh produces a report without Python

- **WHEN** a user runs `./benchmarks/tpch/scripts/run_benchmark.sh` on a machine with no `python3` interpreter
- **THEN** the script completes successfully and writes `comparison.md`
