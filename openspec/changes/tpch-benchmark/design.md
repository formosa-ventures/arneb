## Context

After Changes 1-9, the engine supports distributed query execution. We need to validate performance against the industry standard (TPC-H) and compare with Trino.

## Goals / Non-Goals

**Goals:**

- Generate TPC-H data at multiple scale factors
- Run all 22 queries with timing and resource metrics
- Automated comparison with Trino baseline
- Reproducible benchmark methodology

**Non-Goals:**

- TPC-H certification (requires formal audit)
- TPC-DS support (99 queries — too many SQL features needed for Phase 2)
- Continuous benchmarking in CI (manual runs for now)

## Decisions

### D1: Data generation

**Choice**: Shell script wrapping `dbgen` (TPC-H official tool). Convert CSV output to Parquet using a small Rust helper. Store in `benchmarks/tpch/data/sf{N}/`.

**Rationale**: `dbgen` is the official TPC-H data generator, ensuring standard-compliant data. Parquet conversion matches the engine's preferred file format. Storing by scale factor keeps data organized.

### D2: Query files

**Choice**: `benchmarks/tpch/queries/q{01-22}.sql`. Adapted from TPC-H spec with minor syntax adjustments for our SQL parser.

**Rationale**: Separate SQL files are easy to inspect, version, and modify. Numbering matches the TPC-H specification.

### D3: Benchmark runner

**Choice**: Rust binary at `benchmarks/tpch/src/main.rs`. Connects via tokio-postgres. Runs each query N times (default 5), discards first 2 as warm-up. Records wall clock time per query.

**Rationale**: Rust runner can share types with the engine. tokio-postgres provides a standard PostgreSQL client since the engine speaks the pgwire protocol. Warm-up runs eliminate JIT/cache cold-start effects.

### D4: Metrics collection

**Choice**: Client-side timing (wall clock). Server-side metrics via EXPLAIN ANALYZE if available, or query the REST API for query stats.

**Rationale**: Wall clock time is the most meaningful metric for users. Server-side metrics provide additional insight for optimization but are secondary.

### D5: Trino baseline

**Choice**: `benchmarks/tpch/scripts/run_trino.sh` that runs same queries against Trino via `trino` CLI, captures timing.

**Rationale**: Using the Trino CLI ensures fair comparison — both engines measured from client perspective. Same queries, same data, same hardware.

### D6: Report generator

**Choice**: `benchmarks/tpch/scripts/report.py` reads JSON results from both engines, produces markdown table with speedup ratios.

**Rationale**: Python is well-suited for data processing and report generation. Markdown output is readable in terminals and renders on GitHub.

## Risks / Trade-offs

**[SQL compatibility]** → Some TPC-H queries use features we may not support yet (CASE, subqueries, HAVING). **Mitigation**: Adapt queries where possible. Skip unsupported queries and document limitations.

**[Hardware dependency]** → Results only meaningful on identical hardware. **Mitigation**: Document test environment in benchmark output. All comparisons must be run on the same machine.

**[dbgen availability]** → The official TPC-H tool requires compilation from source. **Mitigation**: Provide clear build instructions in the README. Consider shipping pre-generated SF1 data.
