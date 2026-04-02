## Why

Performance claims require evidence. TPC-H is the industry-standard analytical benchmark used by Trino, ClickHouse, DuckDB, and DataFusion for comparison. A benchmark harness lets us measure query performance across all 22 TPC-H queries, compare against Trino on identical hardware, and track regressions as the engine evolves.

## What Changes

- Create benchmarks/tpch/ directory with TPC-H data generation, query files, and runner
- Integrate TPC-H data generator producing Parquet files for 8 tables at configurable scale factors
- Adapt all 22 TPC-H queries to our SQL dialect
- Build benchmark runner that measures per-query metrics (wall clock, CPU, memory, I/O)
- Create Trino baseline scripts for identical workload comparison
- Build report generator producing markdown comparison tables

## Capabilities

### New Capabilities

- `tpch-data-generator`: TPC-H data generation in Parquet format for SF1/SF10/SF100
- `tpch-queries`: All 22 TPC-H queries adapted for our engine
- `benchmark-metrics`: Per-query and cluster-level metrics collection
- `benchmark-runner`: Automated runner with warm-up, multiple iterations, and JSON output
- `benchmark-report`: Comparison report generator (arneb vs Trino)

### Modified Capabilities

(No existing capabilities modified)

## Impact

- **Directory**: benchmarks/tpch/ (new, not a workspace crate)
- **Dependencies**: tokio-postgres (client for benchmark runner), criterion (optional)
- **External**: Requires Trino installation for baseline comparison
