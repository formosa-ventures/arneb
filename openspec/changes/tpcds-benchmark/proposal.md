## Why

TPC-DS is the industry-standard decision support benchmark, exercising far more SQL complexity than TPC-H: 99 queries across 24 tables in a snowflake schema, covering CTEs, window functions (LAG/LEAD/NTILE), GROUPING SETS/ROLLUP/CUBE, complex multi-table joins (10+), and statistical aggregates. Running TPC-DS against arneb measures both performance and SQL completeness, providing a clear roadmap of which SQL features to prioritize. It also enables direct comparison with Trino, ClickHouse, DuckDB, and DataFusion on the same workload.

## What Changes

- Create benchmarks/tpcds/ directory with query files and Docker Compose data generation
- Add Docker Compose services for TPC-DS data seeding via Trino CTAS into Hive/MinIO
- Adapt all 99 TPC-DS queries, marking unsupported ones with SKIP annotations
- Reuse the existing benchmark runner (benchmarks/tpch/) with --queries-dir pointed at tpcds queries
- Build comparison reports tracking query pass rate and performance vs Trino

## Capabilities

### New Capabilities

- `tpcds-data-generator`: TPC-DS data generation via Trino CTAS into Hive tables on MinIO for tiny/SF1/SF10
- `tpcds-queries`: All 99 TPC-DS queries adapted for arneb, with SKIP markers for unsupported queries
- `benchmark-metrics`: Per-query metrics collection with coverage tracking (pass/skip/fail counts)
- `benchmark-runner`: Shared runner invoked with --queries-dir benchmarks/tpcds/queries
- `benchmark-report`: Comparison report with 99-query pass rate tracking and skip reason breakdown

### Modified Capabilities

- Existing benchmark runner gains no code changes; reused via different --queries-dir argument

## Impact

- **Directory**: benchmarks/tpcds/ (new, not a workspace crate)
- **Docker Compose**: New tpcds-seed service added alongside existing infrastructure
- **Dependencies**: None new (reuses existing runner dependencies)
- **External**: Requires Trino with tpcds connector for data generation and baseline comparison
