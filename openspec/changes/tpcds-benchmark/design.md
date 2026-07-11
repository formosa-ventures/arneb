## Context

The TPC-H benchmark (22 queries, 8 tables, star schema) is implemented and provides basic performance validation. TPC-DS is the next step: 99 queries across 24 tables in a snowflake schema, exercising significantly more SQL features. Most TPC-DS queries require SQL features arneb does not yet support (CTEs, advanced window functions, ROLLUP/CUBE), so the benchmark will initially serve as a progress tracker. As SQL capabilities are added through separate changes, more queries will pass, making the TPC-DS pass rate a measure of SQL completeness.

## Goals / Non-Goals

**Goals:**

- Generate TPC-DS data at multiple scale factors using Trino CTAS into Hive/MinIO
- Include all 99 TPC-DS queries with SKIP markers for unsupported ones
- Reuse the existing benchmark runner without code duplication
- Track incremental progress as SQL features are added (pass count as key metric)
- Automated comparison with Trino baseline on passing queries

**Non-Goals:**

- SQL feature implementation (CTEs, ROLLUP/CUBE, LAG/LEAD are separate changes)
- TPC-DS certification (requires formal audit process)
- Continuous benchmarking in CI (manual runs for now)
- Custom data generator (no dsdgen compilation; Trino provides the data)

## Decisions

### D1: Data generation via Trino CTAS into Hive/MinIO

**Choice**: Use Trino's built-in `tpcds` connector to generate data via CTAS (CREATE TABLE AS SELECT) into Hive tables stored on MinIO. A Docker Compose `tpcds-seed` service orchestrates this: it waits for Trino, HMS, and MinIO to be healthy, then runs CTAS for all 24 TPC-DS tables at the configured scale factor.

**Rationale**: The official `dsdgen` tool requires compilation from source and produces CSV that must be converted. Trino's tpcds connector generates standard-compliant data directly in memory and CTAS writes it into Hive tables in Parquet format on MinIO -- the same format arneb reads. This approach is also planned for migrating TPC-H data generation, so TPC-DS adopts the target architecture from the start.

### D2: Shared benchmark runner

**Choice**: Reuse the existing runner at `benchmarks/tpch/src/main.rs` without modification. The runner already supports `--engine arneb|trino`, `--queries-dir`, and `--queries` flags. For TPC-DS, invoke it with `--queries-dir benchmarks/tpcds/queries`.

**Rationale**: The runner is engine-agnostic and query-set-agnostic by design. It discovers `.sql` files in any directory, handles SKIP markers, records per-query timing, and outputs JSON. No code changes needed. A wrapper script (`benchmarks/tpcds/scripts/run.sh`) provides convenience defaults.

### D3: Query adaptation approach

**Choice**: Include all 99 TPC-DS queries as `benchmarks/tpcds/queries/q{01-99}.sql`. Queries requiring unsupported SQL features begin with `-- SKIP: <reason>` (e.g., `-- SKIP: requires CTE support`). The runner detects this marker and records the query as skipped.

**Rationale**: Including all 99 queries from the start means no query management overhead as features are added -- just remove the SKIP marker when the feature lands. The skip reason documents exactly which SQL feature is blocking each query, creating a natural dependency map between benchmark progress and SQL feature changes.

### D4: Docker Compose integration

**Choice**: Add a `tpcds-seed` service to Docker Compose alongside the existing infrastructure (MinIO, HMS, Trino). The seed service runs a shell script that executes CTAS for all 24 TPC-DS tables. A separate compose profile (`tpcds`) controls whether TPC-DS seeding runs.

**Rationale**: Using Docker Compose profiles avoids slowing down TPC-H-only workflows. The seed service follows the same pattern that will be used when TPC-H migrates to CTAS-based generation, keeping infrastructure consistent.

### D5: Incremental progress tracking

**Choice**: The primary metric is the number of passing queries out of 99. The comparison report includes a "Coverage" section showing: X/99 passing, with a breakdown by skip reason (e.g., "CTE: 35 queries, Window frames: 12 queries"). Performance comparison is secondary and only applies to passing queries.

**Rationale**: With ~15-20 queries expected to pass initially, raw performance comparison is less meaningful than tracking coverage. As SQL features are added (CTE support alone could unlock ~25-30 queries), the pass count becomes the primary progress indicator. Performance comparison gains importance once coverage exceeds 50%.

## Risks / Trade-offs

**[Low initial coverage]** -> Only ~15-20 of 99 queries may pass initially. **Mitigation**: This is expected and the benchmark is designed as a progress tracker. Each SQL feature change unlocks a batch of queries, providing clear evidence of progress.

**[SKIP marker maintenance]** -> Skip reasons must be updated as SQL features are added. **Mitigation**: Each SKIP marker references a specific SQL feature. When a feature change is implemented, grep for its name in skip markers to find newly-unblocked queries.

**[Data generation time]** -> CTAS for 24 tables at SF10 may take several minutes. **Mitigation**: Default to SF1 for development. Docker Compose health checks ensure seed service waits for dependencies. Data persists in MinIO volumes across restarts.

**[Query adaptation accuracy]** -> TPC-DS queries from the spec may need syntax adjustments beyond just SKIP markers. **Mitigation**: Use Trino's own TPC-DS query set as the reference, since Trino's syntax is close to arneb's. Validate all non-skipped queries against Trino first.
