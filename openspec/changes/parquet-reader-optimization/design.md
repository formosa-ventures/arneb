## Context

Arneb's Parquet reading path currently supports only column projection pushdown. The `ParquetRecordBatchStreamBuilder` is used in 4 locations (`crates/connectors/src/file.rs` lines 141, 185, 395 and `crates/hive/src/datasource.rs` line 114) but only `with_projection()` is called. The builder also supports `with_row_filter()`, `with_predicate()`, and `with_batch_size()` which are unused.

For TPC-H SF1, the `lineitem` table has ~6M rows. A query filtering on `l_shipdate` currently reads all rows. With row group pruning, if the file has 10 row groups and only 1 matches the date range, 90% of I/O is eliminated.

## Goals / Non-Goals

**Goals:**

- Row group pruning using min/max column statistics
- Predicate pushdown for simple comparisons (=, <, >, <=, >=, !=)
- Configurable batch size via ScanContext
- Apply optimizations to both file connector and Hive data source

**Non-Goals:**

- Complex predicate pushdown (OR, IN, LIKE, function calls)
- Bloom filter support (Parquet bloom filters are optional and rarely written)
- Column chunk encryption support
- Page-level filtering (page index — can be added later)
- Predicate pushdown for nested types
- Write-path optimizations (statistics generation)

## Decisions

### D1: Row group pruning via RowGroupMetaData statistics

**Choice**: Before reading, iterate over `ParquetRecordBatchStreamBuilder::metadata().row_groups()` and check each row group's column statistics against the filter predicate. Skip row groups where statistics prove no rows can match. Use `with_row_selection()` to specify which row groups to read.

**Rationale**: This is the standard approach used by DataFusion, DuckDB, and Spark. It requires no changes to the Parquet file — statistics are already present in standard Parquet files.

### D2: Filter expression representation for pushdown

**Choice**: Extend `ScanContext` with an optional filter expression (using the existing `PlanExpr` type from `crates/planner`). The physical planner passes the WHERE clause filter to `ScanContext` when creating scan operators. The data source translates supported expressions to Parquet predicates and leaves unsupported ones for in-memory filtering.

**Rationale**: Reuses the existing expression representation. The data source can inspect the expression tree and push down what it supports (simple column comparisons) while leaving complex expressions for the execution layer.

### D3: Predicate pushdown via ArrowPredicate

**Choice**: For pushed-down expressions, use parquet crate's `ArrowPredicate` trait with `RowFilter`. This evaluates predicates during Parquet decoding, before materializing full Arrow arrays.

**Rationale**: `ArrowPredicate` operates at the row level during decoding, which is more efficient than row group pruning alone. It combines with row group pruning for maximum I/O reduction.

### D4: Configurable batch size

**Choice**: Add `batch_size: Option<usize>` to `ScanContext`. When set, call `builder.with_batch_size(n)` on the ParquetRecordBatchStreamBuilder. Default remains 8192 (parquet crate default).

**Rationale**: Batch size affects memory usage and vectorization efficiency. Smaller batches reduce peak memory; larger batches improve throughput. Making it configurable allows tuning per workload.

## Risks / Trade-offs

**[Statistics availability]** -> Not all Parquet files have column statistics. **Mitigation**: Row group pruning is opportunistic — if statistics are absent, all row groups are read (current behavior).

**[Expression translation complexity]** -> Converting execution-layer expressions to Parquet predicates requires type-aware translation. **Mitigation**: Start with simple column comparisons (Column op Literal). Add support for conjunctions (AND) but not disjunctions (OR) initially.

**[Correctness risk]** -> Incorrect predicate pushdown can silently filter out valid rows. **Mitigation**: Comprehensive tests comparing pushed-down vs. in-memory filtering results. Benchmark with TPC-H queries that have date/numeric filters.

**[ScanContext API change]** -> Adding filter to ScanContext is a cross-cutting change. **Mitigation**: The field is `Option`, so existing code passes `None` and works unchanged.
