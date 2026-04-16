## 1. ScanContext Extension

- [x] 1.1 Add `filter: Option<PlanExpr>` field to `ScanContext` in execution crate
- [x] 1.2 Add `batch_size: Option<usize>` field to `ScanContext`
- [x] 1.3 Update all existing ScanContext construction sites to pass None for new fields
- [x] 1.4 Update physical planner to pass WHERE clause filter to ScanContext when available

## 2. Row Group Pruning

- [x] 2.1 Implement `can_prune_row_group(statistics, filter) -> bool` function for simple comparisons
- [x] 2.2 Support pruning for types: Int32, Int64, Float64, Utf8, Date32, Decimal128, Timestamp
- [x] 2.3 Support pruning operators: =, !=, <, <=, >, >=
- [x] 2.4 Support AND conjunctions (all conditions must match for inclusion)
- [x] 2.5 Integrate row group pruning into `ParquetDataSource::scan()` in file connector
- [x] 2.6 Integrate row group pruning into `HiveDataSource::scan()`
- [x] 2.7 Add debug logging for pruned vs. read row groups
- [x] 2.8 Write unit tests: row group with stats outside filter range is skipped
- [x] 2.9 Write unit tests: row group with stats inside filter range is read
- [x] 2.10 Write unit tests: row group with no stats is always read

## 3. Predicate Pushdown

- [x] 3.1 Implement `PlanExpr -> ArrowPredicate` translation for simple column comparisons
- [x] 3.2 Handle type conversions (Arneb types to Arrow types for predicate)
- [x] 3.3 Integrate predicate pushdown via `with_row_filter()` in file connector
- [x] 3.4 Integrate predicate pushdown via `with_row_filter()` in Hive data source
- [x] 3.5 Unsupported expressions fall through to in-memory filtering (no error)
- [x] 3.6 Write correctness tests: compare results with and without pushdown
- [x] 3.7 Write TPC-H regression test: Q6 (single-table filter-heavy query) results unchanged

## 4. Batch Size Configuration

- [x] 4.1 Apply `batch_size` from ScanContext to `builder.with_batch_size()` in file connector
- [x] 4.2 Apply `batch_size` from ScanContext to builder in Hive data source
- [x] 4.3 Write test: custom batch size produces correct results

## 5. Benchmarking

- [x] 5.1 Benchmark TPC-H Q6 (date range filter on lineitem) with and without row group pruning (requires Docker + seeded data)
- [x] 5.2 Benchmark TPC-H Q1 (date filter) with and without predicate pushdown (requires Docker + seeded data)
- [x] 5.3 Document performance improvement in README or benchmark report
