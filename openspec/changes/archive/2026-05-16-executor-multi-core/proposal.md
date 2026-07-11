## Why

Arneb runs each query **single-threaded** within the standalone /
coordinator role. Phase-1 explore agent verified:

> No `tokio::spawn` or `rayon` in `crates/execution/src/operator.rs`
> (lines 1–960): all operators use `async fn execute()` that return
> a single `SendableRecordBatchStream`. No per-partition parallelism.
> `ScanExec::execute()` calls `source.scan()` once. `HiveDataSource::scan()`
> reads all Parquet files sequentially in a single `for` loop.
> Distributed mode exists but is opt-in via `--role coordinator|worker`;
> default is standalone/single-threaded.

The machine running this work has 14 cores. Tokio's multi-thread
runtime spins up ~15 OS threads — but Arneb only drives one of them
per query. **13 of 14 cores sit idle on every TPC-H query.**

Trino, by contrast, drives every available core: splits scan in
parallel, parallelizes hash-join probe per partition, runs
multi-stage pipeline graph with worker pool concurrency. The
single-thread gap accounts for an estimated **2-8× wall-clock
difference** depending on query shape, on top of the join-order
gap covered by `planner-join-reorder`.

This change scopes "the second big lever": teach Arneb to
parallelize execution within a single query across all available
cores in the same process. It is the on-machine counterpart to
the existing coordinator/worker distributed mode — same partitioning
idea, different fabric.

## What Changes

1. **Partitioning concept in physical plan.**
   - Introduce `Partitioning` enum (`UnknownPartitioning(n)`,
     `RoundRobinBatch(n)`, `Hash(Vec<PlanExpr>, n)`) attached to
     every `ExecutionPlan` via a new
     `fn output_partitioning(&self) -> Partitioning` method.
   - Each operator declares both its required input partitioning
     (`required_input_partitioning(&self) -> Vec<Partitioning>`) and
     its own output partitioning.
2. **Repartition operator.**
   - New `RepartitionExec` that fans batches across N partitions
     using either round-robin (default for scans → join-probe) or
     hash partitioning (for shuffle-style joins / aggregates).
   - Implemented with `tokio::sync::mpsc` channels per partition;
     producer task fans out, consumer tasks pull.
3. **Per-partition execution.**
   - `ExecutionPlan::execute(partition: usize) -> SendableRecordBatchStream`
     replaces the current parameterless `execute()`. Each partition
     index drives one independent stream.
   - Top-level query executor spawns N tasks (default
     `num_cpus::get()`) over the root operator's partitions and
     merges via a final `CoalescePartitionsExec`.
4. **Operator parallelization.**
   - `ScanExec`: file connectors return N independent streams (one
     per file or one per row-group group). HMS / S3 supports
     concurrent reads natively.
   - `FilterExec`, `ProjectionExec`: trivially per-partition (already
     stateless).
   - `HashJoinExec`: build side collects all partitions into one
     hash table; probe runs per-partition over the left input.
     (Multi-build-side parallel join is a separate stage 2.)
   - `HashAggregateExec`: each partition builds a partial hash map;
     a final `MergePartialAggregateExec` consolidates. For
     low-cardinality groupings this is unambiguously a win; for
     high-cardinality, partial maps grow large — falls back to a
     single-partition aggregate when cardinality estimates suggest
     this is cheaper. (Estimates come from `planner-join-reorder`'s
     cost model.)
   - `SortExec`: each partition sorts independently; a
     `SortMergeExec` k-way merges. Or `lexsort + take` on the union
     if N is small.
   - `LimitExec`: each partition limits to N, final merge keeps
     first N.
5. **Configuration.**
   - `[execution]` block in arneb.toml:
     - `target_partitions: usize` (default `num_cpus::get()`)
     - `parallel_file_scan: bool` (default `true`)
     - `parallel_hash_aggregate: bool` (default `true`)
   - CLI override: `--target-partitions=N`.
6. **Cooperation with distributed mode.**
   - Coordinator/worker split (existing) still runs *one fragment
     per worker*; this change parallelizes within each fragment.
     The two layers compose: 8 workers × 14 cores each = 112-way
     parallelism on a cluster of 8 nodes.

## Capabilities

### New Capabilities

- `partitioning`: physical-plan partitioning metadata + required-input
  enforcement.
- `repartition-exec`: `RepartitionExec` operator with round-robin
  and hash variants.
- `parallel-aggregate`: partial + merge aggregation across partitions.
- `parallel-sort`: per-partition sort + k-way merge.

### Modified Capabilities

- `execution-operators`: `ExecutionPlan::execute()` signature gains
  `partition: usize`. Every operator implements per-partition
  semantics.
- `physical-planner`: inserts `RepartitionExec` / `CoalescePartitionsExec`
  to satisfy operator partitioning requirements.
- `datasource`: `DataSource::scan(&self, ctx, partition: usize)`.
  Connector reports `partition_count()`. File connector defaults to
  one partition per file (row-group splitting in v2).
- `hive-data-source`: gains per-file partition split.
- `pg-server` / `protocol`: result stream becomes a merge of N
  partitions before encoding to the client.

## Impact

- **Behavior**: identical observable output (same rows in same order
  *only when ORDER BY is explicit*). Without ORDER BY, the
  row order may change vs single-thread execution — this is
  standard for parallel SQL engines but needs to be called out
  in release notes.
- **Memory**: per-partition state multiplies. Hash aggregate uses
  N hash maps (N × small per-partition cardinality), then merges.
  For SF1 typical cardinalities ≤ 200K groups, total memory grows
  ~Nx — acceptable on a 32 GB machine. Future work: spill-to-disk.
- **Tests**: extensive new test suite for `RepartitionExec`, per-
  partition correctness of every operator, plus end-to-end
  `trino-diff` 16/16 at 1e-9 (with ORDER BY in every diff-able
  query — which the existing TPC-H queries already have).
- **Performance**: target **≥ 4× speedup** on SF1 geomean over the
  current post-quick-wins baseline (0.165× of Trino → ~0.66× of
  Trino). Hot-loop queries (Q01 scan/aggregate, Q06 filter-heavy)
  should be near-linear in core count.
- **Effort**: estimated 6–8 weeks for a single engineer. Largest
  uncertainty: backpressure across partition boundaries through
  mpsc channels. The current `SendableRecordBatchStream` is
  pull-based; the new fabric must preserve that or risk OOM on
  large queries.
- **Dependencies**: zero new crates. Reuses tokio's multi-thread
  runtime + mpsc channels.
- **Composes with the join-reorder change**: that change reduces
  *work per query*; this change reduces *time per unit work*. They
  multiply: a query that does 5× less work (reorder) at 4× core
  count (parallelism) gets 20× faster.
- **Out of scope**:
  - Spill-to-disk for memory-bound aggregates / sorts.
  - Adaptive partitioning (re-partition mid-query based on data
    skew). Future stage 3.
  - GPU offload.
  - Vectorized expression eval / SIMD (covered by future
    `vectorized-expression` change).
  - Cross-machine NUMA awareness.

## Open questions for design.md

- **API boundary**: is `execute(partition: usize)` cleaner, or should
  partitions be implicit inside the stream? Two options have wildly
  different blast radius.
- **Hash aggregate merge cost**: at high cardinality, merging N
  partial hash maps is itself a hash join. Worth instrumenting
  before committing — may need partition-pruning aggregate.
- **Cardinality estimate threshold** below which we serialize: TBD,
  needs SF10 measurements.
- **Sort merge vs concat+sort**: for small N (≤4), `concat_batches`
  + single `lexsort` may beat k-way merge. Bench both.
- **Default `target_partitions`**: `num_cpus::get()` is the obvious
  pick but contended workloads (Web UI + benchmark) may want a
  lower default.

## Status

**DRAFT** — proposal only. No implementation in this change yet.
Sister proposal to `planner-join-reorder`; the two together are the
critical-path work for closing the Arneb-vs-Trino gap on TPC-H.
Implementation priority: do `planner-join-reorder` first (smaller
blast radius, easier rollback, immediate 3-5× on multi-join), then
this change. Composing both targets a final 12-20× speedup over
today's `exec-typed-hash-keys` end state.
