## Context

Arneb's execution engine today is single-task per query. Every
`ExecutionPlan::execute()` returns one `SendableRecordBatchStream`
that's polled on a single tokio task. With 14 cores available, that
leaves 13 idle. The job for this design is the architecture of a
shift to partition-parallel execution within a query, preserving the
existing stream contract and the existing distributed-mode boundary.

Both Trino and DataFusion arrived at similar shapes for this: a
`Partitioning` annotation on every physical operator, a
`RepartitionExec` that fans out via channels, and an `execute(partition: usize)`
API. The DataFusion model is the closest reference and worth
re-reading for inspiration — but its operator catalog is much larger
than Arneb's, so we get to pick a simpler surface.

## Goals / Non-Goals

**Goals:**
- N-way parallel execution within a single query, where N defaults
  to `num_cpus::get()`.
- Preserve the existing `ExecutionPlan` trait API as closely as
  possible — minimize blast radius across operator implementations.
- Backpressure works across partitions: a slow consumer must slow
  down upstream producers, not OOM the engine.
- Compose with distributed mode without conflict.
- Maintain `trino-diff` 16/16 correctness through the entire
  migration.

**Non-Goals:**
- Spill-to-disk. Memory pressure becomes more real with parallel
  partitions; defer formal spilling to a follow-up.
- Adaptive partitioning. Initial partition count is plan-time
  static. Re-balancing mid-query is future work.
- Vectorized expression eval / SIMD. Orthogonal.
- Multi-build-side parallel hash join. Build side stays single in
  v1 (joins are still parallel on the probe side).
- Aggregate cardinality estimation for partition-pruning. Defer.

## Decisions

### Decision 1: API — `execute(partition: usize)`

Current:
```rust
async fn execute(&self) -> Result<SendableRecordBatchStream, ExecutionError>;
```

Proposed:
```rust
fn output_partitioning(&self) -> Partitioning;
async fn execute(
    &self,
    partition: usize,
) -> Result<SendableRecordBatchStream, ExecutionError>;
```

The top-level runner (in `crates/server/src/query_runner.rs` and the
pgwire backend) spawns N tasks via `tokio::spawn`, each calling
`root.execute(partition_i)`, and merges via a single
`CoalescePartitionsExec` that reads from all of them.

**Alternative considered**: implicit partitioning inside the stream
itself (one stream → N parallel readers via shared state). Rejected:
much harder to reason about back-pressure and operator composition;
DataFusion specifically moved away from this in early designs.

### Decision 2: `Partitioning` enum

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Partitioning {
    /// `n` partitions, no order guarantee within or across them.
    UnknownPartitioning(usize),
    /// `n` partitions filled round-robin from upstream batches.
    /// Useful as a load-balancer.
    RoundRobinBatch(usize),
    /// `n` partitions where rows with the same hash of the given
    /// expressions land in the same partition. Required for
    /// shuffle joins and partitioned aggregates.
    Hash(Vec<PlanExpr>, usize),
}
```

Each operator declares:

- `fn output_partitioning(&self) -> Partitioning`
- `fn required_input_partitioning(&self) -> Vec<Partitioning>` —
  one entry per input child; specifies the partitioning shape this
  operator needs.

The physical planner inserts `RepartitionExec` whenever a child's
output partitioning doesn't satisfy the parent's requirement.

### Decision 3: `RepartitionExec` and `CoalescePartitionsExec`

`RepartitionExec`:
- Spawns one task per input partition that reads its source and
  routes each batch to the appropriate output partition channel
  (mpsc::channel with bounded capacity, e.g., 4 batches).
- For `RoundRobinBatch`: counter-mod-N.
- For `Hash`: evaluate the partition expression on each batch's
  rows, route per-row by hash modulo N. Requires per-partition
  row-batch building.
- Bounded channel size enforces backpressure: when a downstream
  partition is slow, its channel fills, the router blocks, and
  upstream producers stall too. Standard tokio mpsc semantics.

`CoalescePartitionsExec`:
- Takes N input streams, returns one stream that emits batches in
  arbitrary order from any of them (whichever has a batch ready).
- Implemented with `futures::stream::select_all` or equivalent.
- Used as the top-level merge before the final encoder.

### Decision 4: Operator parallelization plan

| Operator         | Partition behavior |
|------------------|---------------------|
| `ScanExec` (file) | One partition per file, or per row-group group |
| `ProjectionExec` | Per-partition (stateless) |
| `FilterExec`     | Per-partition (stateless) |
| `LimitExec`      | Each partition limits to `n`; final merge keeps first `n` |
| `SortExec`       | Per-partition sort + k-way merge in `CoalescePartitionsExec` (or concat+sort for small N) |
| `HashJoinExec`   | Build side coalesces to one partition first (single hash table); probe side runs per-partition. Future: partitioned build for shuffle joins. |
| `HashAggregateExec` | Two-phase: per-partition `PartialAggregate` → repartition by group key → `FinalAggregate` per partition. |
| `NestedLoopJoinExec` | Coalesce both sides first; the cross-product is O(LR) and unsuited to partitioning. |
| `WindowExec`     | Requires partition expression matches window's `PARTITION BY`; otherwise coalesce. |
| `DistinctExec`   | Repartition by all columns; per-partition dedup; coalesce. |

### Decision 5: Backpressure budget

Default channel capacity: 4 batches per partition (~32K rows per
batch × 4 batches per channel × N partitions ≈ comfortable on a
32 GB machine). Configurable via `[execution] channel_capacity = 4`.

For sort and aggregate paths that materialize everything, the
backpressure model degrades to "consume whole input before
producing output" — same as today's single-thread behavior. The
parallelism gain comes from parallel scan and parallel probe, not
parallel materialization (which is bounded by memory).

### Decision 6: Determinism

Within a partition, order is preserved. Across partitions, the
final merge in `CoalescePartitionsExec` makes no guarantee. SQL
without `ORDER BY` is unordered; SQL with `ORDER BY` either runs
`SortExec` at the top (preserving order through k-way merge) or
already establishes order before coalesce. Both paths are
deterministic given the same input partitioning + same data.

`trino-diff` queries all use `ORDER BY`, so 16/16 verification
remains valid.

### Decision 7: Default `target_partitions`

`num_cpus::get()`. For a 14-core machine, that's 14 partitions.

Tuning knobs:
- `[execution] target_partitions = 14` in arneb.toml
- `--target-partitions=14` CLI
- Env: `ARNEB_TARGET_PARTITIONS=14`

On contention with concurrent web UI / other servers, advise
lowering to `min(num_cpus, query_concurrency)`.

### Decision 8: Composition with distributed mode

The existing coordinator/worker split runs ONE plan fragment per
worker. After this change, EACH worker fragment additionally runs
its operators with N-way intra-fragment parallelism. Concretely:
the worker fragment's root `Repartition` boundary becomes the
network-shuffle boundary; intra-worker `Repartition` is local mpsc.
The two boundaries don't conflict — they're nested.

## Test Strategy

- **Unit**: `RepartitionExec` correctness for round-robin and hash
  modes. `CoalescePartitionsExec` non-determinism (any-order is
  OK). Per-operator partition semantics (filter, project, limit,
  sort, aggregate).
- **Property test**: random plans → output set equality between
  N=1 and N=8 execution. Run with quickcheck-style harness.
- **Integration**: `trino-diff` 16/16 at 1e-9 — must remain green.
- **Benchmark**: TPC-H SF1 at 1, 2, 4, 8, 14 partitions to show
  the speedup curve and identify the saturation point.

## Risks

- **Deadlocks via mpsc cycles.** `RepartitionExec` channels must
  form a DAG, not a cycle. Plan construction must enforce
  acyclicity — easy by construction in a tree but worth a debug
  assert.
- **Unbounded memory growth on aggregate.** Partial hash maps × N
  partitions × no spill = OOM on high cardinality. Mitigation:
  fall back to single-partition aggregate when estimated cardinality
  > threshold. Estimate comes from `planner-join-reorder`'s cost
  model. (Hence the recommended sequencing — join-reorder first.)
- **Latency for small queries.** Spawning 14 tasks for a query
  that scans 25 rows is overhead. Adaptive partitioning helps but
  is out of scope; for now, `target_partitions=1` mode disables
  parallelism for users who prioritize low latency.
- **Backpressure subtleties.** Bounded mpsc channels block, which
  in tokio means task yields. If a producer task panics
  mid-batch, the channel close must propagate cleanly to all
  consumers. Standard tokio behavior, but worth a test fixture.
- **Distributed-mode regression.** This change should leave the
  distributed path unchanged at the boundary. CI must include a
  3-worker integration test to verify.

## Rollback

Disabling intra-query parallelism: set `[execution] target_partitions
= 1`. The plan still has `RepartitionExec` nodes, but each fans out
to 1 partition (pass-through). Performance reverts to single-thread
levels. Removing the operator entirely is a much larger revert and
not advised.

## Implementation phasing

Suggested PR sequence:

1. **`partitioning-types`** (small): add `Partitioning` enum,
   `output_partitioning` / `required_input_partitioning` methods to
   trait, default everything to `UnknownPartitioning(1)`. Zero
   behavior change.
2. **`coalesce-and-repartition`** (medium): `CoalescePartitionsExec`
   + `RoundRobinBatch`-only `RepartitionExec`. Plumb through
   physical planner. Default partition count stays 1.
3. **`parallel-scan`** (medium): file connector partitions per file.
   Top-level runner spawns N tasks. `trino-diff` 16/16 stays green.
4. **`hash-repartition`** (medium): add `Hash` variant to
   `RepartitionExec`.
5. **`parallel-aggregate`** (large): two-phase partial+final
   aggregate, with cardinality-driven fallback.
6. **`parallel-hash-join`** (large): probe side parallel; build
   side still coalesced.
7. ~~**`parallel-sort-limit-distinct`** (medium)~~ — **DEFERRED**
   (see "Post-implementation note: deferred 3.7" below).

Each PR independently shippable and bench-validatable. Total
6–8 weeks for one engineer (5–7 with #7 deferred).

## Post-implementation note: deferred 3.7 (parallel sort / limit / distinct / window)

**Date:** 2026-05-16. **Status:** Phase 3.7 was scoped at design
time as a required component of "multi-core executor". After
shipping Phases 3.1–3.6 plus the cross-cutting wins (column
pruning across joins, GroupByHash typed hot-path, dynamic-filter
caps, join-reorder partial-prefix fix, TopK collapse of
`Limit over Sort`), arneb beats Trino on all 16 TPC-H queries in
our benchmark set at ~3× geomean, **without 3.7's per-operator
parallelism**. Trino-source verification (paths below) confirmed
that even Trino does not lean on these mechanisms for default
single-node TPC-H. Phase 3.7's spec file
(`specs/parallel-sort-limit-distinct/spec.md`) was therefore
removed from this change rather than implemented.

Source evidence (Trino repo at
`/Users/bochengyang/formosa-ventures/repos/trino`):

- `core/trino-main/src/main/java/io/trino/operator/OrderByOperator.java`
  — `OrderByOperator` is a single-pass streaming sort over a
  `PagesIndex`, not a per-partition sort + k-way merge.
- `core/trino-main/.../operator/TopNOperator.java:32` plus
  `core/trino-main/.../sql/planner/optimizations/LimitPushDown.java:181`
  — Trino rewrites `Limit(N) over Sort(...)` to `TopNNode(N)`
  (heap of N). Arneb's `TopKExec` (`crates/execution/src/operator.rs`)
  matches this shape and already handles Q02/Q03/Q10.
- `core/trino-main/.../sql/planner/optimizations/AddLocalExchanges.java`
  lines 244 (limit), 300 (final limit), 318 (window) — Trino forces
  the final `LimitOperator` / `WindowOperator` onto `singleStream()`
  even in multi-driver tasks.
- `core/trino-main/.../operator/MarkDistinctOperator.java:33` —
  `MarkDistinctOperator` is a stateless row-marker; real dedup
  happens via aggregate. TPC-H Q16's `COUNT(DISTINCT)` lowers to
  the aggregate path (matches arneb's existing two-phase Aggregate).

**Reactivation criteria.** Re-open 3.7 (or a successor change) when
any of:

1. A target workload uses `SELECT DISTINCT` over high-cardinality
   columns whose pre-dedup row count exceeds ~1 M.
2. A target workload uses window functions over large partitions
   that benefit from per-Hash-partition parallel evaluation.
3. A target workload has unbounded `ORDER BY` (no `LIMIT`) over
   inputs that exceed a single-stream sort's memory budget, OR
   profiling shows the final sort dominating wall-clock for a
   query where Trino runs noticeably faster.

Phase 3.8's acceptance criteria in `tasks.md` were also rewritten
to match the actual shipping target (16/16 wins vs Trino, median
geomean ≤ 0.65× across ≥ 2 bench pairs) instead of the original
"4× scaling at target_partitions=14 vs =1" — that latter framing
assumed parallelism width was the dominant lever, which turned
out to be incorrect for SF1.
