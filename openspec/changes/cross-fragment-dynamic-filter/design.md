## Context

arneb already runs distributed: PlanFragmenter splits a logical plan into N fragments separated by `ExchangeNode` boundaries; each fragment becomes a stage; each stage runs one or more tasks across workers. Same-fragment dynamic filter exists today: after a HashJoin build phase finishes (hash_join.rs:2281), the operator emits a `PlanExpr::InList` and walks the LEFT subtree to inject it into the matching `ScanExec` (operator.rs:347). The InList survives until scan time, where Parquet row-group pruning consumes it (parquet_pushdown.rs:90).

The same-fragment path covers in-memory broadcast joins (whole join lives in one fragment), but **every partitioned equi-join cuts the join into ≥2 fragments** — the partitioned probe scan lives in a child fragment that runs on a different worker, never seeing the parent fragment's InList. SF10 Q09's six-way join is the textbook case: the lineitem probe is the parent of three partition exchanges from supplier/part/orders, and arneb scans all 60 M rows because no DF reaches it.

Trino solved this with a coordinator-side `DynamicFilterService` plus piggybacked DF transport on existing task RPCs (PR #5183). The Trino reference is `core/trino-main/src/main/java/io/trino/server/DynamicFilterService.java` (lines 116, 127, 261, 354, 370, 386, 660, 881, 1031).

arneb's port is smaller than Trino's because **the in-worker pieces already exist** — only the cross-fragment plumbing is missing.

## Goals / Non-Goals

**Goals**:

- Cross-fragment DF for INNER and SEMI (build-side small) joins.
- Coordinator merges per-task partial DFs; probe-side scan awaits the merged Domain with a per-query timeout.
- Best-effort: timeout-or-arrival; no scan ever aborts because a DF didn't show up. Worst case: behave as today (read all rows that pass static filters).
- SF10 Q09 ≥ 0.95× Trino latency, ≤ 6 GB peak (currently 0.50× and 8.1 GB).
- No regression on the 21 already-winning SF10 queries (gate: cell-by-cell trino-diff 17q ✓ at 1e-9 + 22-query latency/memory bench unchanged within ±10%).

**Non-Goals**:

- Broadcast joins via a new BROADCAST partitioning strategy. arneb's Selinger DP already keeps small builds on the build side; cross-fragment DF reduces the probe-side cost regardless of join strategy. A separate A2 change covers broadcast.
- LEFT/RIGHT/FULL OUTER joins. Trino restricts DF to INNER+RIGHT for soundness reasons (the build side carries the "exists" semantics); we do the same.
- AntiJoin (NOT IN / NOT EXISTS). Build side is the "should-be-absent" set, semantically opposite of what a DF should advertise.
- Persistent DF state across queries. DFs are query-scoped and freed at query end.
- Worker-to-worker DF traffic. All DF flows through coord (same as Trino).
- Dynamic re-planning if a DF arrives mid-scan. The scan blocks at most until timeout, then proceeds with whatever it has.

## Decisions

### D1: DynamicFilterId allocation at plan time

**Choice**: A new `DynamicFilterId(u32)` newtype in `crates/common`, allocated by a planner-scoped `DynamicFilterIdAllocator`. During `JoinReorder` (after the join graph is finalized), walk every INNER/SEMI join and allocate one DF id per equi-key pair where the build side's estimated cardinality is below the partitioned-build cap (default 20,000 distinct values, mirroring Trino's `partitionedMaxDistinctValuesPerDriver`). Attach the id to the join's `dynamic_filter_ids: Vec<(DynamicFilterId, ColumnRef, ColumnRef)>` (build column, probe column) and to the matching probe-side `Scan`'s `dynamic_filters_consumed: Vec<(DynamicFilterId, ColumnRef)>`.

**Rationale**: Plan-time is the only point that sees the full join graph and can match build-side join keys to probe-side scan columns. Trino does the same in `PredicatePushDown.visitJoin` (lines 1008–1055). Fragmenter then naturally distributes these annotations to the right fragments — the join's annotation goes with the join (parent fragment), the scan's annotation goes with the scan (child fragment).

**Alternative**: Allocate at fragmentation time. Rejected — fragmentation has already lost the inter-fragment correspondence; matching probe-scan to build-join across fragments would require re-walking the original logical plan.

### D2: Coordinator-side `DynamicFilterService` is per-query, in-process

**Choice**: Add `crates/scheduler/src/dynamic_filter.rs`. The service is owned by `QueryCoordinator` (created at query start, dropped at query end). Internal shape:

```rust
struct DynamicFilterService {
    states: HashMap<DynamicFilterId, DfState>,
    stage_partition_counts: HashMap<StageId, u32>,  // how many tasks must report
}
struct DfState {
    expected_partitions: u32,
    reported_partitions: u32,
    accumulated: Vec<Domain>,            // one per reporting task
    resolved: Option<Domain>,             // intersected, ready to push
    notify: Vec<oneshot::Sender<Domain>>, // workers waiting on this DF
}
```

Workers report their partial Domain via the existing task-completion path (Flight `register_buffer` already runs at end of a task; piggyback the DF payload there). When `reported_partitions == expected_partitions` for a DF, intersect into a final `Domain` and fire all pending `notify` channels.

**Rationale**: Per-query lifetime avoids cross-query contamination. In-process avoids a new RPC service or persistent store. Mirrors Trino's `DynamicFilterContext` lifecycle (one per query, in `DynamicFilterService`).

**Alternative**: A standalone coordinator service shared across queries. Rejected — adds lifecycle complexity (allocator, eviction) for no clear benefit; query-scoped is simpler.

### D3: Piggyback DF payloads on existing Flight RPCs (no new endpoints)

**Choice**: Two existing Flight paths gain optional fields.

1. **Worker → coord (task completion)**: when a task ending with a HashJoin or SemiJoin build emits a DF, the existing task-completion Flight call carries `produced_dynamic_filters: Vec<(DynamicFilterId, SerializedDomain)>`. Coord routes them into `DynamicFilterService::report_partition`.

2. **Coord → worker (task submission)**: the existing `submit_task` Flight Action's `TaskDescriptor` adds `pending_dynamic_filters: Vec<(DynamicFilterId, SerializedDomain)>` for DFs that resolved BEFORE the task was dispatched (the common case when the build-side stage finishes before the probe-side stage starts).

3. **Coord → worker (late arrival)**: for DFs that resolve AFTER a task is already running, the existing heartbeat response (or a new lightweight `notify_dynamic_filter` Flight Action — preferred) carries the resolved Domain. The worker's `DynamicFilterCollector` fires the matching `oneshot`.

**Rationale**: Extending existing RPC payloads is lower risk than adding new endpoints; serde-driven JSON additions are backward-compatible. Trino uses the same piggyback pattern (PR #5183).

**Alternative**: New gRPC service dedicated to DF. Rejected — adds a service, port, lifecycle, retry/timeout policy for what is fundamentally metadata flow already running over Flight.

### D4: Scan-side async wait with per-query timeout

**Choice**: `ScanExec::dynamic_filters` is retained for SAME-fragment InList push (the existing fast path). A new `DynamicFilterCollector` (per task) is added, holding `Map<DynamicFilterId, oneshot::Receiver<Domain>>`. `ScanExec::execute` does:

```rust
// pseudo
let mut filters = self.same_fragment_dfs.lock().clone();
for df_id in self.cross_fragment_dfs {
    let recv = self.collector.take_receiver(df_id);
    match tokio::time::timeout(self.df_timeout, recv).await {
        Ok(Ok(domain)) => filters.push(domain.to_plan_expr(df_id.column())),
        _ => { /* timeout or coord dropped — proceed without */ }
    }
}
self.source.scan(filters, ...)
```

Default timeout: 10 s (mirrors Trino's default `dynamic_filtering_wait_timeout`). Configurable per query via `SET dynamic_filtering_wait_timeout = '5s'`. If the DF doesn't arrive in time, scan proceeds with the static filters only — soundness is preserved (filter is best-effort, not load-bearing).

**Rationale**: `tokio::time::timeout(oneshot::Receiver)` is the standard pattern; arneb already uses oneshot elsewhere. Soundness fallback (no DF → no filter) matches Trino's design.

**Alternative**: Block the scan indefinitely until DF arrives. Rejected — a single failed DF would deadlock the query.

### D5: Domain representation — distinct values → min/max → all

**Choice**: A new `crates/common/src/domain.rs`:

```rust
enum Domain {
    DistinctValues(Vec<ScalarValue>),  // |values| < cap
    Range { min: ScalarValue, max: ScalarValue, nullable: bool },
    All,  // no-op filter
}
```

`JoinDomainBuilder` collects distinct values until either |values| > `max_distinct_values_per_partition` (default 20,000) or accumulated byte size > `max_size_per_partition_bytes` (default 200 KB). On overflow, fall back to min/max range. If min/max collection also overflows (row count > `range_row_limit_per_partition` default 30,000), downgrade to `Domain::All`. Mirror Trino's two-tier degradation (DynamicFilterConfig.java:120-157, partitioned limits).

Intersection on coord side: `DistinctValues ∩ DistinctValues = DistinctValues(set-intersect)`; `DistinctValues ∩ Range = filter the values by range`; `Range ∩ Range = max(mins)..min(maxes)`; `All ∩ X = X`.

**Rationale**: Matches Trino's `JoinDomainBuilder` behavior 1:1 — we already know the cap policy works at scale. The InList→Range degradation is what keeps Q21-shape queries (many discrete probe values) from blowing up memory.

**Alternative**: Bloom filters as the fallback instead of min/max. Rejected for now — adds a new dependency (`bloomfilter`) and a different scan-side codepath; min/max already integrates with Parquet row-group statistics for free.

### D6: Phase ordering — annotation → service → scan-wait → emit → integration

**Choice**: Six phases, in this order:

| Phase | What | LOC | Sessions | Gate |
|---|---|---|---|---|
| A1.1 | DynamicFilterId + Domain + plan annotation, no-op runtime | ~100 | 0.3 | Compiles + clippy + existing tests pass |
| A1.2 | `DynamicFilterService` standalone unit tests | ~200 | 0.5 | New unit tests pass; existing tests unchanged |
| A1.3 | RPC plumbing — payloads added, no consumer yet | ~250 | 0.5 | RPC roundtrip test; existing distributed bench unchanged |
| A1.4 | Scan-side async wait, behind `enable_dynamic_filtering` session param defaulting OFF | ~200 | 0.5 | SF1 trino-diff 17/17 ✓ with flag OFF; flag ON has same correctness |
| A1.5 | HJ/SemiJoin build → coord emit hook, flag still OFF by default | ~100 | 0.2 | SF1 trino-diff 17/17 ✓ with flag ON; bench unchanged with flag OFF |
| A1.6 | Flip flag to default ON; SF10 Q09 measurement | ~50 | 0.2 | SF10 22q bench: Q09 < 12 s AND no other query regresses > 10% |

Each phase commits independently and is reversible. The flag stays OFF until A1.5 lands and A1.6's gate passes.

**Rationale**: Each phase has an independent test. The flag gives an emergency revert that doesn't require a code rollback. Phase A1.6's gate prevents shipping if Q09 doesn't win OR if anything else regresses.

## Risks / Trade-offs

**[10 s default timeout]** → A slow build that runs > 10 s pushes the probe to scan the full table. **Mitigation**: SF10 build phases for queries with cap-eligible DFs (small dimensions) complete in < 2 s in the bench. The 10 s default is generous. Per-query override available.

**[Probe scan starts before DF arrives in some plans]** → If `submit_task` for the probe stage races ahead of the build stage's task-completion, the probe scan opens with no DF, awaits the `oneshot`, times out at 10 s. **Mitigation**: Schedule probe stages AFTER their build-side counterparts when a cross-fragment DF binds them. The existing stage dependency in `coordinator.rs` already does this for ExchangeNode; the DF dependency is a strict subset.

**[Coord becomes a memory bottleneck for very wide queries]** → A 10-join query with 10 DFs accumulates 10 Domains × N partitions on coord. **Mitigation**: `max_size_per_filter` cap (default 10 MB) bounds the per-DF accumulated size; over-cap downgrades to `Domain::All`. Trino has the same defense (DynamicFilterConfig.java:169).

**[Partial DF correctness]** → Per-partition Domains can be inconsistent if the partitioner hashes a join key collision across partitions. **Mitigation**: Intersection of per-partition DistinctValues yields the union when partitioned by the join key (any join key value lives in exactly one partition). Coord's merge logic must union, not intersect, distinct-values per partition. Range merges as `min(mins)..max(maxes)`.

**[Schema mismatch after fragment column pruning]** → Cross-fragment column pruning (commit 9e2a620) rewrites schemas at fragment boundaries. A DF id allocated at plan time may refer to a column not present in the pruned fragment's schema. **Mitigation**: Pruning pass must preserve any column referenced by a `dynamic_filters_consumed` annotation on a child scan; treat the DF column as live for the column-pruning analyzer.

**[Backwards compatibility]** → Older workers running pre-DF code will ignore new RPC fields gracefully (serde `#[serde(default)]`). Mixed-version clusters are unsupported during this rollout; deployments roll all nodes together.

## Open Questions

- **Q1**: Should `dynamic_filter_ids` annotations be visible in `EXPLAIN`? Useful for debugging, but invisible in Trino's `EXPLAIN` (only `EXPLAIN ANALYZE` shows DF metrics). Lean toward: add to `EXPLAIN ANALYZE` only, after A1.6 lands.

- **Q2**: Should we expose a per-DF metric (rows filtered, wait time) via `arneb::profile`? Trino exposes `dynamicFiltersStats` in EXPLAIN ANALYZE. Lean toward: yes, add to the `arneb::profile` target in A1.6 — useful for verifying the feature actually fires on Q09.

- **Q3**: Cap defaults — Trino partitioned: 20K distinct, 200 KB size, 30K range. arneb uses the same? Or tighter for arneb's tighter cgroup target? Lean toward: start with Trino defaults; tune in A1.6 if Q09 measurement says otherwise.
