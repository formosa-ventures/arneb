## Phase A1.1 — DynamicFilterId + Domain + plan annotation (~100 LOC, 0.3 session)

Goal: new types compile and propagate through planner; runtime is a no-op (all DFs collected but never published).

- [ ] 1.1 Add `crates/common/src/dynamic_filter.rs` with `DynamicFilterId(u32)` newtype (Debug, Copy, Clone, Eq, Hash, Serialize, Deserialize)
- [ ] 1.2 Add `crates/common/src/domain.rs` with `Domain` enum (`DistinctValues(Vec<ScalarValue>)`, `Range { min, max, nullable }`, `All`) + `Domain::merge_union` (per-partition merge) + `Domain::merge_intersect` (per-DF coord merge across partitions)
- [ ] 1.3 Add `DynamicFilterIdAllocator` in `crates/planner/src/dynamic_filter.rs`: monotonic u32 counter, one allocator per `QueryPlanner` invocation
- [ ] 1.4 Extend `LogicalPlan::HashJoin` (and `LogicalPlan::SemiJoin`) with `dynamic_filter_ids: Vec<(DynamicFilterId, ColumnRef /*build*/, ColumnRef /*probe*/)>` (empty by default)
- [ ] 1.5 Extend `LogicalPlan::TableScan` (or the existing `Scan`-equivalent) with `dynamic_filters_consumed: Vec<(DynamicFilterId, ColumnRef)>` (empty by default)
- [ ] 1.6 Add planner pass `assign_dynamic_filter_ids` that runs AFTER JoinReorder: walk every INNER/SEMI join, allocate one id per equi-key where estimated build cardinality < `partitioned_max_distinct_values` (default 20,000), tag the join AND the matching probe-side scan
- [ ] 1.7 Update `PlanFragmenter` to preserve `dynamic_filter_ids` and `dynamic_filters_consumed` annotations across fragment boundaries (no actual cross-fragment hookup yet — annotations just survive)
- [ ] 1.8 Unit tests: `dynamic_filter::tests` for Domain merge correctness (DistinctValues∪Range, Range∩Range, All∩X); planner test that Q09's plan tree carries DF annotations on the lineitem scan
- [ ] 1.9 Verify cross-fragment column pruning (`crates/server/src/fragment_pruning.rs`) preserves columns referenced by `dynamic_filters_consumed` (add to the "live columns" set)
- [ ] 1.10 Run quality gate: `cargo fmt --check + clippy + cargo test --workspace --lib` all green; distributed-mode trino-diff 17q ✓ at 1e-9 (no behavior change yet — feature is inert)

## Phase A1.2 — DynamicFilterService standalone (~200 LOC, 0.5 session)

Goal: coord-side service has full API + unit tests; not yet wired into QueryCoordinator.

- [ ] 2.1 Create `crates/scheduler/src/dynamic_filter.rs` with `DynamicFilterService` struct (`states: HashMap<DfId, DfState>`, `stage_partition_counts: HashMap<StageId, u32>`)
- [ ] 2.2 Implement `register_query(filter_ids: &[(DfId, StageId, u32 expected_partitions)])` — initializes empty `DfState` entries
- [ ] 2.3 Implement `report_partition(df_id, partition_idx, partial_domain) -> Result<()>` — appends to `accumulated`; when `reported_partitions == expected_partitions`, merge-intersect and fire `notify` channels
- [ ] 2.4 Implement `subscribe(df_id) -> oneshot::Receiver<Domain>` — workers call this to wait on the merged Domain (single-shot)
- [ ] 2.5 Implement `drop_query()` — clears all state, drops pending receivers (which become `Err` on scan side → timeout fallback)
- [ ] 2.6 Unit tests: 5+ tests covering normal merge, late report after subscribe, subscribe after merge (fast path), partial Domain degradation (DistinctValues → All), drop with pending subscribers
- [ ] 2.7 Quality gate green

## Phase A1.3 — RPC payload extension (~250 LOC, 0.5 session)

Goal: Flight RPC carries DF metadata in both directions; coord routes worker reports into the service; nothing consumes the DFs yet (scan still ignores them).

- [ ] 3.1 Extend `TaskDescriptor` (`crates/rpc/src/task_descriptor.rs:36`) with `pending_dynamic_filters: Vec<(DfId, SerializedDomain)>` (serde `#[serde(default)]` for back-compat)
- [ ] 3.2 Extend task-completion Flight payload with `produced_dynamic_filters: Vec<(DfId, SerializedDomain)>`
- [ ] 3.3 Add new Flight Action `notify_dynamic_filter(QueryId, DfId, SerializedDomain)` — coord→worker for DFs that resolve AFTER task already started
- [ ] 3.4 Coord side: `QueryCoordinator` instantiates `DynamicFilterService` at query start; routes worker `produced_dynamic_filters` to `report_partition`; when service fires, push to all subscribed worker tasks via `notify_dynamic_filter`
- [ ] 3.5 Worker side: receive `pending_dynamic_filters` from `TaskDescriptor`; receive late arrivals from `notify_dynamic_filter`; both routed to a per-task `DynamicFilterCollector` (just stores them; consumers added next phase)
- [ ] 3.6 RPC roundtrip integration test: coord sends a fake DF via `submit_task` → worker `DynamicFilterCollector` has it; worker reports a fake DF via task-completion → coord's `DynamicFilterService` records it
- [ ] 3.7 Quality gate green; distributed bench unchanged within ±2% latency/memory

## Phase A1.4 — Scan-side async wait (~200 LOC, 0.5 session)

Goal: scan operators await cross-fragment DFs with timeout; flag-gated OFF by default; behavior identical to today when flag is OFF.

- [ ] 4.1 Add session params `enable_dynamic_filtering: bool` (default `false`, will flip to `true` in A1.6) and `dynamic_filtering_wait_timeout: Duration` (default 10s)
- [ ] 4.2 Add `DynamicFilterCollector` per task in `crates/execution/src/`: `Map<DfId, Either<oneshot::Receiver<Domain>, Domain>>` (either a pending receiver or an already-arrived Domain)
- [ ] 4.3 Wire `ExecutionContext` to thread the per-task `DynamicFilterCollector` down to `ScanExec` instances
- [ ] 4.4 Modify `ScanExec::execute` to, when `enable_dynamic_filtering=true` AND `dynamic_filters_consumed` is non-empty: for each DF id, take the receiver from the collector and `tokio::time::timeout(timeout, recv).await`. On `Ok(Ok(domain))`, convert `Domain → PlanExpr::InList` (or comparable for Range) and append to `dynamic_filters`. On `Err` or timeout, log and proceed with what's already there.
- [ ] 4.5 Domain → PlanExpr conversion: `DistinctValues(vs) → InList`, `Range{min,max} → BinaryOp(col >= min) AND BinaryOp(col <= max)`, `All → no-op`
- [ ] 4.6 Add unit tests for ScanExec with mocked collector: timeout fires correctly; Domain merged into filter list; same-fragment InList still works (regression guard)
- [ ] 4.7 Quality gate green; distributed bench 22q unchanged (flag OFF)
- [ ] 4.8 With flag ON manually (debug build): SF1 trino-diff 17/17 ✓ at 1e-9 (correctness should be unaffected — scan-side awaits but no producer publishes yet, so it just times out and proceeds)

## Phase A1.5 — Build-side emit (~100 LOC, 0.2 session)

Goal: HJ/SemiJoin actually publish their Domains to coord. With flag ON, end-to-end DF flow is alive.

- [ ] 5.1 Add `crates/execution/src/dynamic_filter_builder.rs` with `JoinDomainBuilder` (per-channel) — accumulate distinct values up to cap; on overflow track min/max; on second overflow downgrade to `Domain::All`
- [ ] 5.2 Modify `HashJoinExec::build_state_inner` (hash_join.rs:2281+) to, when the join has `dynamic_filter_ids` AND `enable_dynamic_filtering=true`: build a `JoinDomainBuilder` per DF id during build phase; on completion, serialize Domain and call the task's `dynamic_filter_publisher` (a sink the RPC layer drains)
- [ ] 5.3 Same change in `SemiJoinExec::execute_inner` (semi_join.rs:382 — existing inject site)
- [ ] 5.4 Wire RPC layer to drain `dynamic_filter_publisher` and ship via task-completion's `produced_dynamic_filters`
- [ ] 5.5 With flag ON: SF1 trino-diff 17/17 ✓ at 1e-9; distributed bench 22q unchanged within ±5% (no Q09 regression; some Q09 may improve modestly even at SF1)
- [ ] 5.6 With flag OFF: distributed bench 22q identical to pre-change baseline (confirms flag actually isolates the feature)
- [ ] 5.7 Quality gate green

## Phase A1.6 — Default ON + SF10 Q09 measurement (~50 LOC, 0.2 session)

Goal: flip default; measure win on SF10 Q09; gate prevents shipping if any other query regresses.

- [ ] 6.1 Flip `enable_dynamic_filtering` default from `false` to `true` in session params + config
- [ ] 6.2 Add `EXPLAIN ANALYZE` per-DF metrics: rows produced, rows discarded by DF (worker side), wait time (scan side). Emit via `arneb::profile` target.
- [ ] 6.3 Run SF10 22-query distributed bench (1 coord + 2 workers, OrbStack 16 GB, total cluster peak metric). Required gate: Q09 lat < 12 s AND Q09 mem < 6 GB AND no other query regresses lat > 10% or mem > 10% from the pre-A1 baseline.
- [ ] 6.4 Run SF10 17q cell-by-cell trino-diff (q01-q16, q19) at 1e-9 tolerance. Gate: 17/17 ✓. (For Q09 cell diff, fall back to isolated mode if Trino+arneb together still exceed 16 GB cgroup.)
- [ ] 6.5 If 6.3 OR 6.4 fails: do NOT commit A1.6. Diagnose, return to a prior phase, re-roll.
- [ ] 6.6 Once gated: update CLAUDE.md to mention `dynamic_filtering_wait_timeout` session param; update memory with new SF10 Q09 numbers
- [ ] 6.7 Quality gate green; commit + open follow-up note for [[A2 broadcast joins]] if motivated by Q09 numbers

## Out of scope / follow-ups

- LEFT/RIGHT/FULL OUTER joins (semantically incompatible with build-side DF)
- AntiJoin / NOT IN (opposite semantics)
- Bloom filter fallback (potential follow-up; min/max range covers the common case)
- DF persistence across queries (no use case)
- DF coalescing across coord-side queries (same)
- BROADCAST join strategy (separate A2 change; orthogonal — DF helps regardless)
