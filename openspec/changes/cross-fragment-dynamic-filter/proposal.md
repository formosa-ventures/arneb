## Why

arneb's in-worker (same-fragment) dynamic filter already works: `HashJoinExec::inject_inlist_dynamic_filters` (crates/execution/src/hash_join.rs:282) extracts distinct values from the build side after the build phase finishes and emits a `PlanExpr::InList` that propagates down through `FilterExec::inject_dynamic_filter` to `ScanExec::dynamic_filters` (crates/execution/src/operator.rs:298), and the Parquet connector consumes it for row-group pruning (crates/connectors/src/parquet_pushdown.rs:25).

What does NOT work today: when the join build side runs on stage N and the probe scan runs on stage N-1 on different workers (the normal distributed pattern for any non-broadcast equi-join), the same-fragment injection mechanism cannot reach across the fragment boundary. The probe-side scan therefore reads ALL rows that pass static filters, even when only ~25 of 60 million lineitem rows match.

This is the root cause of SF10 Q09 being the sole remaining query arneb loses to Trino: 2.0× slower latency (23 s vs 11.5 s) and 0.66× memory (8.1 GB vs 12.4 GB peak). At SF10 Q09's six-way join the lineitem probe is the dominant cost, and without a cross-fragment DF arneb scans the entire 60 M-row table.

Trino's `DynamicFilterService` (core/trino-main/src/main/java/io/trino/server/DynamicFilterService.java) solves this. Port the cross-fragment pieces only — arneb already has the local pieces.

## What Changes

- **NEW**: `DynamicFilterId(u32)` newtype in `crates/common/`, attached to logical-plan join nodes (build-side producer) and scan nodes (probe-side consumer) at plan time. Survives fragmentation.
- **NEW**: `crates/scheduler/src/dynamic_filter.rs` — coordinator-side `DynamicFilterService` that accumulates per-task partial DFs and resolves a per-query future when all partitions of a stage have reported.
- **NEW**: `Domain` value type in `crates/common/` representing a per-column DF payload — distinct values (under cap) OR min/max range OR `Domain::all` (no-op).
- **MODIFIED**: Flight RPC piggybacks DFs on existing methods: worker → coord on the existing `register_buffer` / task-completion path; coord → worker on the existing `submit_task` path. NO new RPC endpoints.
- **MODIFIED**: `ScanExec::dynamic_filters` changes from push-only `Mutex<Vec<PlanExpr>>` to a hybrid: same-fragment push (existing behavior) AND a per-task `DynamicFilterCollector` that holds `Map<DfId, oneshot::Receiver<Domain>>` for cross-fragment DFs. `ScanExec::execute` awaits any pending DF with a per-query timeout (default 10 s) before opening the underlying scan.
- **MODIFIED**: `HashJoinExec::build_state_inner` (the build-completion hook at hash_join.rs:2281) ALSO publishes the merged Domain to the coord-side service when the join carries a cross-fragment DF id.
- **MODIFIED**: `PlanFragmenter` annotates each fragment's plan with the DF ids it produces and consumes, so workers know what to register on the local collector and what to publish to coord.

## Capabilities

### New Capabilities

- `dynamic-filter`: identifier allocation at plan time; cap-aware `JoinDomainBuilder` (distinct values → min/max range → `Domain::all`); coordinator-side service that merges per-partition partials and fires per-DF futures; per-task collector that bridges async waits on scan side; session params `enable_dynamic_filtering` (default true) and `dynamic_filtering_wait_timeout` (default 10 s).

### Modified Capabilities

- `execution-operators`: `ScanExec` adds async-wait path for cross-fragment DFs; `HashJoinExec`/`SemiJoinExec` add publish-to-coord hook after build phase.
- `physical-planner`: `PlanFragmenter` annotates each fragment with DF id sets.
- `flight-rpc-layer`: existing `submit_task` payload carries `initial_dynamic_filters: Vec<(DfId, Domain)>`; existing task-completion/heartbeat path carries `produced_dynamic_filters: Vec<(DfId, PartialDomain)>`. Wire format extension, no new endpoints.

## Impact

- **Affected crates**: `crates/common` (new `DynamicFilterId`, `Domain`), `crates/planner` (annotation), `crates/execution` (hooks in HJ/SemiJoin/ScanExec), `crates/scheduler` (new `DynamicFilterService` + integration in `coordinator.rs`), `crates/rpc` (Flight method payload extension).
- **Downstream**: Parquet connector pushdown unchanged — it already accepts `PlanExpr::InList` from ScanExec.
- **Performance target**: SF10 Q09 latency 23 s → ≤ 12 s (≥ 0.95× Trino baseline); SF10 Q09 peak memory ≤ 6 GB (currently 8.1 GB); no regression on the 21 already-winning SF10 queries.
- **Size estimate**: ~900 LOC across 6 phases (revised down from the 1500 LOC v10 estimate because arneb's in-worker DF infrastructure already exists).
- **Risk to current 22/22 SF1 + 21/22 SF10 wins**: phases land behind an `enable_dynamic_filtering` session param defaulting OFF until SF1 + SF10 22-query trino-diff passes; flip default to ON only when the safety net holds.
