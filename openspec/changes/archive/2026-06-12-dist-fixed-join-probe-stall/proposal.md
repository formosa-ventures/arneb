## Why

At SF30, q21 and q02 are silently/non-deterministically WRONG (q21 returned ~62/100 wrong suppliers). The Layer-1 safety net (committed `1704661`) converts that silent corruption into a clean ERROR, but the queries still cannot produce a result. The root cause is a distributed-exchange stall: a FIXED single-task semi/anti-join builds its huge right side (q21 stage 8 = EXISTS semi over lineitem `l2` = 179,998,372 rows; stage 10 = NOT-EXISTS anti over `l3` = 113,797,647) for minutes BEFORE pulling its probe, while the probe-side partitioned hash-join chain (stage 6 ← 4 ← 2 = `supplier⋈l1⋈orders⋈nation`) runs eagerly, fills its bounded `OutputBuffer`s (cap 64), spills the overflow, then blocks — leaving the Flight connection IDLE for minutes. Under SF30 memory pressure the idle connection's receiver is dropped, truncating the probe non-deterministically (measured: stage2.task0 op-output 49.58M vs 50.05M run-to-run). This is the same mechanism the memory pinned for q18 (q18 N>2 → loud BrokenPipe; q21/q02 N=2 → silent). Making these queries CORRECT requires removing the stall.

## What Changes

- **MEASURE-FIRST (gating task):** instrument the exact idle-connection-drop trigger (h2 idle timeout vs flow-control vs resource/cancellation) using the existing `[EXCHTRACE]` probe plus connection-lifecycle tracing, BEFORE choosing a lever. This area is revert-prone precisely because past changes guessed the lever; pure h2 keepalive was already tried and reverted as ineffective.
- **Eliminate the probe-side stall** so a FIXED consumer's long build no longer starves/idles its probe producers. The exact lever is chosen from the measurement; candidates: (1) drain the probe into a local spill DURING the build so connections stay active and producers unblock; (2) a bounded+spill exchange that never leaves a connection idle; (3) build the smaller side / avoid materialize-then-stall; (4) cross-stage pipelining.
- **Keep memory bounded:** q02 also hits a separate SF30 OOM (`HashAggregateExec` pool exhaustion) that co-occurs with the stall; the chosen lever must not trade the stall for an OOM.
- **Oracle-gated:** every step is validated by `blast_radius_oracle.py` (determinism + cell-diff vs Trino, all 22; committed `d78214b`). Success = q21 AND q02 go ERROR → cell-identical to Trino at SF30 N=2, with the other 19 staying clean and no new errors.
- Non-goal: this change does NOT flip `DEFAULT_MAX_HASH_PARTITIONS` (N stays 2) or re-open adaptive partitioning; it makes the existing N=2 FIXED-join-over-partitioned topology correct under load.

## Capabilities

### New Capabilities
- `dist-exchange-stall-resilience`: a downstream FIXED consumer that defers reading its probe (because it is building a large side first) must not cause the probe-side producers' connections to idle-reset and silently truncate; the exchange/join keeps probe connections live (active drain or bounded+spill) so the query completes with complete, correct data under SF30 memory pressure.

### Modified Capabilities
<!-- None: Layer-1's must_drain safety net already shipped (1704661); this change makes the stall not happen, so the safety net stops firing for q21/q02. No spec-level requirement of an existing capability changes. -->

## Impact

- **Code:** `crates/execution/src/hash_join.rs` (`execute_grace_shared` lazy probe drain, line ~2008) and/or `crates/execution/src/semi_join.rs` (build-then-probe ordering); `crates/server/src/task_manager.rs` (D2 spillable pump `drain_spill_to_sender` blocking behavior, lines 604-698); possibly `crates/rpc/src/exchange_client.rs` / `flight_service.rs` / `output_buffer.rs` (connection keepalive / bounded+spill) — exact set determined by the measure-first task.
- **Diagnostics:** the uncommitted `[EXCHTRACE]` per-ticket SERVE/RECV probe (`exchange_client.rs`/`flight_service.rs`) is reused for validation; commit-or-revert decided at the end.
- **Validation/gate:** `benchmarks/tpch/scripts/blast_radius_oracle.py` (existing); the remote SF30 bench host.
- **Risk:** distributed-exchange changes are historically revert-prone (A.4, broadcast v1, streaming ×2, Z.4); the design encodes invariants + the oracle gate to avoid a silent regression. No API/protocol break; no change to single-node or non-FIXED-join paths.
