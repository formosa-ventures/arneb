## Context

q21/q02 are silently wrong at SF30 because of a distributed-exchange stall. The data flow (q21, measured via `[EXCHTRACE]` + `[FRAGTRACE]` this session):

```
stage 2  supplier ⋈ l1        (hash-partitioned, N=2)   ─┐
stage 4  ⋈ orders             (hash-partitioned, N=2)    ├─ probe chain (lazy)
stage 6  ⋈ nation             (hash-partitioned → N,1)  ─┘
stage 8  EXISTS semi  ← gathers stage 6, BUILDS from scan 7 (l2 = 180M, spills 5 chunks, minutes)   [FIXED, 1 task]
stage 10 NOT-EXISTS anti ← gathers stage 8, BUILDS from scan 9 (l3 = 114M, spills 3 chunks)         [FIXED, 1 task]
stage 11 PartialAggregate → coord FinalAggregate → Sort → Limit 100
```

The FIXED semi/anti-join (`crates/execution/src/semi_join.rs`) reads its RIGHT (build) side to completion first; the probe-side hash joins use `execute_grace_shared`'s `async_stream::try_stream!` (`crates/execution/src/hash_join.rs:2008`) which drains the probe LAZILY by downstream demand. But the coordinator dispatches all tasks eagerly, so the probe producers (stage 2/4/6 worker tasks) run immediately, fill their bounded `OutputBuffer`s (cap 64), spill overflow via the D2 spillable pump (`crates/server/src/task_manager.rs:604-698`), then BLOCK on `drain_spill_to_sender` waiting for the not-yet-draining consumer. The Flight connection sits IDLE for minutes during the build; under SF30 memory pressure the receiver is dropped → the producer's `consumer_gone` path fires.

**Measured truncation** (operator op-output, `task completed rows=`): stage2.task0 = 49,577,920 (run1) vs 50,051,486 (run2) — non-deterministic offset; stage2 total 106.6M/107.1M but l1 = 113,797,647 → ~7M rows truncated. Propagates through stages 4/6/8/10/11 → top-100 churns ~13 suppliers run-to-run.

**Disproven this session** (do not re-investigate): NOT a Flight transport drop (SERVE==RECV per ticket); NOT a semi/anti spill-multipass loss (op-output truncation is upstream at stage 2); NOT a partitioning race (totals vary). **Layer-1 (`1704661`)** already converts the silent truncation into a clean error via the `must_drain` flag; this change makes the truncation NOT HAPPEN.

**Hard constraints from history** ([[project_2026-06-05_nway_nested_join_bug_and_distributed_brain]]): the distributed-exchange area is revert-prone — A.4, broadcast v1, two streaming refactors, and Z.4 all reverted, because changes guessed the lever without a fast oracle or encoded invariants. Pure h2 keepalive was tried and reverted as ineffective (it shifted BrokenPipe→ConnectionReset without fixing the drop). The oracle now exists (`blast_radius_oracle.py`, `d78214b`) and is the non-negotiable gate.

## Goals / Non-Goals

**Goals:**
- q21 AND q02 go from ERROR → cell-identical to Trino at SF30 N=2 (determinism + cell-diff via `blast_radius_oracle.py`), deterministic across ≥3 runs.
- The other 19 queries stay clean (no new errors, no cell-diffs); no regression of the Layer-1 safety net (it should simply stop firing for q21/q02).
- Memory stays bounded: q02's co-occurring `HashAggregateExec` OOM at SF30 is resolved (the fix must not trade the stall for an OOM, and must keep the per-node cap honoured).
- The fix is grounded in a MEASURED drop-trigger, with the chosen lever justified against alternatives.

**Non-Goals:**
- Flipping `DEFAULT_MAX_HASH_PARTITIONS` off 2 / re-opening `dist-adaptive-partition` (N stays 2; this is correctness of the existing topology under load).
- A general cross-stage-pipelining rewrite of the engine (architectural, multi-month) unless the measurement proves nothing smaller works.
- Latency optimization (q21/q02 are deep-join latency losers; that is a separate, already-decided-parked concern).

## Decisions

### D1 — MEASURE the exact idle-connection-drop trigger BEFORE choosing a lever (gating)
The single most important decision is to NOT guess. Instrument, on the SF30 stack, what exactly drops the probe producer's receiver during the build stall:
- Connection-lifecycle trace on both ends: producer-side (when `drain_spill_to_sender` blocks; when the mpsc receiver drops; the `Closed` cause) and consumer-side (`exchange_client.rs` do_get / FlightPassthroughStream — does the inner Flight stream yield Err (reset) or just stop being polled (lazy-drop)?).
- Distinguish the candidates: (a) h2/tonic IDLE timeout on the do_get stream; (b) tonic/h2 flow-control or connection-window exhaustion; (c) a resource/admission cancellation dropping the consumer task; (d) the lazy `try_stream!` being dropped because a higher consumer stopped.
- Rationale: the lever is entirely determined by which of (a)-(d) it is. Reuse the uncommitted `[EXCHTRACE]` probe. **Alternative rejected:** jump straight to a lever (the documented cause of every prior revert).

### D2 — Keep the probe connection ACTIVE during the build (primary candidate, pending D1)
If D1 confirms an idle-driven drop, the most targeted fix is to never leave the probe connection idle while a FIXED consumer builds: drain the probe into a local spill DURING the build (the join eagerly pulls + spills its probe to disk while building its right side), so data keeps flowing and producers unblock; process the spilled probe after the build. Operator-local to `semi_join.rs`/`hash_join.rs`; reuses existing spill infra.
- **Alternative A (bounded+spill exchange):** make the producer-side `OutputBuffer`/pump never block-then-idle — once it would block on `drain_spill_to_sender`, keep spilling and only hand off when the consumer pulls. Pushes the fix into the exchange layer (`task_manager.rs`/`output_buffer.rs`), broader blast radius.
- **Alternative B (build the smaller side):** if the FIXED join can build the (smaller) gathered side instead of the 180M scan, the build is short and never stalls the probe. Depends on cardinality knowledge; ties into the parked build-side-selection work, higher planner risk.
- **Alternative C (keepalive only):** rejected upfront — already tried and reverted.

### D3 — Oracle-gated, incremental, reversible
Land behind the existing oracle with a feature gate where feasible. Validate after EVERY step: SF30 `blast_radius_oracle.py` all-22 ×≥2 (q21+q02 must flip to PASS, 19 stay clean), plus the SF30 memory bench (no OOM, per-node cap honoured). Keep each step a clean revert.
- **Alternative rejected:** a big-bang exchange rewrite — the historical revert pattern.

### D4 — Resolve q02's co-occurring OOM as part of correctness
q02 hit `HashAggregateExec: pool exhausted` at SF30 independently of the stall. The fix is incomplete if q02 swaps a stall-error for an OOM-error. Treat the OOM as in-scope: ensure the aggregate spills/stays under the per-node cap (link to `exec-memory-accounting`).

## Risks / Trade-offs

- **[Revert-prone area]** → oracle-first (D1, D3); every step gated by `blast_radius_oracle.py` + the SF30 memory bench; no lever without a measured trigger.
- **[Probe-spill-during-build adds I/O / latency]** → q21/q02 are already deep-join latency losers (parked); correctness >> latency here. Bound the extra spill by the existing chunk-cap.
- **[Fix the stall but reintroduce an OOM]** → D4 keeps memory in scope; validate with the memory bench, not just the correctness oracle.
- **[Blast radius wider than q21/q02]** → the bug is load-dependent; run the oracle under sustained load (worst case) and ≥3 determinism runs; treat any other query flipping to ERROR as in-scope evidence, not collateral.
- **[Thermal/host noise on the bench]** → use determinism (run-to-run) as the primary signal (host-independent), not absolute latency ([[slows-each-run-is-thermal-throttling]]).

## Migration Plan

No data migration; runtime behavior only. Land incrementally on `feat/perf-column-pruning-3x`. Each step is a standalone commit; rollback = revert the commit (the Layer-1 safety net `1704661` remains as the backstop, so a rollback degrades to "honest error", never to "silent wrong"). Rebuild the SF30 image on the remote bench host between steps.

## §1 MEASURED RESULT (2026-06-12) — hypothesis OVERTURNED

D1 is done. q21 with `ARNEB_MUST_DRAIN=0` + connection-lifecycle tracing (`RECV_ERR` vs `RECV_DROP_MIDSTREAM` on `FlightPassthroughStream`):

- **ZERO `RECV_ERR`** — it is NOT a connection reset / h2 idle-timeout / flow-control (candidates a/b REJECTED). The "idle-during-build → reset" framing in Context above is WRONG.
- The drop is **`RECV_DROP_MIDSTREAM`** = a consumer **abandons its probe mid-stream** (candidate c/d), firing **3.7s AFTER the FIXED semi build completes** (i.e. when probing starts, NOT during the build idle), cascading TOP-DOWN: stage6 abandons stage4 (rows read=4,815,162 then stop) → stage4 abandons stage2 → stage2 abandons stage0, all on partition 0, ~3s apart.
- **Therefore the trigger is OPERATOR-LEVEL, not exchange-level.** A hash join (the chain root being the FIXED semi/anti pulling the partitioned `⋈nation` etc.) short-circuits and stops draining its probe. Prime mechanism: an **empty build partition** (a tiny build side — `nation` = 1 SAUDI row — hashes to only 1 of N partitions, leaving the other partition's build empty) makes the join return empty for that partition WITHOUT draining its probe (`handle_empty_right_partition` at `hash_join.rs:3003` literally `return Ok(empty)` for non-LEFT joins before touching `left`; the per-partition path has the analogous short-circuit). The abandoned probe → upstream producer `consumer_gone` → silent truncation → wrong top-100.

**REVISED DECISION (supersedes D2/Alternatives):** the fix is a TARGETED OPERATOR change, NOT the multi-week exchange-reliability work — when a join short-circuits on an empty/absent build partition, it MUST drain (consume-and-discard) its probe input for that partition before returning, so the shared upstream producer is fully consumed and not truncated. Small, local to `hash_join.rs`. Still oracle-gated (D3) + memory-bounded (D4).

## §3 FIX LANDED + VALIDATED (2026-06-12)

Exact site PINNED: `hash_join.rs` `execute_grace_single` → `BuildChunksResult::Single` arm → `ProbeCollect::Overflow { prefix, rest } => match right_combined { None => ... }` (~line 3571). For an EMPTY build partition with a probe that overflowed the collect budget, the arm returned an empty output stream and **dropped `rest` (the un-drained probe remainder, a remote `ExchangeExec` stream) without reading it** — a single-node optimization ("empty build → INNER yields nothing → don't materialise the probe") that is WRONG distributed: dropping `rest` closes the consumer mid-stream → upstream `consumer_gone` → truncation (the `prefix` already pulled = the traced `rows=4,815,162`) → cascade down the shared partitioned chain → wrong rows. q21's `nation`=1-SAUDI-row build hashes to 1 of N partitions, so the other partition's build is empty → hits this arm.

FIX: drain `rest` to EOF (discard — empty build yields no output) before returning empty, so the upstream producer completes. ~10 lines, local, `cargo check` clean.

**VALIDATED (SF30 N=2, ×2, `must_drain` ON):** q21 AND q02 both flip **ERROR → 🟢 PASS (deterministic + cell-identical to Trino, 0 diff both runs)**. The same arm covered BOTH affected queries. Full-22 regression in progress.

The other §1 candidate levers (D2 probe-spill-during-build, bounded+spill exchange, build-smaller-side, cross-stage pipelining) and the q18-style "idle-connection-reset" framing are MOOT — the trigger was a local-optimization probe-drop, not an exchange-reliability problem.

## Open Questions (resolved)

- ~~Pin the exact short-circuit site~~ — DONE: `hash_join.rs:~3571` grace_single empty-build + Overflow arm.
- ~~Does draining-the-probe-on-empty-build alone make q21+q02 correct?~~ — YES, both PASS.
- ~~D1: which of (a)-(d) drops the connection?~~ — none; it's case (c/d) operator probe-abandon, not a connection drop.
- REMAINING: does q02's separate SF30 OOM (`HashAggregateExec`) recur under sustained load? (The full-22 regression under load answers; q02 passed this 2-query run without OOM.)
- Is the connection drop the ONLY truncation path, or does a second mechanism co-exist under heavier load? (The oracle under sustained load answers this.)
- Does the EXCHTRACE probe get committed as permanent diagnostic infra (like `ARNEB_TRACE_FRAGMENTS`) or reverted at the end?
- Is q02's OOM purely aggregate-side, or does the stall-fix change the memory profile enough to resolve it incidentally?
