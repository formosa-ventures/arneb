## Context

arneb runs distributed queries as a coordinator + N workers connected by an Arrow Flight exchange. A query is cut into stages at `ExchangeNode` boundaries; an upstream stage's worker tasks write partitioned output into a per-task **OutputBuffer** (`crates/rpc`), and the downstream stage pulls it via Flight `do_get` + `RepartitionExec` (`crates/execution/src/repartition.rs`, an `MpscStream` over a bounded tokio mpsc channel).

Two properties of the current implementation combine into the failure:

1. **A hard wall-clock deadline.** The coordinator's Flight `do_get` waits up to **5 minutes** for a worker to register its OutputBuffer for a task; if the worker's task hasn't reached the point of registering/producing within 5 min, `do_get` fails the whole query (`"worker never registered OutputBuffer within 5 min"`). This is a fixed deadline independent of scale or host.
2. **Back-pressure that can stall rather than smoothly flow-control.** When a downstream consumer is slow, the upstream producer blocks on the bounded channel / output buffer. Under fewer cores the producer/consumer interleave poorly: measured `RepartitionExec first_batch_ms ≈ 40 s` with **CPU idle ~1.5 %** (the cluster is *waiting*, not computing). These per-stage waits accumulate across q09's many stages until some stage trips the 5-min deadline.

Measured this session on an 8-core / 31 GB host, seeded SF10: arneb q09 completes in ~28.6 s when it works but intermittently stalls→fails; Trino completes 3/3 (60/34/26 s). Ruled out cgroup cap (stalls at cpus=2 and cpus=5), over-subscription (idle CPU), OOM (~300 MB RSS), and host capacity (20/22 SF10 queries pass). The defect is arneb's exchange.

**Prior attempt (constraint):** the 2026-05-23 per-batch streaming-output refactor (`async_stream::try_stream!`) passed 23 unit tests but was **reverted** because it deadlocked: the `task_manager` admission semaphore held a permit across the entire task body, so a producer that blocked on a full OutputBuffer never released its permit → its downstream consumer was never admitted → permanent stall. Phase A (2026-05-23) later **deleted that semaphore**, removing the structural cause — but the streaming refactor was never re-landed on top of that fix.

## Goals / Non-Goals

**Goals:**
- An 8-core host reliably completes **all 22 TPC-H queries at SF10** (no `OutputBuffer` hard-fail).
- The exchange scales to **SF100**: no fixed wall-clock deadline that a heavier/slower stage trips. Slow degrades to *slow* (or spill), never to hard *failure*.
- A slow or stalled consumer never deadlocks its producer; back-pressure is bounded and flows.
- Distributed cell-correctness (trino-diff parity at 1e-9) preserved.

**Non-Goals:**
- Making q09 *faster* (it is already ~competitive when it completes) — this is a reliability change, not a speed optimization.
- cgroup/CPU-cap auto-detection (already landed this session) and per-query algorithmic wins (cache-fit / parallel-probe — orthogonal, flag-gated).
- Changing the query plan / fragmentation shape.

## Decisions

**D1 — Remove the hard 5-minute OutputBuffer-registration deadline; replace with liveness, not a wall-clock cap.**
A fixed 5-min deadline cannot scale (SF100 stages legitimately exceed it) and converts "slow" into "failure". Replace it with a *progress/liveness* signal: the `do_get` consumer fails only if the producer task has **died or made no progress for a heartbeat interval**, not if it merely takes a long time. Alternative considered: just raise the constant (e.g. 30 min) — rejected, it only moves the cliff and still fails at larger scale / slower hosts. Alternative: per-query configurable timeout — kept as an optional safety ceiling (default unbounded/large), but liveness is the primary mechanism.

**D2 — Bounded exchange buffer (back-pressure). [spill-to-disk descoped → memory-accounting change]**
Give the exchange a bounded in-memory buffer (a per-partition `mpsc::channel` with configurable capacity, default 4; OutputBuffer cap=64): a producer that outruns its consumer parks on the full channel instead of accumulating without limit. This is the back-pressure mechanism.

**Resolved empirically (the Open Question below):** D1 (liveness) + this bounded buffer removed the SF10 q09/q21 hard-fail **without disk spill** — the failure was the wall-clock deadline, not unbounded exchange memory (worker RSS during the stall was ~300 MB, not OOM). So the original D2 plan to *spill the overflow to disk* is **descoped from this change**. It remains worthwhile for SF30/SF100-scale intermediates, but disk-spilling the exchange must coordinate with the global `MemoryPool` (spill under pool pressure, not at a fixed per-channel cap) — implementing it here in isolation would be retrofitted when allocation-level accounting lands. It is therefore owned by the upcoming memory-accounting change ("Killer 3"), which tracks the exchange channel in the global pool **and** spills it. Alternative considered: ship the isolated spill here anyway — rejected (guaranteed retrofit; overlaps the memory change's core).

**D3 — Re-land deadlock-free per-batch streaming output on top of the Phase-A semaphore removal.**
The 2026-05-23 streaming refactor was correct in approach but deadlocked via the admission semaphore, which Phase A has since removed. Re-land per-batch streaming so a worker produces and registers its OutputBuffer incrementally (first batch ASAP), reducing first-batch latency, **and verify no remaining resource (lock, permit, pool reservation) is held across a back-pressure park**. This is the explicit constraint: a producer parked on a full buffer must hold nothing that gates its consumer's admission or execution.

**D4 — Decouple admission/scheduling from stream back-pressure (verification, not new mechanism).**
Phase A removed the count-based admission semaphore; the RSS-based admission gate is a polling wait that holds no permit. Confirm (with a deadlock-regression test: producer blocked on a full buffer while a dependent consumer awaits admission) that no admission path can be starved by a parked producer.

## Risks / Trade-offs

- **Re-introducing the 2026-05-23 deadlock** → Mitigation: D4 deadlock-regression test as a gate; audit every `.await` on the producer path for held resources (locks, MemoryPool reservations, admission state) across a back-pressure park.
- **Spill changes peak disk / adds I/O latency** → Mitigation: bounded, configurable spill threshold; spill only engages when the in-memory credit window is exhausted (i.e. only on genuine consumer-slowness), so the fast path is unchanged.
- **Removing the deadline could let a genuinely hung query run forever** → Mitigation: D1 liveness/heartbeat detection (fail on producer death / no-progress), plus an optional large configurable ceiling — distinguishes "slow" (allowed) from "hung" (failed).
- **Distributed correctness regressions** → Mitigation: trino-diff cell parity (1e-9) on the full suite + the existing distributed-mode validation as a ship gate.
- **Credit-based flow control is non-trivial** → Mitigation: start with the minimal viable form (bounded buffer + spill overflow + liveness) before full credit windows; measure whether spill-overflow alone fixes q09/q21 SF10 before adding credit complexity.

## Migration Plan

Land incrementally behind verification, not a feature flag (this is a correctness fix, not an opt-in):
1. D1 (liveness replaces hard deadline) — smallest, highest-leverage; re-test q09/q21 SF10 (expect: completes slow instead of failing).
2. D2 (bounded buffer + spill overflow) — re-test q09/q21 SF10 reliability + SF100 smoke.
3. D3 (re-land streaming output) — re-test no-deadlock regression + first-batch latency.
4. D4 verification test throughout.
Rollback: each step is independently revertable; D1 alone is expected to remove the hard-fail.

## Open Questions

- ~~Is D1 (liveness) alone enough to make q09/q21 SF10 reliable, or is D2 (spill) also required?~~ **RESOLVED: D1 + the bounded buffer were enough at SF10 — no disk spill needed.** The stall was the wall-clock deadline, not unbounded memory (RSS ~300 MB during the stall). Exchange disk-spill is consequently descoped to the memory-accounting change (see D2).
- Exact liveness signal: does the worker emit a periodic progress heartbeat on the Flight stream today, or must one be added? (Affects D1 scope.)
- Does SF100 fit on the 8-core / 31 GB host at all (disk-spill volume), or is SF100 a separate seeding/capacity exercise once SF10 is reliable?
