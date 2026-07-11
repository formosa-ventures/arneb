## Context

arneb workers run plan fragments under a cgroup memory limit. The only memory the engine currently *accounts* is what operators reserve via `MemoryReservation::try_grow` against a single process-global `GreedyMemoryPool` (`crates/execution/src/memory_pool.rs`) — in practice the HashJoin/SemiJoin build hash tables and the aggregate group state. Everything else allocates Arrow buffers directly and is invisible to the pool:

- `RepartitionExec` bounded `mpsc::channel` buffers (`crates/execution/src/repartition.rs`) — cap=4 batches/channel × N partitions.
- The rpc `OutputBuffer` (`crates/rpc/src/output_buffer.rs`) — cap=64 batches/partition, the worker-to-worker exchange staging.
- Parquet scan decode buffers (`crates/connectors`) — decoded column chunks before they become a `RecordBatch`.
- Intermediate `FilterExec`/`ProjectionExec`/`RepartitionExec` output batches — the wide `lineitem⋈orders` intermediate at SF30 is ~4.3 GB and lives in these.

Measured at SF30 q09: HJ build ~165 MB, worker peak ~9.5 GB → ~97 % of the peak is untracked. q21 OOMs; a clean run thrashed the host to ~23 GB. `partition_count`↑ did not cut the peak because the untracked total is per-node, not per-partition.

The just-archived `exec-exchange-backpressure` change shipped the **bounded** exchange (back-pressure) but **descoped disk-spill** to here, on the explicit ground that exchange spill must trigger on **global pool pressure**, not a fixed per-channel cap — which requires the pool to first *see* the exchange bytes. So Phase 1 (track) is the prerequisite for Phase 2 (spill).

## Goals / Non-Goals

**Goals:**
- The worker's tracked memory reflects its true peak: exchange channels, OutputBuffer, scan decode buffers, and intermediate operator batches are all registered against the global pool.
- Under pool pressure, spillable consumers (exchange overflow, join/aggregate build) degrade to disk; the process does not OOM. **q21 SF30 completes** on the constrained host.
- SF30 q09/q21 runs become **stable** (not bistable) — a precondition for honestly measuring the later DF / distribution changes.
- Distributed cell-correctness (trino-diff 1e-9) preserved; the fast path (pool not pressured) adds no measurable overhead.

**Non-Goals:**
- A custom `#[global_allocator]` that intercepts every `alloc` (ClickHouse-style). Rejected: not Rust-idiomatic with Arrow's `Arc<Buffer>` sharing, hard to attribute, and overkill — DataFusion-style explicit reservation at the few large allocation sites is enough and is what the alloc audit concluded.
- Killer 1 (distribution-property / broadcast), cross-fragment dynamic filter, per-query algorithmic speed wins.
- Reducing the *size* of the intermediate (that's late-materialization / DF / distribution work) — this change makes the existing size **survivable and bounded**, not smaller.

## Decisions

**D1 — Track exchange buffers via `MemoryReservation`, not a new mechanism.**
Register the `RepartitionExec` channel and rpc `OutputBuffer` as reservations against the existing global `MemoryPool`: `try_grow` on enqueue, `shrink`/`free` on dequeue/drop (RAII via the reservation's `Drop`). The bound (cap=4 / cap=64) already provides back-pressure; this makes those bounded bytes *visible* to the pool so the pool-pressure spill (D2) can fire. Alternative: a separate exchange-only budget — rejected (two budgets can't co-decide who spills; the point is one global view).

**D2 — Spill the exchange overflow on global-pool pressure, reusing Grace IPC spill.**
When `try_grow` for an incoming exchange batch would exceed the pool limit and the consumer is behind, spill that batch to disk (Arrow IPC, the SemiJoin/HashJoin Grace machinery in `spill.rs`) and drain in FIFO order; release the reservation on spill, re-reserve on read-back. This is the descoped `exec-exchange-backpressure` D2, now correct because the trigger is *pool pressure* (shared with join/aggregate spill) rather than a fixed per-channel cap. Alternative: fixed per-channel spill threshold — rejected (the original descope reason: it can't balance exchange vs build memory).

**D3 — Sweep the diffuse allocations behind the same reservation API.**
Bring Parquet scan decode buffers and intermediate `FilterExec`/`ProjectionExec`/`RepartitionExec` batches under `MemoryReservation`. These are many small sites (~160 in the alloc audit); land them in audited batches, each gated by re-measuring the SF30 worker peak so we see the tracked fraction climb toward the real RSS. Order by leverage (biggest untracked contributors first, per the profile).

**D4 — Optional `TrackConsumersPool` wrapper for diagnostics.**
Wrap the `GreedyMemoryPool` to record per-consumer reserved bytes and, on `ResourceExhausted`, report the top consumers. This makes the diffuse D3 work debuggable (which site is the real hog) and mirrors DataFusion's `TrackConsumersPool`. Cheap, additive, no behaviour change on the success path.

## Risks / Trade-offs

- **Mis-accounting → spill too eagerly (slow) or too late (OOM).** Mitigation: D4 consumer reporting to verify tracked ≈ RSS at SF30; tune the spill trigger against measured peak; keep the fast path (no pressure) reservation-only (no spill).
- **Reservation overhead on the hot path.** Mitigation: reserve per *batch* (coarse), not per row/buffer; the fast path is one atomic add/sub per batch — negligible vs batch construction.
- **Spill I/O latency on the exchange path.** Mitigation: spill only engages under genuine pool pressure (slow consumer + tight memory); the common case never spills. Bounded, configurable threshold.
- **Re-introducing a back-pressure deadlock.** Mitigation: reuse the `exec-exchange-backpressure` D4 invariant + its regression test — a producer parked or spilling holds no admission permit / lock / pool reservation across the park.
- **Distributed correctness regression.** Mitigation: trino-diff cell parity (1e-9) on the full suite as a ship gate after each phase.

## Migration Plan

Incremental, each phase independently revertable and measurement-gated:
1. **D1** (track exchange channel + OutputBuffer) — re-measure SF30 worker peak: tracked fraction jumps from ~3 % toward the exchange's real share. No behaviour change yet (still bounded, not spilling).
2. **D2** (pool-pressure exchange spill) — re-run q21 SF30: expect it to **complete** (spill instead of OOM). q09 SF30 stable (not bistable).
3. **D3** (diffuse alloc sweep, in audited batches) — re-measure until tracked ≈ RSS; q21/q09 peak bounded under the cgroup.
4. **D4** verification/diagnostics throughout.
Rollback: each phase is a separable set of reservation call-sites; D2 is the only behaviour change and is independently revertable.

## Open Questions

- What pool-pressure threshold triggers exchange spill without hurting the fast path — a fixed high-water fraction of the limit, or adaptive? Resolve empirically after D1 makes the exchange bytes visible.
- Do scan decode buffers (D3) need reservation, or does the `RecordBatch` they produce already get counted once it enters a tracked operator? Measure the double-count risk before wiring scan-side reservations.
- Is a single process-global pool sufficient, or is a per-query sub-pool (DataFusion `TrackConsumersPool` + per-query split) needed so one heavy query can't starve a co-resident one? Defer until multi-query-per-worker is a real scenario.
- Does SF100 fit on the 8-core/31 GB host once spill engages (disk volume), or is SF100 a separate capacity exercise? Inherited from `exec-exchange-backpressure`.
