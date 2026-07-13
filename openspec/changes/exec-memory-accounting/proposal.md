## Why

arneb's `MemoryPool` (`crates/execution/src/memory_pool.rs`, a vendor-port of DataFusion's API — `GreedyMemoryPool` only) is consulted **only at the operator level** via `try_grow` (HashJoin/SemiJoin build, aggregate). The large Arrow allocations that actually blow the cgroup at SF30 **bypass it entirely**: `RepartitionExec` channel buffers, the rpc `OutputBuffer` (worker-to-worker exchange), Parquet scan decode buffers, and intermediate `FilterExec`/`ProjectionExec` batches.

Direct SF30 q09 profile evidence (this project's measurements):

- HashJoin **build state is only ~165 MB** per worker, yet **worker peak hits ~9.5 GB**; **q21 SF30 OOMs**; a clean SF30 run **thrashed the host to ~23 GB / SSH-unresponsive**.
- `partition_count`↑ produced **no memory benefit** — because untracked allocs scale with the node's **total** data, not per-partition. Splitting into more partitions cannot cut a peak the pool cannot see.
- These untracked allocs make SF30 runs **bistable** (one run 333 s, another >600 s, q21 dies) — which **pollutes every SF30 measurement**.

This is "Killer 3" in the SF30 attack plan and is the **prerequisite that makes every other SF30 optimisation measurable**: until allocations are tracked, the bench/profile gates for the dynamic-filter and distribution-property changes will be fighting the same bistable/host-death noise. It is also the **only thing that makes q21 SF30 survive** — a speed win is meaningless if the query OOMs.

Reference designs (from `docs/software-arch/`, all locally source-verified): DataFusion accounts every large allocation through `MemoryReservation`/`try_grow` and uses `TrackConsumersPool` to name the biggest consumer on OOM (`datafusion.md §8`); ClickHouse intercepts at the allocator layer with a global `hard_limit` (`clickhouse.md §8.1`); DuckDB routes all large allocations through `BufferManager` so nothing bypasses tracking (`duckdb.md §8`). The Rust-native path is DataFusion-style "register every large reservation through the existing `MemoryReservation`", **not** a custom `GlobalAlloc`.

This change also **inherits the exchange disk-spill that was descoped** from the just-archived `exec-exchange-backpressure` change (its D2): that spill must coordinate with the global pool (spill under pool pressure, not at a fixed per-channel cap), which is exactly what this change builds.

## What Changes

Land incrementally, each phase measurement-gated (the fast path — pool not under pressure — must be unchanged):

- **Phase 1 — Track the exchange buffers in the global pool.** Register the bounded `RepartitionExec` channel and the rpc `OutputBuffer` as `MemoryReservation`s against the global `MemoryPool`, so their bytes count toward the worker limit. (The tracking half of the descoped D2.)
- **Phase 2 — Pool-pressure disk-spill of the exchange overflow.** When the global pool — not a fixed per-channel cap — is under pressure, spill exchange-channel overflow batches to disk (Arrow IPC, reusing the SemiJoin/HashJoin Grace spill machinery) and drain them in order. Delivers the SF30/SF100 degrade-to-disk that `exec-exchange-backpressure` deferred. (The spill half of the descoped D2, now correctly pool-coordinated.)
- **Phase 3 — Track the remaining diffuse allocations.** Bring Parquet scan decode buffers and intermediate `FilterExec`/`ProjectionExec`/`RepartitionExec` batches under `MemoryReservation` — the broader "track every large reservation" sweep. (Punch list of ~160 file:line hotspots already exists in this project's alloc audit.)
- **Optional — better OOM diagnostics.** A `TrackConsumersPool` equivalent that names the biggest consumer on `ResourceExhausted`, to make the diffuse Phase-3 work debuggable.

## Capabilities

### New Capabilities
- `memory-accounting`: every large Arrow allocation on a worker (exchange channels, OutputBuffer, scan decode buffers, intermediate operator batches) is registered against a single global `MemoryPool`, so the worker's tracked memory reflects its true peak; under pool pressure the spillable consumers (exchange overflow, join/aggregate build) degrade to disk rather than OOMing the process.

### Modified Capabilities
- `repartition-exec`: its bounded channel becomes pool-tracked and, under global pool pressure, spills overflow to disk (the descoped-from-`exec-exchange-backpressure` behaviour, now pool-coordinated).
- `exchange-backpressure`: the bounded exchange gains pool-tracked buffering + pool-pressure disk-spill (completing the SF30/SF100 degrade-to-disk it deferred).

## Impact

- **Code**: `crates/execution` (`memory_pool.rs`, `repartition.rs`, `spill.rs`, scan/`operator.rs` buffers), `crates/rpc` (`output_buffer.rs`), `crates/connectors` (Parquet decode buffers).
- **Behaviour**: q21 SF30 **completes** on the constrained 8-core/31 GB host within a bounded worker peak (no OOM, no host-death thrash); SF30 q09/q21 runs become **stable** (not bistable); worker peak reflects the true allocation total. Peak disk usage rises when pool-pressure spill engages (bounded, configurable).
- **Risk**: over-counting/under-counting reservations could either spill too eagerly (slow) or too late (OOM); the fast path (pool not pressured) must add no measurable overhead. Distributed cell-correctness (trino-diff 1e-9) must hold throughout.
- **Out of scope**: Killer 1 (distribution-property / correct broadcast — separate later change); `cross-fragment-dynamic-filter` (separate existing change); per-query algorithmic speed wins (cache-fit / parallel-probe — orthogonal, flag-gated).
