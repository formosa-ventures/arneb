## 1. Reproduce & instrument baseline

- [x] 1.1 Stand up a reliable SF10 repro of the q09/q21 stall (warm cluster, no per-query recreate, on a constrained host or via an artificial per-node CPU cap) and capture the failing signature (`RepartitionExec first_batch_ms` ~40s, CPU idle, `do_get` OutputBuffer 5-min hard-fail)
- [x] 1.2 Locate and document the hard deadline: the OutputBuffer-registration timeout in the Flight `do_get` path (`crates/rpc`) — exact constant, where it is enforced, what error it raises
- [x] 1.3 Locate the current back-pressure path: `RepartitionExec`/`MpscStream` bounded channel (`crates/execution/src/repartition.rs`) and the worker OutputBuffer producer (`crates/rpc`), and confirm where a producer parks when the buffer/channel is full
- [x] 1.4 Add a deterministic test harness for an artificially slow consumer (so the stall is reproducible without a 60M dataset) to drive the unit/integration tests below

## 2. D1 — Replace the hard wall-clock deadline with liveness

- [x] 2.1 Determine the liveness signal: confirm whether the producer already emits periodic progress on the Flight stream; if not, add a lightweight producer heartbeat / progress counter
- [x] 2.2 Replace the fixed 5-minute OutputBuffer-registration deadline in `do_get` with a liveness check: fail only on producer death or no-progress-for-heartbeat-interval, not on elapsed wall-clock time
- [x] 2.3 Add an optional, configurable large safety ceiling (default unbounded / very large) via `arneb.toml`, distinct from liveness
- [x] 2.4 Tests: (a) a slow stage that exceeds the old deadline now COMPLETES (slow, not failed); (b) a dead producer is detected promptly and fails with a clear error
- [x] 2.5 Re-run the SF10 q09/q21 repro from 1.1 — expect: completes (slow) instead of OutputBuffer hard-fail. Record whether D1 alone resolves it (per design Open Question)

## 3. D2 — Bounded exchange buffer (back-pressure). [disk-spill descoped → memory-accounting change]

> **Descope recorded 2026-06-04.** D1 (liveness) + the bounded in-memory buffer removed the SF10 hard-fail without disk spill — the stall was the wall-clock deadline, not unbounded exchange memory (RSS ~300 MB). The original 3.1–3.5 spill-to-disk plan is moved to the upcoming memory-accounting change ("Killer 3"), which must spill the exchange channel **through the global `MemoryPool`** (spill under pool pressure, not a fixed per-channel cap) — doing it here in isolation would be retrofitted. Only the bounded (back-pressure) half ships here.

- [x] 3.1 Bound the in-memory exchange: per-partition `mpsc::channel` with configurable capacity (`RepartitionExec`); OutputBuffer cap=64 — producer parks on full channel (back-pressure) instead of unbounded accumulation
- [x] 3.2 Make the in-memory channel capacity configurable via `[execution] channel_capacity` in `arneb.toml` (default 4)
- [~] 3.3 ~~spill-file lifecycle~~ — **DESCOPED → memory-accounting change** (exchange disk-spill must coordinate with the global `MemoryPool`)
- [~] 3.4 ~~Tests: large-intermediate spill / fast-path zero-spill~~ — **DESCOPED → memory-accounting change** (no spill path in this change)
- [x] 3.5 Re-ran SF10 q09/q21 reliability (warm, repeated) — completes reliably with bounded exchange memory (no OOM) via D1+bound alone; SF30/SF100 disk-degrade tracked in the memory-accounting change

## 4. D3 — Re-land deadlock-free per-batch streaming output

- [x] 4.1 Re-introduce per-batch streaming output (the reverted 2026-05-23 `async_stream::try_stream!` approach) on top of the Phase-A admission-semaphore removal, so a worker registers/produces its OutputBuffer incrementally (first batch ASAP)
- [x] 4.2 Audit the producer path: assert no lock, admission permit, or `MemoryPool` reservation is held across a back-pressure park (`.await` on a full buffer)
- [x] 4.3 Streaming output verified on HEAD: `async_stream::try_stream!` per-batch path present (`operator.rs:2077`, `hash_join.rs` ×6); 239/239 `arneb-execution` lib tests green; deadlock-free-under-slow-consumer property proven by `d4_producer_blocked_on_full_buffer_does_not_block_consumer` (passes). First-batch-latency-drops is a bench assertion covered by the 6.3 distributed validation, not a unit test.

## 5. D4 — Deadlock-regression guard (decouple admission from back-pressure)

- [x] 5.1 Add a regression test for the exact deadlock class: a producer parked on a full exchange buffer while a dependent downstream consumer awaits admission — assert the consumer is still admitted and the query progresses
- [x] 5.2 Confirm the RSS-based admission gate (polling, no held permit) and all admission paths cannot be starved by a parked producer; document the invariant

## 6. Validate & ship

- [x] 6.1 Single-node 22/22 unit + doc tests + clippy --all-targets green
- [x] 6.2 trino-diff full suite at SF1 cell-parity (1e-9) — no correctness regression
- [x] 6.3 Distributed-mode validation: q09/q21 reliable at SF10 (warm, N repeats) on the constrained host, cell-identical to Trino; record per-query latency vs Trino
- [~] 6.4 SF100 reliability smoke — **DEFERRED as follow-up** (needs a suitably-resourced host; SF100 degrade-to-disk depends on the exchange disk-spill that is descoped to the memory-accounting change, so SF100 smoke belongs with that change)
- [x] 6.5 Memory updated with the outcome: D1 (liveness) was the necessary+sufficient fix for the SF10 hard-fail; bounded buffer shipped; D2 disk-spill descoped to the memory-accounting change; D3 streaming re-landed; D4 guard green
