## Why

arneb's heaviest distributed queries (TPC-H q09, q21) **intermittently fail** at SF10 on a resource-constrained host (8-core / 31 GB) — not slowly, but with a hard failure: `"worker never registered OutputBuffer within 5 min"`. Direct measurement this session, same host and seeded SF10 data:

- **arneb q09 SF10** completes in **~28.6 s when it works** (competitive with — even faster than — Trino), but **intermittently stalls**: a `RepartitionExec` waits ~40 s for its first batch while every worker sits at **~1.5 % CPU** (idle — not CPU-bound, not OOM, worker RSS ~300 MB). These inter-stage waits accumulate past the **hard 5-minute OutputBuffer-registration deadline**, so the coordinator's Flight `do_get` fails the whole query.
- **Trino on the same host + data** completes q09 SF10 **3/3 reliably** (60 / 34 / 26 s) — it has no such failure mode.

Ruled out as causes: NOT cgroup/CPU cap (stalls identically at `cpus=2` and `cpus=5` per node; CPU idle during the stall — not throttled, not starved), NOT over-subscription (idle CPU), NOT OOM (~300 MB), NOT host data/RAM capacity (arneb completed 20/22 SF10 queries; q09 completes warm). **The host can do it; arneb's exchange intermittently can't.** This is the long-standing "inter-stage idle" weak spot flagged repeatedly across the q09 investigation — now reproduced cleanly and isolated. It is a **reliability gap in arneb's distributed exchange**, not a speed gap and not a host/config problem.

Now, because: (1) it is the *only* real correctness/reliability deficit versus Trino on the hardest queries, (2) it blocks the goal of running SF10 (and SF100) on commodity/constrained hosts, and (3) we finally have a clean, isolated reproduction and ruled out the environmental red herrings.

## What Changes

- Replace the unbounded/blocking inter-stage exchange with a **bounded buffered exchange**, so a slow consumer applies smooth back-pressure without deadlocking the producer. (Spill-to-disk of the bounded overflow is **descoped to the memory-accounting change** — see below — since it must coordinate with the global `MemoryPool`; D1 liveness + the bounded buffer removed the SF10 reliability gap without disk spill.)
- **Remove or make adaptive the hard 5-minute OutputBuffer-registration deadline** so a genuinely slow stage degrades to *slow*, never to a hard *failure*. A heavier scale (SF100) or fewer cores must not trip a fixed wall-clock deadline.
- Guarantee **admission/scheduling is fully decoupled from stream back-pressure** (building on the Phase-A removal of the `task_manager` admission semaphore) — a producer blocking on a full output buffer must never hold a resource that prevents its downstream consumer from being admitted. **BREAKING for the prior streaming refactor approach**: the 2026-05-23 per-batch streaming output must be re-landed deadlock-free under this constraint.
- Target: an 8-core host reliably completes **all 22 TPC-H queries at SF10**, and the exchange scales to **SF100** with no fixed internal deadline.

## Capabilities

### New Capabilities
- `exchange-backpressure`: bounded, flow-controlled inter-stage (worker-to-worker Flight) exchange with no hard wall-clock registration deadline — a slow stage degrades to slow (parking on the bounded buffer + liveness), never to a hard query failure. (Disk-spill of the bounded overflow for SF30/SF100-scale intermediates is descoped to the memory-accounting change, which owns tracking + spilling the exchange channel through the global `MemoryPool`.)

### Modified Capabilities
- `repartition-exec`: its consume/produce flow-control requirement changes — it must propagate bounded back-pressure to its upstream (and downstream) without deadlocking, rather than blocking indefinitely on a channel while its peer waits.

## Impact

- **Code**: `crates/execution` (exchange / repartition operators, output buffering), `crates/rpc` (Arrow Flight output buffer + `do_get` registration/timeout), `crates/server` (`task_manager` admission — confirm full decoupling from stream back-pressure).
- **Behaviour**: heavy distributed queries (q09/q21) become reliable at SF10 on constrained hosts; the `"OutputBuffer never registered within 5 min"` hard-fail path is removed/replaced. SF30/SF100-scale degrade-to-disk (when the bounded exchange overflows) is delivered by the follow-on memory-accounting change, not here.
- **Risk**: re-introducing the 2026-05-23 producer-blocked-while-holding-admission deadlock — the design must explicitly prevent it. Distributed correctness (trino-diff cell parity) must hold across the change.
- **Out of scope**: the cgroup/bench-harness CPU-cap auto-detection (done this session) and per-query algorithmic speed wins (cache-fit / parallel-probe — orthogonal, already flag-gated).
