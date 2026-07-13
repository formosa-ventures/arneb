## 1. Baseline & instrument (measure before changing)

- [~] 1.1 Instrumentation LANDED (measurement remote-gated): `MemoryPool::reserved_peak()` high-water (GreedyMemoryPool + UnboundedMemoryPool track peak; QueryMemoryPool delegates to global = worker-level peak) + a worker `arneb::mem` "worker tracked memory" event at task completion (`tracked_peak_bytes` / `tracked_now_bytes`). The actual SF30 baseline capture (tracked_peak vs RSS, expect ≈3% before D1's effect) needs a constrained remote bench host (access details in memory, not in-repo) + the `arneb::mem` overlay — run next session.
- [x] 1.2 **Attribute the untracked peak — DONE via jemalloc heap profile (2026-06-08), the
  mandate after 3 code-guesses failed.** Built a `heap-profiling` image (`tikv-jemallocator/profiling`
  = jemalloc `--enable-prof`, gated cargo feature; Dockerfile `CARGO_FEATURES` build-arg;
  `docker-compose.heapprof.yml` sets per-node `_RJEM_MALLOC_CONF=prof:true,...,lg_prof_interval:31`
  + host-mounts `/extdrive/jeprof`). Ran q18 SF30 to the worker OOM, `jeprof --text --cum` on the
  near-peak dump (PIE: `sed` the binary path in the dump; jeprof from jemalloc-5.3.0 `bin/jeprof.in`):
  - **PASS 1 (before any fix): 95.7 % of the 10.76 GB peak = `UnionAllExec::execute` → `collect_stream`**
    (set_ops.rs) fully materialising every child of the distributed GATHER node (~90M-row exchange).
    +4.0 % = `arrow_flight::flight_data_to_arrow_batch` (transient Flight decode, RECEIVE side).
    NOT the aggregate / hash-join-build-concat / projection (the 3 disproven code-guesses).
  - **PASS 2 (after streaming UnionAll): 96.5 % of the 10.71 GB peak MOVED to
    `HashJoinExec::execute_grace_single` → `collect_stream` (hash_join.rs:3188)** — the cache-fit
    (no-spill) probe path drains the ENTIRE left/probe input untracked (q18 probe = the wide
    lineitem⋈orders intermediate). The peak is unchanged because the consumer re-collects what
    UnionAll stopped buffering. **This is the next target** (see §4.4).
  See [[project_2026-06-08_q18_oom_heapprofile_rootcause]] for the full chain + addr2line citations.
- [x] 1.3 Pool wiring confirmed: `main.rs:394-407` builds `global_pool` = Greedy(cgroup bytes) | Unbounded, optionally wrapped in `QueryMemoryPool(query_cap)`, plumbed via `.with_memory_pool()` (`task_manager.rs:360`). Already-reserving sites: `collect_stream_pool_tracked` (SortExec/TopKExec inputs, `operator.rs:351`), HashJoinExec build + spill-load (`hash_join.rs:727/865/1634/1885`, `with_can_spill(true)`). Pattern = `MemoryConsumer::new(name).register(pool)` → per-batch `try_grow`, RAII `Drop` releases. Gap: `RepartitionExec` channel + rpc `OutputBuffer` hold no reservation.

## 2. D1 — Track exchange buffers in the global pool

- [x] 2.1 RepartitionExec channel pool-tracked: `memory_pool` field (default `Unbounded`) + `with_memory_pool` setter; each routed batch carries a `ChannelReservationGuard` (`try_grow` best-effort on enqueue, RAII `Drop` releases when the consumer pulls it in `MpscStream::poll_next` or the channel drops — leak-safe both paths). D1 best-effort: `try_grow` failure → granted 0, no gating (gating/spill = D2). Wired the distributed worker site (`task_manager.rs:462`); single-node planner path defaults to `Unbounded`. (`crates/execution/src/repartition.rs`)
- [x] 2.2 rpc `OutputBuffer` pool-tracked via `TrackedSender`/`TrackedReceiver` wrappers (guard mechanics encapsulated; external `.send`/`.try_send`/`.recv() -> Option<RecordBatch>` APIs unchanged). Channel item is now `TrackedBatch { batch, guard }`; guard `try_grow`s on enqueue (best-effort D1) and releases when the consumer pulls it (`TrackedReceiver::recv`) or the channel drops — leak-safe. Wired the worker site (`task_manager.rs:560`); `take_senders`/`take_receiver`/`write_batch`/`flight_service::async_stream` updated; `try_send` preserved (pumper's Full→spill path keeps working, batch recoverable + reservation released). **Broadcast (`BroadcastOutputBuffer`) deferred** — separate in-memory-replay path, bounded-small by design (only small build sides broadcast); follow-up.
- [x] 2.3 Unit tests (both halves): `channel_bytes_are_pool_tracked_and_released` (RepartitionExec) + `output_buffer_bytes_are_pool_tracked_and_released` (OutputBuffer) — peak-spy pool asserts reservation grows in flight (`peak > 0`) and fully releases on consume (`reserved == 0`, no leak). 824 workspace lib tests green; clippy clean.
- [ ] 2.4 Re-measure SF30 worker peak: tracked fraction jumps from ~3 % toward the exchange's real share; **no behaviour change yet** (still bounded, not spilling). Record the new tracked-vs-RSS ratio

## 3. D2 — Pool-pressure disk-spill of exchange overflow

- [x] 3.1 `OutputBuffer` exchange: on `try_grow` failure under pool pressure the overflow batch is spilled to disk (Arrow IPC, reusing `spill.rs` `SpillWriter`), with the reservation released (never made for `PoolFull`; dropped-guard release for `ChannelFull`) and the spill file drained FIFO at stream end (`sender.send` re-reserves best-effort on read-back). New `TrackedSender::try_send_pooled` → `TrackedSendOutcome{Sent,PoolFull,ChannelFull,Closed}` (`output_buffer.rs`); pumper wired (`task_manager.rs`). (RepartitionExec intra-worker channel deferred — the worker-to-worker `OutputBuffer` is the SF30 thrash dominator and already had the spill machinery.)
- [x] 3.2 Trigger is **global pool pressure** = `try_grow` failure (the trait exposes `reserved()`/`try_grow` but not `limit()`, so the natural pressure signal is `try_grow` itself — matches the design). Fast path with the default `UnboundedMemoryPool` (single-node / tests) never returns `PoolFull` → never spills; test `try_send_pooled_fast_path_unbounded_never_pool_full` asserts this.
- [x] 3.3 Deadlock-safety holds BY CONSTRUCTION: `try_send_pooled` hands the overflow batch back with NO live reservation (PoolFull never reserved; ChannelFull guard dropped), the spill `write` is synchronous (no `.await`, no reservation held), and the end-drain `sender.send().await` blocks on channel capacity holding no pool reservation or lock — so nothing is held across a back-pressure park or spill I/O. (Documented inline at the call site.)
- [x] 3.4 Tests: (trigger) `try_send_pooled_spills_on_pool_pressure` (tight pool → PoolFull, rejected batch NOT reserved) + `try_send_pooled_fast_path_unbounded_never_pool_full` (Unbounded → never PoolFull); (drain/no-loss, 3.4a) the pumper's spill-drain was extracted to `drain_spill_to_sender` and `drain_spill_preserves_all_batches_in_fifo_order` asserts 5 spilled batches arrive through the tracked sender in FIFO order with no loss/dup; (round-trip) reused `spill.rs`'s existing 5-batch round-trip test; (release/no-leak) D1's `output_buffer_bytes_are_pool_tracked_and_released`. 29 rpc + 12 server tests + clippy clean. (3.4c deadlock-regression is moot: Phase A removed the admission semaphore, and `try_send_pooled`/drain hold no pool reservation across any `.await` — D4 invariant preserved by construction.)
- [x] 3.5 Remote SF30 (a constrained bench host — access details in memory, not in-repo; bounded pool confirmed `query_max_memory_per_node=6.44 GB`,
  `spill_budget_source=config` → D2's `try_grow`-failure path is genuinely active): **q21 SF30
  COMPLETES — exit 0, 230 s, 100 rows (was OOM)**; **q09 SF30 STABLE across 3 runs — 306 / 343 /
  326 s, all 25 rows (was bistable 333 s vs >600 s)**. 91 "exchange overflow spilled to disk"
  events fired; no OOM-kill, no worker crash. D2 catches the aggregate-memory case the fixed
  channel-cap spill misses (it spills on TOTAL pool pressure, not per-channel-full). _Caveat:
  no controlled A/B (q21 WITHOUT D2) re-run on this exact host — the OOM baseline is from the
  proposal's prior measurement; the bounded pool + 91 spills + q21-now-completes are strong
  circumstantial confirmation._
  - **SF30 cell-diff vs Trino (D2 CORRECTNESS gate, N=2, follow-up run): q01/q03/q05/q07/q08/q09
    all cell-identical (1e-9)** — including q09 which spills heavily (1.4–4.9 GB spilled, FIFO
    drain preserves values). So D2's spill is correctness-clean.
  - **BUT D2 is necessary, NOT sufficient, for q18 SF30: worker OOM-killed (`Exited 137`).**
    D2's exchange spill fired (tracked_peak 3.65 GB < 6 GB cap) but the worker RSS still blew the
    cgroup because of UNTRACKED diffuse operator intermediates — the logs show a single
    `ProjectionExec` output at **5.2 GB** and a 90 M-row probe intermediate that no reservation
    covers. This is exactly the **D3** territory (track `FilterExec`/`ProjectionExec`/scan-decode
    batches). q21 in the same run failed only collaterally (it could not connect to the
    already-dead worker-2). **Conclusion: D2 lands the exchange half; full SF30 memory safety
    (q18 completes, no worker OOM) needs D3. Re-run the diff+bench AFTER D3.**

## 4. D3 — Sweep the diffuse allocations

- **D3 attribution (2026-06-06, from the SF30 q18 OOM): the design's "FilterExec/ProjectionExec
  first" ordering is WRONG for the OOM case.** Those are STREAMING operators that hold ~1 batch at
  a time — the log's `ProjectionExec bytes=5.2 GB` is CUMULATIVE over 1406 batches (~3.7 MB
  instantaneous), not retained memory. The actual untracked ACCUMULATOR that OOM-killed worker-2 is
  the **`HashAggregateExec` group state** (`aggregate.rs` / `group_by_hash.rs` have ZERO pool
  reservation — confirmed by grep). q18's `lineitem GROUP BY l_orderkey` builds ~45 M groups
  (~2 GB+) on one FIXED worker, untracked + non-spillable. So D3 must track the AGGREGATE first.
  Key insight: just making the aggregate VISIBLE to the pool lets D2's exchange spill BALANCE
  (aggregate grows → pool pressure → exchange spills → tracked stays under cap → no worker OOM) —
  the aggregate may not even need its own spill if the spillable exchange yields enough.
- [~] 4.1 **`HashAggregateExec` group state now pool-tracked (the real OOM accumulator).** Added a
  `memory_pool` field (default `Unbounded`), plumbed from `ExecutionContext` at all 3 planner
  construction sites; `GroupByHash::heap_bytes()` (table + key-store capacities) + a coarse
  ~16 B/group/aggregate estimate are `try_resize`d per batch in BOTH grouping batch-aware paths
  (`execute_streaming_batch_aware` + the `execute_parallel_batch_aware` global merge). Fail-fast
  `ResourceExhausted` on pool exhaustion (vs the worker OOM-kill); Unbounded default → never fails
  → existing aggregate behaviour unchanged. Test `hash_aggregate_group_state_fails_fast_under_tight_pool`
  (2000 groups / 256 B pool → ResourceExhausted); 227 execution tests + clippy + workspace build green.
  **Remaining (follow-up):** the legacy `execute_streaming` (DISTINCT) path, `execute_no_grouping_*`
  (bounded-small), and `StreamingHashAggregateExec`; refine the coarse accumulator estimate; and
  **re-validate q18 SF30 on the remote** (does the aggregate-tracking + D2 exchange spill now keep
  the worker under the cgroup → q18 completes instead of OOM-kill?).
  Then the streaming `FilterExec`/`ProjectionExec` batches only if 1.2 attribution shows residual
  instantaneous share (they hold ~1 batch each, so likely low).
  - **REMOTE RE-VALIDATION (2026-06-06): D3 aggregate-tracking is INSUFFICIENT for q18 SF30 — the
    worker STILL OOM-kills (`Exited 137`, this time worker-1; only 10 spill events).** So the
    aggregate was NOT the (sole) dominant untracked site. Host is NOT oversubscribed by other
    services (~1.5 GB total), so this is genuinely arneb untracked memory elsewhere — most likely
    the 90 M-row HashJoin probe intermediate / `ProjectionExec` output / Parquet decode. **Lesson
    (again): do task 1.2 ATTRIBUTION (jemalloc heap profile during a q18 run) to find the real
    dominant site BEFORE more tracking — blind reasoning ("aggregate must be it") was disproven by
    the remote, twice.** D3 aggregate-tracking is KEPT (a real untracked gap closed, tested,
    correctness-clean, makes the aggregate pool-visible) but is not the q18 fix. q18 SF30 full
    memory safety is deferred to attribution-first D3 continuation.
  - **ATTRIBUTION DONE (code-level, 2026-06-06) — q18's dominant untracked retainer is in
    `HashJoinExec::execute_single` (`hash_join.rs`), NOT the aggregate.** The build is collected
    via `build_with_spill` (tracked), but `right_combined = concat_batches(...)` (~line 2941)
    materialises it into a SINGLE ~5 GB batch (q18: 90 M rows) and immediately `let _ = reservation;`
    (~line 2946) DROPS the build reservation — so `right_combined` is held UNTRACKED through the
    whole probe; the probe-side `left_batches = collect_stream(left_stream)` (~line 2892, ~22.5 M
    rows) is also untracked. Matches the worker log (`right_rows=90 M`). Resolves the profile
    red herring: profile `bytes=` is CUMULATIVE (total over batches), not retained — streaming
    ProjectionExec's "5.2 GB" was never the retainer; the concat is. **Fix shape:** keep the build
    reservation alive through the probe (thread it into the `execute_single_finish` stream instead
    of dropping at 2946) + `collect_stream_pool_tracked` the `left_batches` collect → converts the
    worker OOM-crash to a clean `ResourceExhausted` (no worker death / no q21 collateral). FULL q18
    completion additionally needs a streaming probe (no 90 M-row concat) = the previously-reverted
    streaming refactor → separate deadlock-prone follow-up.
  - **FIX IMPLEMENTED (2026-06-06, local+tested):** `execute_single` now (a) collects the probe
    side via `collect_stream_pool_tracked` (was untracked `collect_stream`) and (b) HOLDS both the
    probe + build reservations through the probe via a new `ReservationHoldingStream`/`hold_reservations`
    wrapper (was `let _ = reservation;` dropping the build before the probe). Only the `Single` arm
    holds (q18's path); the `Multipass` arm drops the probe reservation before re-probing (build
    already on disk) to preserve its tight per-pass load budget. Tests: `execute_single_probe_collect_
    fails_fast_under_tight_pool` (small build + 5000-row probe + 500 B pool → clean ResourceExhausted,
    not OOM) + the existing `hash_join_inner_spill_multipass` (re-tuned). 228 execution tests + clippy
    + workspace build green. **q18 SF30 should now FAIL-FAST clean (no worker OOM / no q21 collateral)
    or COMPLETE if it fits ~6 GB — REMOTE RE-VALIDATION PENDING** (may still OOM if other untracked
    sites — Parquet decode — push RSS past the cgroup beyond the now-tracked join working set).
  - **REMOTE RE-VALIDATION (2026-06-06): the HashJoin fix ALSO does NOT prevent the q18 worker OOM
    (`Exited 137`), and CRUCIALLY no `ResourceExhausted` fired** — so the tracked total never reached
    the 6 GB pool; the worker RSS blew the cgroup from memory the pool does NOT see at all. **Three
    code-reasoned D3 targets (aggregate, then HashJoin probe/build) have now each failed to stop the
    q18 OOM on the remote.** The dominant untracked memory is diffuse / allocator-level — candidates
    the operator-tracking can't reach: Parquet scan decode buffers, the exchange RECEIVE side
    (D1/D2 tracked only the OutputBuffer SEND side / Flight decode on the consumer), and Arrow /
    tikv-jemalloc retained-but-freed pages. **MANDATE: stop code-guessing the dominant site — the
    next step MUST be a jemalloc heap profile (task 1.2, "Track B") during a q18 SF30 run to
    attribute the real top allocators, THEN track/spill those.** D2 + the D3 aggregate + HashJoin
    reservation work are all KEPT (real untracked gaps closed, tested, correctness-clean, q21/q09
    complete) but q18 SF30 full memory safety is blocked on the heap-profile attribution.
- [x] 4.1b **`UnionAllExec::execute` now STREAMS its children (the heap-profile PASS-1 dominant
  site).** Was `for child { all_batches.extend(collect_stream(child)) }` then `stream_from_batches`
  — fully materialised the distributed gather (~10 GB, untracked). Now an `async_stream::try_stream!`
  drains child[0]→child[1]→… lazily, yielding each batch (UNION ALL = order-independent concat, so
  correctness-clean; no back-pressure deadlock — Phase A removed the admission semaphore, forwards
  no reservation across `.await`). 2 new TDD tests (laziness: children not executed until polled +
  sequential; concatenation correctness). 230 execution tests + clippy green. **Remote SF30: q18
  COMPLETES (was Exit-137 OOM), 100 rows, 100/100 cell-identical to Trino.** (set_ops.rs)
- [~] 4.1c **Budget-gated streaming probe — the genuine stable fix.** A probe that FITS the pool →
  fast collect-based path (unchanged, now reservation-held). A probe that OVERFLOWS → bounded
  STREAMING probe via the proven `execute_grace_inner` (the same path the real-spill case already
  uses in production — NOT the reverted refactor). New helpers `collect_probe_within_budget` +
  `prepend_batches` + `ProbeCollect{Fits,Overflow}` (operator.rs, `try_grow` failure = the overflow
  signal, D2-consistent). Applied to BOTH:
  - `execute_grace_single` Partitioned cache-fit branch (hash_join.rs:3188) — composite-key joins.
  - **`execute_grace_single` Single branch (hash_join.rs:3243) — THE q18 site.** Correction: q18's
    joins are all SINGLE-KEY (`o_orderkey=l_orderkey`, `c_custkey=o_custkey`), so they never qualify
    for the composite-key cache-fit path (gate needs `right_key_indices.len() >= 2`); they return
    `BuildChunksResult::Single` and collected the whole probe at 3243 — verified by the heap profile
    (the dominant site after 4.1b was STILL `execute_grace_single → collect_stream`, the Single
    branch). The Overflow path here routes through `execute_grace_inner` with a 1-partition build +
    empty `PartitionedSpillWriter…finish()`.
  Tests: `collect_probe_within_budget_{overflows,fits}` + `prepend_batches_…` (operator.rs) +
  `grace_single_probe_overflow_streams_and_is_correct` (hash_join.rs, tight pool → Overflow →
  streaming → correct INNER matches). 234 execution tests + clippy green; all existing Grace-HJ
  tests unaffected (Fits path unchanged). **REMOTE q18 SF30 VALIDATED: 10.7 GB OOM/fragile → 7.4 GB
  peak, COMPLETES robustly (3.6 GB cgroup margin), 100 rows, 100/100 cell-identical to Trino,
  ~5m44s, NO deadlock/hang.** The stale deadlock comment (3175-3184) does NOT apply post-Phase-A —
  confirmed empirically (`collect_probe_within_budget` shows in the heap profile bounding the probe).
- [ ] 4.2 Bring Parquet scan decode buffers under reservation (`crates/connectors`); first verify the double-count risk (does the produced `RecordBatch` already get counted downstream?) per the design Open Question
- [ ] 4.3 After each audited batch, re-measure SF30 worker peak: tracked bytes climb toward true RSS; record the converging ratio
- [ ] 4.4 Tests: reservation grows/releases correctly for each newly-tracked site; no leak under error/cancel paths

## 5. D4 — Diagnostics: name the largest consumers on OOM

- [x] 5.1 `TrackConsumersPool` decorator (`crates/common/src/memory_pool.rs`) records per-consumer reserved bytes (HashMap keyed by `MemoryConsumer::name`); wraps any inner pool. Wired in `main.rs` over the bounded `GreedyMemoryPool` (Unbounded left unwrapped — never OOMs), top_n=5.
- [x] 5.2 On inner `try_grow` failure the error is augmented with `top consumers: [name=bytesB, ...]` (largest first). Test `track_consumers_pool_names_top_on_oom` asserts the biggest consumer is named.
- [x] 5.3 Success path transparent: `try_grow` Ok just does one HashMap insert; `reserved`/`reserved_peak` delegate to inner. Test `track_consumers_pool_success_path_is_transparent`. 19 common pool tests green; clippy clean.

## 6. Validate & ship

- [ ] 6.1 Single-node 22/22 unit + doc tests + `clippy --all-targets` green
- [ ] 6.2 trino-diff full suite at SF1 cell-parity (1e-9) — no correctness regression
- [ ] 6.3 Distributed-mode validation on the constrained host: **q21 SF30 completes** within the bound; q09/q21 SF30 stable (N repeats); record worker peak (tracked & RSS) + per-query latency vs Trino; no latency regression on the SF10 winning set beyond a documented bound
- [ ] 6.4 SF100 degrade-to-disk smoke on a suitably-resourced host (or documented as a follow-up if seeding/capacity is a separate exercise)
- [ ] 6.5 Update memory + resumption with the outcome: tracked-vs-RSS convergence, q21 SF30 status, which alloc sites dominated, and the SF30 stability result
