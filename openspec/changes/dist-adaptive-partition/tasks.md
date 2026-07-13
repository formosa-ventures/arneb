## 1. Baseline & estimate source (spike)

- [x] 1.1 Exchange-insertion site = `fragment.rs:615`; the `partition_count` binding feeds all 4 join exchange sites (broadcast path :667/:687, default path :704/:708/:727). Both join children are hash-repartitioned, so per-partition pressure is driven by `max(estimated_cardinality(left_plan), estimated_cardinality(right_plan))` (the bigger child stream being distributed). Join *output* cardinality matters for the NEXT exchange, not this one. (Resolves design Open Q2.)
- [x] 1.2 Estimate source = `crate::cost::estimated_cardinality(plan, &CatalogStats)` (cost.rs:97; falls back to `DEFAULT_TABLE_SIZE=10000` when a leaf has no stats). `stats` is ALREADY on the fragmenter (`with_stats`, wired at `coordinator.rs:90` from `exec_ctx.catalog_stats()`). `worker_count` = `node_registry.alive_count()` — available in the coordinator (`coordinator.rs:59/103`) but NOT yet threaded into the fragmenter → needs a new `.with_worker_count()` builder.
- [ ] 1.3 Record the current N=2 SF30 baseline for q09 + q18 on the remote host (per-partition peak RSS, total cluster peak, latency, cell-correct vs Trino). PARTIAL: this session's SF30 bench already has totals (q09 293s / 20.2 GB cluster; q18 OOM). Per-partition-specific RSS deferred to the remote validation phase (task 5.3/6.1).

## 2. Config knobs (runtime-tunable)

- [x] 2.1 Add `ARNEB_HASH_PARTITION_TARGET_ROWS` (target rows-per-partition) and `ARNEB_MAX_HASH_PARTITIONS` (guardrail cap) resolution in `crates/server/src/config.rs`, mirroring `resolve_budget()` / `resolve_query_cap()` (env → config → default)
- [x] 2.2 Pick safe in-source defaults so SF1/small plans still produce ≤ 2 partitions (no behaviour change at small scale); log the effective resolved values
- [ ] 2.3 Unit tests for the resolvers: env override wins, config next, default last; effective value is the resolved one

## 3. The adaptive rule (pure, test-first)

- [x] 3.1 Write failing unit tests for a pure `choose_partition_count(worker_count, estimated_rows: Option<u64>, target_rows, max)` helper: (a) floor of 2; (b) capped at max; (c) unknown estimate → deterministic worker-count-only fallback; (d) monotonic non-decreasing in worker_count; (e) monotonic non-decreasing in estimated_rows up to the cap
- [x] 3.2 Implement `choose_partition_count` to make 3.1 green — `clamp(max(worker_count, ceil(rows / target)), 2, max)`, pure and deterministic
- [x] 3.3 Confirm determinism: same inputs → same N (no Date/random); the hash assignment seed is unchanged (W3-Hash deterministic seed)

## 4. Wire into the fragmenter

- [x] 4.1 Thread worker_count + the child cardinality estimate (from 1.2) + resolved knobs into the fragmenter's hash-exchange insertion
- [x] 4.2 Replace the hard-coded `let partition_count = 2usize;` (`fragment.rs:615`) with `choose_partition_count(...)`; ensure all other `partition_count` consumers in `fragment.rs` (e.g. :667/:687) use the same value
- [x] 4.3 Verify the M×N coordinator scheduling (`coordinator.rs:312-325`) and partitioned-probe path consume the new N unchanged (no scheduling-shape change)

## 5. Correctness gate (non-negotiable)

- [x] 5.1 Single-node: 22/22 unit + doc tests green; `cargo clippy --all-targets -- -D warnings` clean (cargo at `~/.rustup/toolchains/stable-aarch64-apple-darwin/bin/cargo`, sandbox off)
- [ ] 5.2 `trino-diff` SF1 full suite cell-parity (1e-9) — no correctness regression at N > 2
  - **BLOCKED (2026-06-05): the M×N partitioned-probe exchange is correctness-broken at N > 2.** Local SF1, forced `ARNEB_HASH_PARTITION_TARGET_ROWS=500000` → q09 fans out to N=13 (confirmed: `estimated_rows=Some(6001215)`, `partition_count=13`, max submitted partition=12). Result UNDERCOUNTS: arneb `ALGERIA=45,755,156` vs Trino `308,811,555` (~0.148× ≈ 2/13.5, rows dropped). Forcing N=2 (high target) → arneb matches Trino exactly. So the adaptive RULE is correct; it merely EXPOSES a latent N>2 row-drop in arneb's existing exchange (only ever run at the hard-coded N=2). This change CANNOT ship — even with the default 4M target, SF30 (~90M-row intermediate) would fan out to ~23 and break q09. Needs a prerequisite change `dist-exchange-nway-correctness` (fix the M×N hash routing / consumer_partition_id so N>2 colocates correctly), THEN resume here.
  - **Root cause REFINED (2026-06-05):** SINGLE 2-way join `lineitem⋈orders COUNT(*)` at N=13 = `6,001,215` = Trino (single joins fine at N>2). Only NESTED multi-way joins break. The intermediate join fragment's `output_partitioning` carries EMPTY hash columns (`fragment.rs:726`), so the coordinator schedules it "α consumer only" `(N,1)` (`coordinator.rs:338`) and never re-partitions the join's output onto the NEXT join's keys — the M×N `(N,N)` producer path (`coordinator.rs:332`) is inert for join fragments. **This is the A.4-reverted work** ([[project_2026-05-20_a4_revert_root_cause]]). Prerequisite change must re-partition intermediate join output onto next-level keys (activate M×N for join fragments, done correctly per the A.4 gate), THEN resume here.
- [ ] 5.3 SF30 on a constrained remote bench host (access details in memory, not in-repo): q09 + q18 cell-by-cell vs Trino at the new adaptive N (no row loss / duplication / value drift) — this is the spec's N-way-fan-out-preserves-results gate

## 6. Measure, tune default, ship

- [ ] 6.1 SF30 sweep on the remote host: per-partition peak RSS + latency for a few `target_rows_per_partition` values (e.g. 1M / 4M / 16M); confirm per-partition pressure drops on q09/q18 vs the 1.3 baseline
- [ ] 6.2 Pick the in-source default from 6.1 (best memory/latency trade-off that does not regress the SF10 winning set beyond a documented bound); record the chosen value + the sweep table
- [ ] 6.3 `cargo fmt`; commit in the Taiwan time window with `GIT_AUTHOR_DATE` + `GIT_COMMITTER_DATE` set (only the touched files); message describes the adaptive rule + the SF30 before/after
- [ ] 6.4 Update memory + roadmap note: adaptive-N result (q09/q18 SF30 before/after), chosen default, any residual skew observed (feeds a later change)
