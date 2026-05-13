# Tasks — exec-ahash-hashtables

## 1. Workspace plumbing

- [x] 1.1 Add `ahash = "0.8"` to `[workspace.dependencies]` in root `Cargo.toml`.
- [x] 1.2 Add `ahash.workspace = true` to `crates/execution/Cargo.toml` `[dependencies]`.
- [x] 1.3 `cargo build -p arneb-execution` — confirm `ahash` resolves and builds.

## 2. Internal helper module

- [x] 2.1 Create `crates/execution/src/fast_hash.rs` exporting three `pub(crate)` aliases:
  - `pub(crate) type FastHasher = ahash::AHasher;`
  - `pub(crate) type FastHashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;`
  - `pub(crate) type FastHashSet<K> = std::collections::HashSet<K, ahash::RandomState>;`
- [x] 2.2 Add `mod fast_hash;` to `crates/execution/src/lib.rs`.
- [x] 2.3 Doc comment on the module explaining: AHash is used because Arneb only accepts authoritative internal SQL; SipHash's DoS-resistance is not required and costs 3–5×. Cite this change in the comment.

## 3. Swap call sites

- [x] 3.1 `crates/execution/src/hash_join.rs`: import `FastHashMap`, change `JoinHashMap.map: HashMap<u64, _>` → `FastHashMap<u64, _>`. In `JoinHashMap::build`, change `let mut map: HashMap<...> = HashMap::new()` → `FastHashMap::default()`.
- [x] 3.2 `crates/execution/src/hash_join.rs::hash_row`: replace `std::collections::hash_map::DefaultHasher::new()` with `FastHasher::default()`. Drop the `std::collections::hash_map::DefaultHasher` use line.
- [x] 3.3 `crates/execution/src/operator.rs`: `HashAggregateExec` groups `HashMap<String, GroupState>` → `FastHashMap<String, GroupState>` at both declaration sites (line ~545 and ~635). Use `FastHashMap::default()` instead of `HashMap::new()`.
- [x] 3.4 `crates/execution/src/aggregate.rs::DistinctAccumulator.seen`: `HashSet<String>` → `FastHashSet<String>`. Constructor uses `FastHashSet::default()`. (Note: the String key itself stays for this change; typed-key migration is `exec-typed-hash-keys`.)
- [x] 3.5 `crates/execution/src/semi_join.rs`: replace `DefaultHasher::new()` with `FastHasher::default()`; `HashSet<u64>` → `FastHashSet<u64>`. Drop `use std::collections::hash_map::DefaultHasher`.
- [x] 3.6 `crates/execution/src/set_ops.rs`: replace all six `HashSet<u64>` with `FastHashSet<u64>` and all `DefaultHasher::new()` with `FastHasher::default()`.
- [x] 3.7 `crates/execution/src/window.rs`: replace `DefaultHasher::new()` with `FastHasher::default()`.
- [x] 3.8 Leave `planner.rs::ExecutionContext.data_sources` HashMap alone (init-time only); leave `hash_join.rs:1088` test fixture HashMap alone; leave `distributed.rs::compute_partition_hash` alone (defer).

## 4. Verification

- [x] 4.1 `cargo fmt -- --check` clean.
- [x] 4.2 `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [x] 4.3 `cargo nextest run --workspace` — 100% pass, including hash_join, semi_join, set_ops, window, aggregate, operator tests.
- [x] 4.4 `cargo deny check` — no advisory or license issues from `ahash`.
- [x] 4.5 `trino-diff` skill at tol=1e-9: 16/16 queries values-identical (no correctness regression). **Confirmed 2026-05-12.**
- [x] 4.6 TPC-H SF1 benchmark (8 runs / 2 warmup) — see `benchmarks/tpch/results/after-ahash.md`:
  - Geomean speedup vs baseline: **1.076×** (target 1.10× missed but within tolerance — see Observations note).
  - Median speedup: **1.039×**.
  - Regressions (<0.95×): **0** (none).
  - Wins (≥1.10×): **3** — Q03 (1.294×), Q05 (1.480×), Q07 (1.188×). The three biggest multi-join queries — exactly the queries we expected to win.
  - Result saved to `benchmarks/tpch/results/after-ahash-arneb.json` and `after-ahash.md`.

## 5. PR

- [ ] 5.1 commit via `commit` skill, suggested title: `perf(execution): swap SipHash for AHash on hot-path hash tables`.
- [ ] 5.2 PR via `pr` skill. Body includes:
  - 1-line summary
  - Hot-path file list
  - Before / after table (geomean + per-query) sourced from `baseline-pre-perf-wins.md` → `after-ahash.md`
  - Trino-diff still 16/16 confirmation
- [ ] 5.3 After merge, run `openspec-archive-change` to move into `openspec/changes/archive/<date>-exec-ahash-hashtables/`.
