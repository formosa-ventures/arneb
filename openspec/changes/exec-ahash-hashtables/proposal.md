## Why

Every hash table in `crates/execution/` — `JoinHashMap`, `HashAggregateExec`'s
group map, `DistinctAccumulator`'s dedup set, semi-join's
membership set, set-operation dedup sets, and the window-function
partition hasher — uses `std::collections::hash_map::DefaultHasher`,
which is SipHash. SipHash is designed to resist hash-flooding DoS
attacks from adversarial input, paying a 3–5× per-byte cost over
non-adversarial hashers like AHash.

Arneb runs against trusted, internal SQL — the query engine is
authoritative over its own join keys. Spending CPU cycles on
DoS-resistance is pure overhead. Recent TPC-H SF1 benchmark
(`benchmarks/tpch/results/baseline-pre-perf-wins.md`) shows Arneb at
0.14× geometric mean speed vs Trino on 16 queries, with `hash_join`
and `HashAggregateExec` dominating CPU profile on multi-join queries
(Q05/Q07/Q08/Q09). Even a conservative 1.10–1.30× hash-path speedup
moves the needle on every benchmark query, with zero risk to
correctness.

This is the first of three "quick win" performance changes; the other
two (`exec-typed-hash-keys`, `hive-parallel-file-scan`) target adjacent
overheads.

## What Changes

- Add `ahash = "0.8"` to workspace `[workspace.dependencies]` and pull
  it into `crates/execution/Cargo.toml`.
- Introduce a small module `crates/execution/src/fast_hash.rs` exposing
  three internal aliases:
  - `pub(crate) type FastHasher = ahash::AHasher;`
  - `pub(crate) type FastHashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;`
  - `pub(crate) type FastHashSet<K> = std::collections::HashSet<K, ahash::RandomState>;`

  Keeping the std API shape (HashMap/HashSet) means call sites change
  only the type, not the methods.
- Swap every hot-path call site:
  - `hash_join.rs`: `JoinHashMap::map` → `FastHashMap<u64, ...>`; the
    `hash_row` helper's `DefaultHasher::new()` → `FastHasher::default()`.
  - `aggregate.rs`: `DistinctAccumulator::seen` → `FastHashSet<String>`
    (the String key itself is replaced by a typed key in change
    `exec-typed-hash-keys` — separated to keep this one minimal).
  - `operator.rs`: `HashAggregateExec` group `HashMap` → `FastHashMap`.
  - `semi_join.rs`: `DefaultHasher::new()` → `FastHasher::default()`;
    `HashSet<u64>` → `FastHashSet<u64>`.
  - `set_ops.rs`: same as semi-join — every `HashSet<u64>` and
    `DefaultHasher::new()` swapped.
  - `window.rs`: `DefaultHasher::new()` → `FastHasher::default()`.
- Skip non-hot-path sites: `planner.rs` (init-time data-source
  registry) and `hash_join.rs:1088` (test fixture) keep std HashMap
  for symmetry with non-execution callers and to avoid noise.

## Capabilities

### New Capabilities

- None. This is a pure internal performance optimization with no
  observable behavior change.

### Modified Capabilities

- None. Spec contracts remain identical; only implementation hashes
  change.

## Impact

- **Behavior**: Identical. Every aggregate, join, distinct, semi-join,
  set-op, and window-function result is bit-for-bit unchanged.
- **API surface**: No public API change. The `fast_hash` module is
  `pub(crate)`-only.
- **Build**: One new dependency (`ahash`), Apache-2.0 / MIT licensed
  (verify under `cargo deny check`). No transitive risk.
- **Tests**: Existing `cargo test --workspace` must remain green. No
  new tests needed — correctness is already covered by the seven
  PB-001/PB-002/PB-003 regression tests plus the workspace suite.
- **Benchmark gate**: TPC-H SF1 with 8 runs / 2 warmup must show
  ≥ 1.10× geometric mean speedup vs the `baseline-pre-perf-wins-arneb.json`
  results captured at the start of the performance work. No query
  may regress beyond 0.95× of baseline.
- **Security**: Hash-flooding DoS attacks become theoretically possible
  on the join key space, but Arneb only accepts authoritative
  internal SQL — no untrusted input flows into hash keys. If a future
  change exposes a public endpoint, this trade-off needs reevaluation
  (noted in `design.md`).
- **Out of scope**: Changing the hash-table data structure
  (e.g., hashbrown's `RawTable`, or replacing `HashMap<u64, Vec<...>>`
  with a flat array). Replacing the string-keyed group map with a
  typed key is a separate change (`exec-typed-hash-keys`).
