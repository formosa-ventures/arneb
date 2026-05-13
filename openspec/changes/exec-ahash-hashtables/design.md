## Context

`crates/execution/` contains seven hot-path uses of
`std::collections::hash_map::DefaultHasher` (SipHash). The CPU profile
on the TPC-H SF1 baseline shows hash computation as a measurable
fraction of total time on every multi-join and aggregate query.
SipHash gives Arneb nothing useful: the engine controls its own input
SQL, so adversarial hash-flooding is a non-threat.

`ahash` is the de-facto replacement chosen by DataFusion, Polars,
hashbrown, rustc itself (internally), and most performance-sensitive
Rust projects. It produces 64-bit hashes 3–5× faster than SipHash on
modern CPUs (AES-NI on x86_64, NEON on ARM64), with a quality
distribution that exceeds DoS-resistance requirements for trusted
input.

## Goals / Non-Goals

**Goals:**
- Swap SipHash for AHash in every `crates/execution/` hot-path hash
  table and per-row hasher, with no behavior change.
- Keep the change small and reviewable in one PR. Minimum diff, maximum
  test coverage reuse.
- Establish `FastHasher` / `FastHashMap` / `FastHashSet` as the
  workspace-wide idiom for performance-sensitive code. Future
  `crates/execution/` additions reuse them rather than reaching back to
  std.

**Non-Goals:**
- Replacing the hash *data structure* (e.g., hashbrown's `RawTable`,
  flat-array hash join). That's a deeper change with API impact.
- Swapping the *key type* of `HashAggregateExec` from `String` to a
  typed key. Tracked separately as `exec-typed-hash-keys` so this
  change stays small and bisectable.
- Using AHash across the entire workspace. `planner.rs`, `protocol/`,
  `server/`, etc. don't have hot-path hashing — leave them as std
  HashMap to avoid touch noise.

## Decisions

### Decision 1: AHash over FxHash, hashbrown, or fnv

| Crate | Quality | Speed | Dependencies | Picked? |
|-------|---------|-------|--------------|---------|
| `ahash` | Good (AES-NI) | Fastest on x86_64/ARM64 | None besides std | **Yes** |
| `rustc-hash` (FxHash) | Adequate | Fast on small keys, weaker distribution on large | Tiny | No |
| `fnv` | Adequate | Slow on multi-byte keys | None | No |
| `hashbrown` w/ AHasher | Same as ahash | Same | Larger | No (overkill) |

AHash wins: best distribution, accelerated on every target we care
about, zero transitive dependency surface. It's also the de-facto
Rust ecosystem default for non-adversarial hashing.

### Decision 2: Type aliases, not wrapper types

`FastHashMap<K, V>` is a `type` alias for `std::collections::HashMap<K, V, ahash::RandomState>`, not a wrapper struct. Reasons:
- Zero runtime cost beyond AHash itself.
- Methods (`insert`, `get`, `entry`, etc.) are identical — call sites change one line (the type), not every method call.
- Compatible with std iterators, serde, etc. No re-export work.

`ahash::RandomState` is randomized per-process (seeded from
`std::collections::hash_map::RandomState`), so two runs hash the same
key differently. That's fine because Arneb never persists hash values
— they're all in-process state.

### Decision 3: Hot-path scope

Sites changed:
- `hash_join.rs` — JoinHashMap + per-row hasher
- `aggregate.rs` — DistinctAccumulator's seen set
- `operator.rs` — HashAggregateExec's groups map
- `semi_join.rs` — membership HashSet + per-row hasher
- `set_ops.rs` — UNION/INTERSECT/EXCEPT dedup HashSets + per-row hasher (six sites)
- `window.rs` — partition key hasher

Sites left as std HashMap:
- `planner.rs::ExecutionContext.data_sources` — initialized once at engine startup, never on the hot path.
- `hash_join.rs:1088` (test) — fixture inside `#[cfg(test)]`, no perf cost.
- `distributed.rs::compute_partition_hash` — distributed-mode shuffle hash. Hot in distributed mode but not in the single-node benchmark; defer to a future change so the diff stays focused.

## Test Strategy

- **No new tests.** Correctness is already covered by:
  - `cargo test -p arneb-execution` — hash_join, hash_aggregate, semi_join, set_ops, window suites
  - PB-001/2/3 regression tests for the planner and accumulator paths
  - workspace `cargo nextest run --workspace`
- **Benchmark gate.** Run TPC-H SF1 (8 runs / 2 warmup × 16 queries × 2 engines) before and after. Compare to `benchmarks/tpch/results/baseline-pre-perf-wins-arneb.json`. Required: ≥ 1.10× geomean speedup; no query worse than 0.95× of baseline.
- **Correctness gate.** Run `trino-diff` skill at relative tolerance 1e-9. Required: 16/16 queries values-identical (unchanged from baseline).

## Risks

- **Hash randomization across runs.** `ahash::RandomState` seeds from `std::collections::hash_map::RandomState`, so two processes hash the same key to different `u64` values. This already happens with `DefaultHasher` — no behavior regression — but worth re-checking tests that depend on hash ordering (none currently).
- **Cargo-deny license / advisory.** `ahash` is MIT-licensed; verify under `cargo deny check`. No known advisories at 0.8.x.
- **DoS surface.** If Arneb later exposes a public, untrusted SQL endpoint, hash-flooding on join keys becomes a theoretical attack vector. Document in proposal Impact section; treat as future-work review item.
- **Benchmark noise.** SF1 single-table queries (Q04, Q06) may not show meaningful speedup because hash isn't dominant there. The 1.10× gate is on geomean, not per-query — the per-query "no regression beyond 0.95×" guard catches noise.

## Rollback

`ahash` swap is one-line-per-site. To revert, change `FastHashMap` →
`HashMap`, `FastHashSet` → `HashSet`, `FastHasher::default()` →
`DefaultHasher::new()`, drop the workspace dep, delete `fast_hash.rs`.
No data migration, no state file. Reverting is a single PR.
