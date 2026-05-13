## Context

Profiling and code review during the `exec-ahash-hashtables` change
showed that the AHash swap delivered measurable wins on multi-join
queries (Q03, Q05, Q07: 19–48% faster) but only ~3–7% on
aggregate-heavy queries (Q01, Q02, Q10, Q12, Q13). The reason is
clear from `operator.rs:545-1014`: `HashAggregateExec` constructs
its group hash key by calling `format!()` on every row's scalars and
joining with `'|'`:

```rust
let key = parts.join("|");   // one String alloc per row
let entry = groups.entry(key).or_insert_with(...);
```

AHash makes the *lookup* faster, but the *key construction* still
costs an allocation and a string-conversion per row. For Q01
(GROUP BY 2 cols × 6M lineitem rows), that's 12M string concatenations.
`DistinctAccumulator` has the same pattern at row-level:
`format!("i32:{}", value)` for every input row.

This change replaces the String key with a typed
`GroupKey(Vec<ScalarValue>)` wrapper. Hash + Eq are implemented to
treat floats by their raw bit pattern, preserving the existing
NaN-deduplicates-to-itself semantic from the
`format!("f64:{}", value.to_bits())` approach.

## Goals / Non-Goals

**Goals:**
- Cut the per-row string-allocation cost in `HashAggregateExec` and
  `DistinctAccumulator` while preserving every observable behavior
  (including NaN-in-DISTINCT collapsing).
- Keep the change focused — touch only the two hot paths, do not
  vectorize key extraction (that's a follow-up).
- Reach geomean ≥ 1.20× speedup vs the end-state of
  `exec-ahash-hashtables` on SF1.

**Non-Goals:**
- Vectorized (batch-wise) key extraction. Today's path extracts one
  scalar per row inside a Rust loop; vectorizing requires reorganizing
  the entire aggregation operator and is a much larger change.
- Replacing `ScalarValue` with a more compact / interned
  representation. Future work.
- Changing GROUP BY semantics (NULL grouping, type coercion). Pure
  internal rewrite.

## Decisions

### Decision 1: Wrap `Vec<ScalarValue>`, don't add Hash + Eq to `ScalarValue` directly

`ScalarValue` derives `PartialEq` today. Adding `Hash` and `Eq`
would break the existing IEEE 754 semantics: `Float64(NaN) == Float64(NaN)` would suddenly return `true` in callers that
expected SQL's standard `NaN != NaN` rule. While we *want* that
collapse-rule inside DISTINCT and GROUP BY, the rest of the codebase
(scalar comparisons, optimizer constant folding) must not change.

The wrapper approach localizes the rule to exactly two call sites:

```rust
pub(crate) struct GroupKey(pub(crate) Vec<ScalarValue>);

impl Hash for GroupKey { /* manual, with to_bits for floats */ }
impl PartialEq for GroupKey { /* manual, with to_bits for floats */ }
impl Eq for GroupKey {}
```

### Decision 2: Float hash via `to_bits()`

For `Float32`: `state.write_u8(tag_f32); state.write_u32(f.to_bits())`.
For `Float64`: `state.write_u8(tag_f64); state.write_u64(f.to_bits())`.

Tags ensure `Float32(1.0)` and `Float64(1.0)` hash to different
slots. The same tag bytes prefix every variant so that
`Int32(1)`, `Int64(1)`, `Float32(1.0)`, `Float64(1.0)`, `Utf8("1")` are
all distinguishable.

NaN handling: `f64::to_bits()` is bit-preserving — every distinct
NaN payload hashes differently, matching the existing string-key
behavior (`format!("f64:{}", value.to_bits())`). For SQL DISTINCT this
is the correct semantics: NaN deduplicates with itself only if it's
the *same NaN payload*. In practice TPC-H produces no NaNs (no
`0.0/0.0` or `sqrt(-1)` paths), so this is purely a correctness
contract, not a hot-path concern.

### Decision 3: One `GroupKey` shape for both call sites

`DistinctAccumulator` always has a *single* scalar per row;
`HashAggregateExec` may have many. Using the same `GroupKey` wrapper
for both keeps the API surface tiny — a one-element Vec is a small
heap allocation but matches the multi-column path's cost model.

If single-column DISTINCT becomes a benchmark hot spot, a future
change can specialize `DistinctAccumulator` with a typed
`FastHashSet<ScalarValueOrd>` and skip the Vec wrapper.

### Decision 4: Output-schema reconstruction reads directly from `GroupKey`

Today's `build_aggregate_output()` splits the joined String key by
`'|'` and re-parses each token into a `ScalarValue` to populate the
group-by columns. After this change, those values are already
`ScalarValue` inside the `GroupKey` — no parsing needed. This
removes a fragile string-split step and gives back any cycles spent
on string-→-scalar reconstruction.

## Test Strategy

- **Reuse existing tests.** PB-003 regression tests in
  `aggregate.rs` and `operator.rs` (3 tests) plus the workspace
  HashAggregate suite must remain unmodified and green.
- **One new test** for NaN behavior:
  - Build two `GroupKey([Float64(NaN)])` from the same NaN bit pattern
    and verify they hash and compare equal.
  - Build two `GroupKey([Float64(NaN)])` from *different* NaN
    payloads (e.g., construct NaN from `f64::from_bits()`) and verify
    they remain distinct. This pins the contract in the spec.
- **`trino-diff` skill** at 1e-9: 16/16 unchanged.
- **TPC-H SF1 benchmark** (8 runs / 2 warmup) compared to
  `after-ahash-arneb.json`. Required: geomean ≥ 1.20×, no query
  worse than 0.95×.

## Risks

- **NaN payload variance.** If two NaN values arrive with different
  bit patterns (only possible from `unsafe` casts or
  `from_bits()`-constructed NaNs — never from normal arithmetic),
  they will *not* collapse under DISTINCT. The existing
  string-key code has the same behavior because `to_bits()` was the
  basis there too. Logged as expected behavior in spec.
- **Per-group memory.** A `Vec<ScalarValue>` plus its `String`
  member for Utf8 is heavier than the equivalent `"a|b"` string.
  For SF1 typical cardinalities (≤ 200K groups), this adds at most
  a few MB. Worth a callout in `IMPACT` but not a blocker.
- **Output-schema reconstruction.** The current code that splits the
  String key by `'|'` to materialize columns has to be replaced. If
  the new path mishandles a type (e.g., gets the Arrow type wrong
  on output array construction), tests will catch it — but the
  fixup work is the biggest mechanical risk in this PR.

## Rollback

Revert one file (`fast_hash.rs` is unchanged; `group_key.rs` is the
new module) and undo the HashAggregateExec / DistinctAccumulator
edits. No data migration; no persistent state involved.
