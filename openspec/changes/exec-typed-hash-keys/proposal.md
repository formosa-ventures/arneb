## Why

`HashAggregateExec` builds its group hash key with `format!()`:

```rust
// crates/execution/src/operator.rs ~ line 965-1014
fn group_key(cols: &[ArrayRef], row: usize) -> String {
    let mut parts = Vec::with_capacity(cols.len());
    for col in cols {
        parts.push(extract_scalar(col, row)?.to_string());
    }
    parts.join("|")
}
```

And `DistinctAccumulator` does the same pattern at row-level for
dedup:

```rust
// crates/execution/src/aggregate.rs ~ line 594-653
fn dedup_key(arr: &ArrayRef, index: usize) -> Result<String, ExecutionError> {
    match arr.data_type() {
        Int32 => Ok(format!("i32:{}", ...)),
        ...
    }
}
```

Every row that flows through GROUP BY or COUNT(DISTINCT) allocates a
`String` and walks the type-tagged formatter. On TPC-H Q14 (200K
groups × `SUM(CASE WHEN ... THEN ... ELSE 0 END)`) and Q16 (~100K
groups × COUNT(DISTINCT)), this string allocation dominates the
aggregate path. The change also affects every aggregate query with
two or more group-by columns (Q01 / Q02 / Q03 / Q10 / Q12 / Q13).

The `exec-ahash-hashtables` change replaced SipHash with AHash —
that helped the *hash function*, but didn't touch the *key
construction*. This change attacks the key construction: typed
`Vec<ScalarValue>` instead of `String`, with bit-pattern float
equality (so NaN deduplicates to itself, matching the existing
string-key behavior).

## What Changes

- Introduce `crates/execution/src/group_key.rs` with a small wrapper
  type `GroupKey(Vec<ScalarValue>)`. Hash + Eq are implemented
  manually so that:
  - `Null`, `Boolean`, integer, `Utf8`, `Binary`, `Date32`,
    `Decimal128`, `Timestamp` use their natural bit-equal hashing.
  - `Float32(f32)` hashes / compares via `f32::to_bits() as u32`.
    `Float64(f64)` via `f64::to_bits() as u64`. NaN is therefore
    deduplicated to itself (matching the existing `"f64:NaN"`
    string-key behavior and SQL DISTINCT semantics).
- `HashAggregateExec` `FastHashMap<String, GroupState>` →
  `FastHashMap<GroupKey, GroupState>`. The `group_key()` helper is
  rewritten to return `GroupKey` (still row-wise extraction; batch-wise
  vectorization is deferred). The output schema build path that
  previously parsed the joined String back into columns is replaced
  by reading scalars directly from the stored `GroupKey`.
- `DistinctAccumulator` `FastHashSet<String>` → `FastHashSet<GroupKey>`
  (single-element `GroupKey` per scalar). The whole `dedup_key()`
  function is deleted. `update_batch` walks the array, extracts one
  `ScalarValue` per row via the existing `array_value_to_scalar`
  helper (or a tightly-scoped per-type extractor), wraps in
  `GroupKey`, and inserts.
- No public API change. `GroupKey` is `pub(crate)` to `crates/execution`.

## Capabilities

### New Capabilities

- None. Internal performance refactor.

### Modified Capabilities

- `accumulators`: the existing requirement that mentions
  "type-prefixed key" for `DistinctAccumulator` is reworded to specify
  the typed `GroupKey` representation. Observable behavior — NaN
  collapses, NULL skipped — is unchanged; the implementation
  description is the change.

## Impact

- **Behavior**: identical output for every TPC-H query and every
  unit test. NaN-in-DISTINCT semantics are explicitly preserved
  (bit-pattern equality is the same rule today's
  `format!("f64:{}", value.to_bits())` implements).
- **API**: no public change.
- **Tests**: existing PB-003 regression tests must remain green
  unmodified. A new unit test pins the NaN/-NaN behavior at the
  `GroupKey` level.
- **Benchmark gate**: TPC-H SF1 (8 runs / 2 warmup) against the
  baseline from `exec-ahash-hashtables` end state
  (`after-ahash-arneb.json`):
  - Required: geomean speedup ≥ 1.20× (over the AHash-end baseline,
    not the original pre-perf baseline).
  - No query slower than 0.95× of the AHash-end baseline.
  - Aggregate-heavy queries (Q01, Q14, Q16) expected ≥ 1.30×.
- **Memory**: `GroupKey` holds owned `Vec<ScalarValue>` (so owned
  Strings for Utf8). Per-group memory is ~1.5× the string-key
  approach for short strings but lower per-row allocation churn
  (one Vec alloc per group vs. one String alloc per row). For SF1
  this is a clear net win; for very high-cardinality groups
  (millions) this should still come out ahead but is worth a future
  micro-bench.
- **Out of scope**:
  - Batch-wise (vectorized) key extraction. Today's row-wise extract
    is kept; vectorizing is a separate, larger change that requires
    rewriting the column projection through the aggregator.
  - Replacing `ScalarValue` with a more compact representation
    (interned strings, dictionary-encoded categoricals). Deferred.
