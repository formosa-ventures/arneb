## Context

The execution engine currently has only NestedLoopJoinExec, which evaluates the join condition for every pair of rows (O(n*m)). For equi-joins (WHERE a.id = b.id), hash join provides O(n+m) by hashing the build side and probing with the other.

## Goals / Non-Goals

**Goals:**
- Implement HashJoinExec for equi-join conditions
- Support all standard join types: INNER, LEFT, RIGHT, FULL
- Handle multi-column join keys
- Follow SQL null semantics (nulls never match)
- Automatically select hash join when applicable

**Non-Goals:**
- Sort-merge join — deferred to later
- Semi-join / anti-join optimizations
- Spill-to-disk for oversized hash tables — in-memory only for now
- Build-side selection (always uses right side as build)

## Decisions

1. **Build side is always the right input**: Simplifies implementation. A future optimization can swap inputs based on estimated cardinality.

2. **JoinHashMap structure**: `HashMap<u64, Vec<(usize, usize)>>` where key is hash of join columns, value is list of (batch_index, row_index) pairs. Composite keys are hashed together.

3. **Null handling**: Rows with NULL in any join key column are never matched (SQL standard). For LEFT/RIGHT/FULL joins, unmatched rows are emitted with NULL-filled columns from the other side.

4. **Async streaming**: Build phase collects all batches from build side (pipeline breaker). Probe phase streams through probe side, emitting matched/unmatched rows per batch.

5. **Equi-join detection**: JoinSelection rule inspects join conditions. If all conditions are equality comparisons between left and right columns, use HashJoin. Otherwise, fall back to NestedLoop.

## Risks / Trade-offs

- **Memory pressure**: Build side is fully materialized in hash table. Large build sides can OOM. Spill-to-disk is a Phase 2 enhancement.
- **Hash collisions**: Using u64 hash with collision chains (Vec). In practice, hash collisions are rare for join keys.
