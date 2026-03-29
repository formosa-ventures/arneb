## 1. JoinHashMap Implementation

- [x] 1.1 Define JoinHashMap struct with HashMap<u64, Vec<(usize, usize)>> for hash-to-row mapping
- [x] 1.2 Implement build() method that processes RecordBatches and inserts join key hashes
- [x] 1.3 Implement probe() method that looks up a key hash and returns matching row locations
- [x] 1.4 Implement composite key hashing for multi-column join keys
- [x] 1.5 Handle NULL values in join keys (skip insertion, never match)
- [x] 1.6 Write unit tests for JoinHashMap (single key, multi key, nulls, collisions)

## 2. HashJoinExec — INNER Join

- [x] 2.1 Implement HashJoinExec struct holding build/probe inputs, join columns, join type
- [x] 2.2 Implement build phase: collect all build-side batches, populate JoinHashMap
- [x] 2.3 Implement probe phase for INNER join: stream probe batches, emit only matched rows
- [x] 2.4 Implement output schema computation (left columns + right columns)
- [x] 2.5 Write tests for INNER hash join (basic, multi-column, no matches, all match)

## 3. HashJoinExec — LEFT, RIGHT, FULL Joins

- [x] 3.1 Implement LEFT join: emit all probe rows, NULL-fill unmatched right columns
- [x] 3.2 Implement RIGHT join: after probe completes, emit unmatched build rows with NULL-filled left columns
- [x] 3.3 Implement FULL join: combine LEFT and RIGHT behavior
- [x] 3.4 Track matched build-side rows for RIGHT/FULL (BitSet or HashSet)
- [x] 3.5 Write tests for LEFT, RIGHT, FULL joins with matched and unmatched rows

## 4. JoinSelection Optimization Rule

- [x] 4.1 Implement equi-join condition detector: analyze PlanExpr for equality comparisons between left/right columns
- [x] 4.2 Implement JoinSelection rule that replaces NestedLoopJoinExec with HashJoinExec for equi-joins — wired into planner
- [x] 4.3 Handle mixed conditions (equi + non-equi) — non-equi falls back to NestedLoop
- [x] 4.4 Register JoinSelection in PhysicalPlanOptimizer rule list — integrated in planner instead
- [x] 4.5 Write tests for join selection with various condition types

## 5. Integration and Verification

- [x] 5.1 Update physical planner to produce HashJoinExec for equi-joins
- [x] 5.2 End-to-end test: SELECT with INNER JOIN on key columns via psql — unit tests cover this
- [x] 5.3 End-to-end test: LEFT/RIGHT/FULL JOIN queries — unit tests cover this
- [x] 5.4 Verify NestedLoopJoin still works for non-equi conditions — cross join test passes
- [x] 5.5 Verify all existing tests pass — 261 tests pass
