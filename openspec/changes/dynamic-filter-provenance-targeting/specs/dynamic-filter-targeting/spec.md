## ADDED Requirements

### Requirement: Dynamic filters target only join-equal probe columns

A hash join's build-side dynamic filter (`build_key IN (distinct build values)`) SHALL be applied
only to probe-side columns that are join-equal to the probe-side join key — i.e. members of that
key's equivalence class within the probe subtree. It SHALL NOT be applied to a column merely because
it shares a name with the join key.

#### Scenario: Self-join twin is not pruned
- **WHEN** the probe subtree of a dynamic-filter-producing join contains a same-named column from a
  different table instance (a self-join twin) that is NOT join-equal to the build key
- **THEN** the dynamic filter is NOT applied to that twin column (only the genuinely join-equal
  column(s) are pruned)

#### Scenario: Transitively-equal cross-table column is still pruned
- **WHEN** a probe-side column from a different table is transitively join-equal to the build key
  (via equi-joins in the probe subtree)
- **THEN** the dynamic filter IS applied to that column (the cross-table pruning optimization is
  preserved)

### Requirement: Dynamic-filter routing is by column index, not name

The dynamic filter SHALL be routed to its target scan by descending the probe subtree and remapping
the target column index through each operator, applying at the owning scan by index. Name matching
SHALL NOT decide which scan receives the filter.

#### Scenario: Filter reaches exactly the owning scan
- **WHEN** a dynamic filter targets a probe-side column at a known index
- **THEN** it is applied at exactly that column's owning scan, regardless of whether other scans in
  the subtree expose columns of the same name

### Requirement: Self-join queries are cell-correct with dynamic filters enabled

Enabling build-side self-join reorder together with dynamic filters SHALL produce results identical
(within float tolerance) to the reference engine.

#### Scenario: q08-class self-join is cell-correct
- **WHEN** a self-join query is reordered so both self-join leaves sit on the probe spine and a
  dynamic filter is produced from a build key whose name collides with a non-equal twin
- **THEN** the query result is cell-for-cell correct (no value corruption from a misrouted filter)

### Requirement: Cross-table dynamic-filter pruning is preserved

The provenance-targeted injection SHALL preserve the existing cross-table ("sibling") pruning that
collapses large probe-side builds.

#### Scenario: q18-class build stays collapsed
- **WHEN** a query relies on injecting a build key's distinct values onto a transitively-equal
  cross-table probe column (q18: the main lineitem scan pruned by a deduped-orderkey build)
- **THEN** that probe-side build remains collapsed (does not regress to the full unpruned
  cardinality)
