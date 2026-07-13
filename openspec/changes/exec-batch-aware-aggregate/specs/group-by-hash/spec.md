## ADDED Requirements

### Requirement: GroupByHash batch interface

The system SHALL provide `GroupByHash` in
`crates/execution/src/group_by_hash.rs` with:

```rust
pub struct GroupByHash {
    table: FastHashMap<GroupKey, u32>,
    keys: Vec<GroupKey>,
}

impl GroupByHash {
    pub fn new() -> Self;
    pub fn get_group_ids(&mut self, group_cols: &[ArrayRef]) -> Result<Vec<u32>, ExecutionError>;
    pub fn num_groups(&self) -> usize;
    pub fn keys(&self) -> &[GroupKey];
}
```

`get_group_ids` SHALL assign a stable `u32` ID to each row in the
batch, inserting a new ID on first sight of a `GroupKey`. The
returned `Vec<u32>` length SHALL equal the row count of the input
batch (read from `group_cols[0].len()`; all `group_cols` must share
a row count).

#### Scenario: identical group key returns the same id across calls

- **GIVEN** a fresh `GroupByHash`
- **WHEN** `get_group_ids(&[col_with_a_a_b])` is called returning `[0, 0, 1]`, then `get_group_ids(&[col_with_b_a])` is called
- **THEN** the second call returns `[1, 0]` (existing ids reused)

#### Scenario: NULL is a distinct group

- **GIVEN** a fresh `GroupByHash` and input column `[1, NULL, 1, NULL]`
- **WHEN** `get_group_ids` is called
- **THEN** the result is `[0, 1, 0, 1]` (NULL gets its own id, never colliding with a non-null value)

#### Scenario: num_groups reflects unique ids

- **GIVEN** the above NULL scenario
- **WHEN** `num_groups()` is called
- **THEN** it returns `2`

#### Scenario: keys() preserves insertion order

- **GIVEN** input column `[b, a, b]`
- **WHEN** `get_group_ids` is called
- **THEN** `keys()[0] == GroupKey::single(ScalarValue::Utf8("b".into()))` and `keys()[1] == GroupKey::single(ScalarValue::Utf8("a".into()))`

### Requirement: multi-column group keys

`get_group_ids` SHALL accept arbitrarily many group columns; the
group key for row `i` is the tuple `(col0[i], col1[i], ...)`.

#### Scenario: two-column group key

- **GIVEN** input cols `[a, a, b]` and `[1, 2, 1]`
- **WHEN** `get_group_ids` is called
- **THEN** the result is `[0, 1, 2]` (three distinct (col0, col1) tuples)
