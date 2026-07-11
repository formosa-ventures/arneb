//! Batch-aware group-by hashing with typed-array hot path.
//!
//! `GroupByHash` assigns a stable `u32` ID to each distinct group key
//! across one or more `RecordBatch`es. The hot path of
//! `HashAggregateExec` calls [`GroupByHash::get_group_ids`] once per
//! batch to obtain a `Vec<u32>` aligned with the batch row count, then
//! hands those IDs to each [`crate::GroupedAccumulator`] in a single
//! `add_input` call.
//!
//! Internal storage adapts to the group-key shape on the first batch:
//!
//! * **Bigint fast path** — single `Int64` column. Keys stored as flat
//!   `Vec<i64>` (no `ScalarValue` enum unwrap on the hot compare),
//!   hashed via `RandomState::hash_one`. Mirrors Trino's
//!   `BigintGroupByHash` (`core/trino-main/.../BigintGroupByHash.java`).
//!   Cuts TPC-H Q17's inner `GROUP BY l_partkey` (6M rows, 200K groups)
//!   from ~430ms toward the per-output-row rate seen on low-cardinality
//!   aggregates like Q01.
//! * **Generic path** — multi-col or non-Int64 keys, stored as
//!   `Vec<GroupKey>`. Compares typed-array-vs-stored-ScalarValue per
//!   row without re-allocating a `ScalarValue` (only Vacant insert
//!   materialises a `GroupKey`).
//!
//! Generic-path keys are materialised lazily into a `Vec<GroupKey>`
//! inside `KeyStorage::Generic`; the parallel-partial-merge path reads
//! them via `build_group_arrays` + `get_group_ids`.

use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::sync::OnceLock;

use ahash::RandomState;
use arneb_common::error::ExecutionError;
use arneb_common::types::ScalarValue;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{self, DataType as ArrowDataType, Int64Type};
use arrow::row::{RowConverter, RowParser, SortField};

use crate::group_key::{GroupKey, TAG_FOR_TYPED};
use crate::operator::extract_scalar;

/// Maps `GroupKey` → `u32` group ID across batches.
///
/// Stateful: instances accumulate keys across multiple calls to
/// [`get_group_ids`]. Used by `HashAggregateExec` and reused per
/// partition in the parallel path.
pub(crate) struct GroupByHash {
    /// Raw hash table storing only the group_id payload.
    table: hashbrown::HashTable<u32>,
    /// Seeded hasher state. Stable across rehashes.
    state: RandomState,
    /// Default-off opt-in to the radix-partitioned bigint path.
    radix_agg_enabled: bool,
    /// Default-off custom bigint lookup with a contiguous, prefetchable
    /// bucket array. Captured once when this instance is constructed.
    agg_prefetch_enabled: bool,
    /// Optional pre-size hint, already gated by `ARNEB_AGG_PRESIZE` and
    /// capped to avoid turning an over-estimate into an immediate OOM.
    capacity_hint: Option<usize>,
    /// Lazy-initialised on first call to [`get_group_ids`]. After
    /// init, all subsequent calls assume the same key shape (planner
    /// guarantees this for a single `HashAggregateExec`).
    storage: KeyStorage,
}

/// Internal key store. Variant chosen on first batch.
enum KeyStorage {
    Uninit,
    /// Single Int64 key. `keys[group_id] = i64`. For NULL keys we
    /// stash the position in `null_group_id` and write a placeholder
    /// 0 into `keys` so positional indexing stays cheap.
    Bigint {
        keys: Vec<i64>,
        null_group_id: Option<u32>,
        /// Custom open-addressing lookup used only by `ARNEB_AGG_PREFETCH`.
        prefetch_table: Option<BigintPrefetchTable>,
        /// Optional two-level table for high-cardinality Int64 keys.
        /// Each partition stores `(key, group_id)` inline so equality
        /// checks avoid a random load from `keys`.
        radix_tables: Option<Vec<hashbrown::HashTable<(i64, u32)>>>,
    },
    /// Multi-column (or non-Int64 single col) key encoded via
    /// Apache Arrow's [`RowConverter`] into a byte slice that's
    /// bytewise-comparable for equality. Mirrors Trino's
    /// `FlatGroupByHash` — pointer-free per-row storage in a shared
    /// `Vec<u8>` buffer with parallel `(start, len)` ranges per group.
    /// Cached `ahash` per group lets `hashbrown`'s rehash closure
    /// avoid re-scanning the row bytes.
    FlatRow {
        converter: Arc<RowConverter>,
        parser: RowParser,
        buffer: Vec<u8>,
        /// `(start, len)` of group_id's encoded row inside `buffer`.
        ranges: Vec<(u32, u32)>,
        /// Cached `state.hash_one(row_bytes)` per group, parallel to
        /// `ranges`. Used by `hashbrown::HashTable`'s rehash closure
        /// so growing the table doesn't pay O(N × row_len) ahash work.
        hashes: Vec<u64>,
    },
    /// Slowest-fallback path: arbitrary types not supported by
    /// `RowConverter`. `keys[group_id] = GroupKey` (Vec<ScalarValue>).
    Generic {
        keys: Vec<GroupKey>,
    },
}

impl Default for GroupByHash {
    fn default() -> Self {
        Self::new()
    }
}

/// Fixed seed so the table's `hasher_for_id` closure (called on
/// rehash) and per-row hashing produce identical bytes — ahash's
/// `RandomState::default()` randomises per-process.
const STATE_SEEDS: (u64, u64, u64, u64) = (0xA571_E6F1, 0xC0FF_EE15, 0xDECA_FBAD, 0x1234_5678);
const RADIX_AGG_PARTITIONS: usize = 256;
const RADIX_AGG_BITS: u32 = 8;
const RADIX_AGG_SHIFT: u32 = 64 - RADIX_AGG_BITS;
pub(crate) const AGG_PRESIZE_MAX_GROUPS: usize = 64_000_000;
const AGG_ADAPTIVE_HIGH_CARDINALITY_RATIO: f64 = 0.5;
const AGG_ADAPTIVE_NEAR_CAPACITY_NUM: usize = 7;
const AGG_ADAPTIVE_NEAR_CAPACITY_DEN: usize = 8;
const AGG_ADAPTIVE_GEOMETRIC_FACTOR: usize = 4;

impl GroupByHash {
    pub(crate) fn new() -> Self {
        Self::with_options(radix_agg_enabled(), agg_prefetch_enabled(), None)
    }

    pub(crate) fn with_estimated_groups(estimated_groups: Option<usize>) -> Self {
        Self::with_options(
            radix_agg_enabled(),
            agg_prefetch_enabled(),
            estimated_groups,
        )
    }

    #[cfg(test)]
    fn with_radix_agg(radix_agg_enabled: bool) -> Self {
        Self::with_options(radix_agg_enabled, false, None)
    }

    fn with_options(
        radix_agg_enabled: bool,
        agg_prefetch_enabled: bool,
        estimated_groups: Option<usize>,
    ) -> Self {
        Self {
            table: hashbrown::HashTable::new(),
            state: RandomState::with_seeds(
                STATE_SEEDS.0,
                STATE_SEEDS.1,
                STATE_SEEDS.2,
                STATE_SEEDS.3,
            ),
            radix_agg_enabled,
            agg_prefetch_enabled,
            capacity_hint: agg_presize_capacity(estimated_groups),
            storage: KeyStorage::Uninit,
        }
    }

    #[cfg(test)]
    fn with_agg_prefetch_for_test(agg_prefetch_enabled: bool) -> Self {
        Self::with_options(false, agg_prefetch_enabled, None)
    }

    #[cfg(test)]
    fn with_capacity_hint_for_test(radix_agg_enabled: bool, capacity_hint: Option<usize>) -> Self {
        Self {
            table: hashbrown::HashTable::new(),
            state: RandomState::with_seeds(
                STATE_SEEDS.0,
                STATE_SEEDS.1,
                STATE_SEEDS.2,
                STATE_SEEDS.3,
            ),
            radix_agg_enabled,
            agg_prefetch_enabled: false,
            capacity_hint,
            storage: KeyStorage::Uninit,
        }
    }

    /// Number of distinct groups seen so far.
    pub(crate) fn num_groups(&self) -> usize {
        match &self.storage {
            KeyStorage::Uninit => 0,
            KeyStorage::Bigint { keys, .. } => keys.len(),
            KeyStorage::FlatRow { ranges, .. } => ranges.len(),
            KeyStorage::Generic { keys } => keys.len(),
        }
    }

    /// Reserve hash buckets and key storage for at least `groups` groups.
    ///
    /// This is allocation-only: it does not insert keys, alter existing
    /// group IDs, or touch accumulator state. If called before the first
    /// batch, the reservation is applied lazily when storage is initialised.
    pub(crate) fn reserve_groups(&mut self, groups: usize) {
        let target = groups.min(AGG_PRESIZE_MAX_GROUPS);
        if target == 0 || target <= self.storage_capacity() {
            return;
        }
        match &mut self.storage {
            KeyStorage::Uninit => {
                self.capacity_hint = Some(self.capacity_hint.unwrap_or(0).max(target));
            }
            KeyStorage::Bigint {
                keys,
                prefetch_table,
                radix_tables,
                ..
            } => {
                reserve_vec_to(keys, target);
                if let Some(table) = prefetch_table {
                    table.reserve(target, keys, &self.state);
                } else if let Some(tables) = radix_tables {
                    reserve_radix_tables_to(&self.state, tables, target);
                } else {
                    reserve_table_to(&self.state, &mut self.table, target, |state, &gid| {
                        state.hash_one(keys[gid as usize])
                    });
                }
            }
            KeyStorage::FlatRow { ranges, hashes, .. } => {
                reserve_vec_to(ranges, target);
                reserve_vec_to(hashes, target);
                reserve_table_to(&self.state, &mut self.table, target, |_state, &gid| {
                    hashes[gid as usize]
                });
            }
            KeyStorage::Generic { keys } => {
                reserve_vec_to(keys, target);
                reserve_table_to(&self.state, &mut self.table, target, |state, &gid| {
                    hash_generic_key(state, &keys[gid as usize])
                });
            }
        }
    }

    /// Default-off adaptive presize hook for partial aggregates. The caller
    /// supplies total rows seen so far, allowing high-cardinality streams to
    /// grow from observed data instead of relying only on planner estimates.
    pub(crate) fn adaptive_reserve_after_batch(&mut self, rows_seen: usize) {
        if agg_presize_adaptive_enabled() {
            self.adaptive_reserve_after_batch_inner(rows_seen);
        }
    }

    fn adaptive_reserve_after_batch_inner(&mut self, rows_seen: usize) {
        let groups = self.num_groups();
        if rows_seen == 0 || groups == 0 {
            return;
        }
        let ratio = groups as f64 / rows_seen as f64;
        if ratio <= AGG_ADAPTIVE_HIGH_CARDINALITY_RATIO || !self.near_capacity(groups) {
            return;
        }
        let target = groups
            .saturating_mul(AGG_ADAPTIVE_GEOMETRIC_FACTOR)
            .min(AGG_PRESIZE_MAX_GROUPS);
        self.reserve_groups(target);
    }

    fn near_capacity(&self, groups: usize) -> bool {
        let capacity = self.storage_capacity();
        capacity == 0
            || groups.saturating_mul(AGG_ADAPTIVE_NEAR_CAPACITY_DEN)
                >= capacity.saturating_mul(AGG_ADAPTIVE_NEAR_CAPACITY_NUM)
    }

    pub(crate) fn storage_capacity(&self) -> usize {
        match &self.storage {
            KeyStorage::Uninit => self.capacity_hint.unwrap_or(0),
            KeyStorage::Bigint {
                keys,
                prefetch_table,
                radix_tables,
                ..
            } => prefetch_table
                .as_ref()
                .map(BigintPrefetchTable::capacity)
                .or_else(|| {
                    radix_tables
                        .as_ref()
                        .map(|tables| tables.iter().map(|table| table.capacity()).sum())
                })
                .unwrap_or_else(|| self.table.capacity())
                .max(keys.capacity()),
            KeyStorage::FlatRow { ranges, .. } => self.table.capacity().max(ranges.capacity()),
            KeyStorage::Generic { keys } => self.table.capacity().max(keys.capacity()),
        }
    }

    /// exec-memory-accounting D3: approximate heap bytes held by the group
    /// key store + hash table (the accumulators are sized separately by the
    /// caller). Used by `HashAggregateExec` to reserve the group state against
    /// the global `MemoryPool` so a large GROUP BY (e.g. q18's 45 M-group
    /// `lineitem GROUP BY l_orderkey`) is visible to the pool and the
    /// pool-pressure exchange spill (D2) can balance, instead of silently
    /// growing past the cgroup and OOM-killing the worker.
    pub(crate) fn heap_bytes(&self) -> usize {
        let table = self.table.capacity() * std::mem::size_of::<u32>();
        let storage = match &self.storage {
            KeyStorage::Uninit => 0,
            KeyStorage::Bigint {
                keys,
                prefetch_table,
                radix_tables,
                ..
            } => {
                let radix_table_bytes = radix_tables
                    .as_ref()
                    .map(|tables| {
                        tables
                            .iter()
                            .map(|t| t.capacity() * std::mem::size_of::<(i64, u32)>())
                            .sum::<usize>()
                    })
                    .unwrap_or(0);
                let prefetch_table_bytes = prefetch_table
                    .as_ref()
                    .map_or(0, BigintPrefetchTable::heap_bytes);
                keys.capacity() * std::mem::size_of::<i64>()
                    + radix_table_bytes
                    + prefetch_table_bytes
            }
            KeyStorage::FlatRow {
                buffer,
                ranges,
                hashes,
                ..
            } => {
                buffer.capacity()
                    + ranges.capacity() * std::mem::size_of::<(u32, u32)>()
                    + hashes.capacity() * std::mem::size_of::<u64>()
            }
            // GroupKey is a Vec<ScalarValue>; estimate a flat ~32 B/group
            // (this fallback path is rare — RowConverter covers most types).
            KeyStorage::Generic { keys } => keys.capacity() * 32,
        };
        table + storage
    }

    /// Assign a `u32` group ID to each row in the batch. Inserts new
    /// keys on first sight, reuses existing IDs otherwise.
    ///
    /// On the first call, picks the storage variant by inspecting the
    /// columns' Arrow data types — single `Int64` opts into the
    /// Bigint fast path; everything else uses the generic path.
    pub(crate) fn get_group_ids(
        &mut self,
        group_cols: &[ArrayRef],
    ) -> Result<Vec<u32>, ExecutionError> {
        let n_rows = group_cols.first().map(|c| c.len()).unwrap_or(0);
        for c in group_cols.iter().skip(1) {
            if c.len() != n_rows {
                return Err(ExecutionError::InvalidOperation(format!(
                    "GroupByHash::get_group_ids: group cols have mismatched lengths ({} vs {})",
                    n_rows,
                    c.len()
                )));
            }
        }

        // Choose storage on first call.
        if matches!(self.storage, KeyStorage::Uninit) {
            let custom_bigint = (self.radix_agg_enabled || self.agg_prefetch_enabled)
                && group_cols.len() == 1
                && matches!(group_cols[0].data_type(), ArrowDataType::Int64);
            if let Some(capacity) = self.capacity_hint.filter(|_| !custom_bigint) {
                self.table = hashbrown::HashTable::with_capacity(capacity);
            }
            self.storage = pick_storage(
                group_cols,
                self.radix_agg_enabled,
                self.agg_prefetch_enabled,
                self.capacity_hint,
            );
        }

        match &mut self.storage {
            KeyStorage::Bigint {
                keys,
                null_group_id,
                prefetch_table,
                radix_tables,
            } => Ok(get_group_ids_bigint(
                &self.state,
                &mut self.table,
                keys,
                null_group_id,
                prefetch_table.as_mut(),
                radix_tables.as_mut(),
                &group_cols[0],
            )),
            KeyStorage::FlatRow {
                converter,
                buffer,
                ranges,
                hashes,
                ..
            } => get_group_ids_flat(
                &self.state,
                &mut self.table,
                converter,
                buffer,
                ranges,
                hashes,
                group_cols,
            ),
            KeyStorage::Generic { keys } => {
                get_group_ids_generic(&self.state, &mut self.table, keys, group_cols)
            }
            KeyStorage::Uninit => unreachable!(),
        }
    }

    /// Materialise the group-key column(s) as Arrow arrays. One
    /// `ArrayRef` per original group-by expression, in declared
    /// order. The Bigint fast path produces a single `Int64Array`
    /// directly from its flat `Vec<i64>`; the generic path converts
    /// each `Vec<ScalarValue>` slice via
    /// [`crate::operator::scalars_to_array`].
    pub(crate) fn build_group_arrays(&self) -> Result<Vec<ArrayRef>, ExecutionError> {
        match &self.storage {
            KeyStorage::Uninit => Ok(vec![]),
            KeyStorage::Bigint {
                keys,
                null_group_id,
                ..
            } => Ok(vec![build_bigint_array(keys, *null_group_id)]),
            KeyStorage::FlatRow {
                converter,
                parser,
                buffer,
                ranges,
                ..
            } => build_flat_arrays(converter, parser, buffer, ranges),
            KeyStorage::Generic { keys } => build_generic_arrays(keys),
        }
    }
}

/// Assign input rows to grace-spill partitions using the same typed,
/// width-normalized group-key hash bytes as `GroupByHash`.
pub(crate) fn group_partition_assignments(
    group_cols: &[ArrayRef],
    n_partitions: usize,
) -> Result<Vec<u32>, ExecutionError> {
    if n_partitions == 0 {
        return Err(ExecutionError::InvalidOperation(
            "aggregate spill partition count must be greater than zero".to_string(),
        ));
    }
    let n_rows = group_cols.first().map(|c| c.len()).unwrap_or(0);
    for c in group_cols.iter().skip(1) {
        if c.len() != n_rows {
            return Err(ExecutionError::InvalidOperation(format!(
                "group_partition_assignments: group cols have mismatched lengths ({} vs {})",
                n_rows,
                c.len()
            )));
        }
    }

    let state = RandomState::with_seeds(STATE_SEEDS.0, STATE_SEEDS.1, STATE_SEEDS.2, STATE_SEEDS.3);
    let typed: Vec<TypedGroupCol> = group_cols
        .iter()
        .map(TypedGroupCol::from_array)
        .collect::<Result<_, _>>()?;
    let mut out = Vec::with_capacity(n_rows);
    for row in 0..n_rows {
        let h = hash_typed_row(&state, &typed, row);
        out.push((h % n_partitions as u64) as u32);
    }
    Ok(out)
}

// ===========================================================================
// Storage variant selection
// ===========================================================================

fn pick_storage(
    group_cols: &[ArrayRef],
    radix_agg_enabled: bool,
    agg_prefetch_enabled: bool,
    capacity_hint: Option<usize>,
) -> KeyStorage {
    if group_cols.is_empty() {
        return KeyStorage::Generic { keys: Vec::new() };
    }

    // Fast path: single Int64 column → BigintGroupByHash analog.
    if group_cols.len() == 1 && matches!(group_cols[0].data_type(), ArrowDataType::Int64) {
        return KeyStorage::Bigint {
            keys: vec_with_capacity_hint(capacity_hint),
            null_group_id: None,
            prefetch_table: agg_prefetch_enabled
                .then(|| BigintPrefetchTable::new(capacity_hint.unwrap_or(0))),
            radix_tables: (radix_agg_enabled && !agg_prefetch_enabled)
                .then(|| new_radix_tables(capacity_hint)),
        };
    }

    // Flat-row path: arrow::row::RowConverter supports all primitive,
    // string, decimal, date, and most nested types. We probe via
    // `supports_fields` before instantiation so unsupported shapes
    // (e.g. Union types) fall back to the slow Generic path.
    let fields: Vec<SortField> = group_cols
        .iter()
        .map(|c| SortField::new(c.data_type().clone()))
        .collect();
    if RowConverter::supports_fields(&fields) {
        if let Ok(conv) = RowConverter::new(fields) {
            let parser = conv.parser();
            return KeyStorage::FlatRow {
                converter: Arc::new(conv),
                parser,
                buffer: Vec::new(),
                ranges: vec_with_capacity_hint(capacity_hint),
                hashes: vec_with_capacity_hint(capacity_hint),
            };
        }
    }

    KeyStorage::Generic {
        keys: vec_with_capacity_hint(capacity_hint),
    }
}

fn radix_agg_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_RADIX_AGG")
            .map(|v| v == "1")
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            radix_agg = enabled,
            "ARNEB_RADIX_AGG effective value (default off; =1 to enable radix bigint aggregation)"
        );
        enabled
    })
}

fn agg_prefetch_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_AGG_PREFETCH")
            .is_ok_and(|value| matches!(value.as_str(), "1" | "true"));
        tracing::info!(
            target: "arneb::config",
            agg_prefetch = enabled,
            "ARNEB_AGG_PREFETCH effective value (default off; =1/true to enable bigint aggregate hash prefetch)"
        );
        enabled
    })
}

fn agg_presize_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_AGG_PRESIZE")
            .map(|v| v == "1")
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            agg_presize = enabled,
            max_groups = AGG_PRESIZE_MAX_GROUPS,
            "ARNEB_AGG_PRESIZE effective value (default off; =1 to pre-size aggregate group hash tables)"
        );
        enabled
    })
}

fn agg_presize_capacity(estimated_groups: Option<usize>) -> Option<usize> {
    let estimated = estimated_groups.filter(|&n| n > 0)?;
    if !agg_presize_enabled() {
        return None;
    }
    let capped = estimated.min(AGG_PRESIZE_MAX_GROUPS);
    if capped < estimated {
        tracing::info!(
            target: "arneb::config",
            estimated_groups = estimated,
            applied_groups = capped,
            "ARNEB_AGG_PRESIZE estimate capped"
        );
    }
    Some(capped)
}

pub(crate) fn agg_presize_adaptive_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_AGG_PRESIZE_ADAPTIVE")
            .map(|v| v == "1")
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            agg_presize_adaptive = enabled,
            max_groups = AGG_PRESIZE_MAX_GROUPS,
            "ARNEB_AGG_PRESIZE_ADAPTIVE effective value (default off; =1 to adaptively pre-size aggregate group hash tables)"
        );
        enabled
    })
}

fn vec_with_capacity_hint<T>(capacity_hint: Option<usize>) -> Vec<T> {
    capacity_hint.map_or_else(Vec::new, Vec::with_capacity)
}

fn reserve_vec_to<T>(values: &mut Vec<T>, target: usize) {
    if values.capacity() < target {
        values.reserve(target - values.len());
    }
}

fn reserve_table_to<T, F>(
    state: &RandomState,
    table: &mut hashbrown::HashTable<T>,
    target: usize,
    hasher: F,
) where
    F: Fn(&RandomState, &T) -> u64,
{
    if table.capacity() < target {
        table.reserve(target - table.len(), |value| hasher(state, value));
    }
}

fn reserve_radix_tables_to(
    state: &RandomState,
    tables: &mut [hashbrown::HashTable<(i64, u32)>],
    target: usize,
) {
    let per_partition = target.saturating_add(RADIX_AGG_PARTITIONS - 1) / RADIX_AGG_PARTITIONS;
    for table in tables {
        if table.capacity() < per_partition {
            table.reserve(per_partition - table.len(), |&(key, _)| state.hash_one(key));
        }
    }
}

fn new_radix_tables(capacity_hint: Option<usize>) -> Vec<hashbrown::HashTable<(i64, u32)>> {
    let per_partition = capacity_hint
        .map(|capacity| capacity.saturating_add(RADIX_AGG_PARTITIONS - 1) / RADIX_AGG_PARTITIONS);
    (0..RADIX_AGG_PARTITIONS)
        .map(|_| {
            per_partition.map_or_else(
                hashbrown::HashTable::new,
                hashbrown::HashTable::with_capacity,
            )
        })
        .collect()
}

// ===========================================================================
// Bigint fast path
// ===========================================================================

/// Open-addressing bigint lookup whose contiguous bucket array can be
/// prefetched by hash. Buckets contain group IDs; keys remain in insertion
/// order in `KeyStorage::Bigint::keys`.
struct BigintPrefetchTable {
    buckets: Vec<u32>,
    mask: usize,
    len: usize,
}

impl BigintPrefetchTable {
    const EMPTY: u32 = u32::MAX;
    const MIN_BUCKETS: usize = 8;

    fn new(group_capacity: usize) -> Self {
        let mut table = Self {
            buckets: Vec::new(),
            mask: 0,
            len: 0,
        };
        table.allocate_for_groups(group_capacity);
        table
    }

    /// Usable group capacity at a maximum 50% load factor.
    fn capacity(&self) -> usize {
        self.buckets.len() / 2
    }

    fn heap_bytes(&self) -> usize {
        self.buckets.capacity() * std::mem::size_of::<u32>()
    }

    fn reserve(&mut self, groups: usize, keys: &[i64], state: &RandomState) {
        if groups <= self.capacity() {
            return;
        }
        let old_buckets = std::mem::take(&mut self.buckets);
        self.allocate_for_groups(groups);
        self.len = 0;
        for group_id in old_buckets {
            if group_id != Self::EMPTY {
                self.insert_rehashed(state.hash_one(keys[group_id as usize]), group_id);
            }
        }
    }

    fn allocate_for_groups(&mut self, groups: usize) {
        let bucket_count = groups
            .saturating_mul(2)
            .max(Self::MIN_BUCKETS)
            .checked_next_power_of_two()
            .unwrap_or(usize::MAX / 2 + 1);
        self.buckets = vec![Self::EMPTY; bucket_count];
        self.mask = bucket_count - 1;
    }

    fn insert_rehashed(&mut self, hash: u64, group_id: u32) {
        let mut slot = hash as usize & self.mask;
        loop {
            if self.buckets[slot] == Self::EMPTY {
                self.buckets[slot] = group_id;
                self.len += 1;
                return;
            }
            slot = (slot + 1) & self.mask;
        }
    }

    #[inline]
    fn find_or_insert(&mut self, hash: u64, value: i64, keys: &mut Vec<i64>) -> u32 {
        let mut slot = hash as usize & self.mask;
        loop {
            let group_id = self.buckets[slot];
            if group_id == Self::EMPTY {
                let group_id = keys.len() as u32;
                keys.push(value);
                self.buckets[slot] = group_id;
                self.len += 1;
                return group_id;
            }
            if keys[group_id as usize] == value {
                return group_id;
            }
            slot = (slot + 1) & self.mask;
        }
    }

    /// Hint the cache line containing the initial bucket for `hash`.
    /// No-op outside x86_64 because stable Rust has no portable intrinsic.
    #[allow(unsafe_code)]
    #[inline]
    fn prefetch_slot(&self, hash: u64) {
        #[cfg(target_arch = "x86_64")]
        {
            let slot = hash as usize & self.mask;
            // SAFETY: `slot` is masked into the allocated bucket range, and
            // `_mm_prefetch` is a CPU hint that does not dereference it.
            unsafe {
                core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(
                    self.buckets.as_ptr().add(slot) as *const i8,
                );
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            let _ = hash;
        }
    }
}

fn get_group_ids_bigint(
    state: &RandomState,
    table: &mut hashbrown::HashTable<u32>,
    keys: &mut Vec<i64>,
    null_group_id: &mut Option<u32>,
    prefetch_table: Option<&mut BigintPrefetchTable>,
    radix_tables: Option<&mut Vec<hashbrown::HashTable<(i64, u32)>>>,
    col: &ArrayRef,
) -> Vec<u32> {
    let arr = col.as_primitive::<Int64Type>();
    let n = arr.len();
    let mut out = Vec::with_capacity(n);

    if let Some(prefetch_table) = prefetch_table {
        return get_group_ids_bigint_prefetch(state, prefetch_table, keys, null_group_id, arr);
    }

    if let Some(tables) = radix_tables {
        if arr.null_count() == 0 {
            for row in 0..n {
                let value = arr.value(row);
                let id = bigint_radix_insert_or_get(state, tables, keys, value);
                out.push(id);
            }
        } else {
            for row in 0..n {
                if arr.is_null(row) {
                    let id = match *null_group_id {
                        Some(id) => id,
                        None => {
                            let id = keys.len() as u32;
                            keys.push(0); // placeholder
                            *null_group_id = Some(id);
                            id
                        }
                    };
                    out.push(id);
                    continue;
                }
                let value = arr.value(row);
                let id = bigint_radix_insert_or_get(state, tables, keys, value);
                out.push(id);
            }
        }
        return out;
    }

    // Hoist null-check loop out: if no nulls, run the tight value-only
    // loop with no per-row null branch (mirrors Trino's
    // `block.mayHaveNull()` hoist before the probe inner loop).
    if arr.null_count() == 0 {
        for row in 0..n {
            let value = arr.value(row);
            let id = bigint_insert_or_get(state, table, keys, value);
            out.push(id);
        }
    } else {
        for row in 0..n {
            if arr.is_null(row) {
                let id = match *null_group_id {
                    Some(id) => id,
                    None => {
                        let id = keys.len() as u32;
                        keys.push(0); // placeholder
                        *null_group_id = Some(id);
                        id
                    }
                };
                out.push(id);
                continue;
            }
            let value = arr.value(row);
            let id = bigint_insert_or_get(state, table, keys, value);
            out.push(id);
        }
    }
    out
}

fn get_group_ids_bigint_prefetch(
    state: &RandomState,
    table: &mut BigintPrefetchTable,
    keys: &mut Vec<i64>,
    null_group_id: &mut Option<u32>,
    arr: &Int64Array,
) -> Vec<u32> {
    const PREFETCH_DIST: usize = 8;

    let hashes: Vec<u64> = arr
        .values()
        .iter()
        .map(|&value| state.hash_one(value))
        .collect();
    // Reserve for the worst case before probing so the bucket pointer remains
    // stable throughout this batch's look-ahead prefetch loop.
    table.reserve(table.len.saturating_add(arr.len()), keys, state);

    let mut out = Vec::with_capacity(arr.len());
    for row in 0..arr.len() {
        let prefetch_row = row + PREFETCH_DIST;
        if prefetch_row < arr.len() {
            table.prefetch_slot(hashes[prefetch_row]);
        }

        let group_id = if arr.is_null(row) {
            match *null_group_id {
                Some(group_id) => group_id,
                None => {
                    let group_id = keys.len() as u32;
                    keys.push(0);
                    *null_group_id = Some(group_id);
                    group_id
                }
            }
        } else {
            table.find_or_insert(hashes[row], arr.value(row), keys)
        };
        out.push(group_id);
    }
    out
}

#[inline]
fn bigint_insert_or_get(
    state: &RandomState,
    table: &mut hashbrown::HashTable<u32>,
    keys: &mut Vec<i64>,
    value: i64,
) -> u32 {
    let hash = state.hash_one(value);
    let entry = table.entry(
        hash,
        |&gid| keys[gid as usize] == value,
        |&gid| state.hash_one(keys[gid as usize]),
    );
    match entry {
        hashbrown::hash_table::Entry::Occupied(o) => *o.get(),
        hashbrown::hash_table::Entry::Vacant(v) => {
            let id = keys.len() as u32;
            keys.push(value);
            v.insert(id);
            id
        }
    }
}

#[inline]
fn bigint_radix_partition(hash: u64) -> usize {
    ((hash >> RADIX_AGG_SHIFT) as usize) & (RADIX_AGG_PARTITIONS - 1)
}

#[inline]
fn bigint_radix_insert_or_get(
    state: &RandomState,
    tables: &mut [hashbrown::HashTable<(i64, u32)>],
    keys: &mut Vec<i64>,
    value: i64,
) -> u32 {
    let hash = state.hash_one(value);
    let table = &mut tables[bigint_radix_partition(hash)];
    let entry = table.entry(hash, |&(k, _)| k == value, |&(k, _)| state.hash_one(k));
    match entry {
        hashbrown::hash_table::Entry::Occupied(o) => o.get().1,
        hashbrown::hash_table::Entry::Vacant(v) => {
            let id = keys.len() as u32;
            keys.push(value);
            v.insert((value, id));
            id
        }
    }
}

// ===========================================================================
// Flat-row (RowConverter) path — Trino FlatGroupByHash analog
// ===========================================================================

#[allow(clippy::too_many_arguments)]
fn get_group_ids_flat(
    state: &RandomState,
    table: &mut hashbrown::HashTable<u32>,
    converter: &RowConverter,
    buffer: &mut Vec<u8>,
    ranges: &mut Vec<(u32, u32)>,
    hashes: &mut Vec<u64>,
    group_cols: &[ArrayRef],
) -> Result<Vec<u32>, ExecutionError> {
    let rows = converter
        .convert_columns(group_cols)
        .map_err(|e| ExecutionError::InvalidOperation(format!("RowConverter encode: {e}")))?;
    let n = rows.num_rows();
    let mut out = Vec::with_capacity(n);

    for i in 0..n {
        let row = rows.row(i);
        let row_bytes: &[u8] = row.as_ref();
        let hash = state.hash_one(row_bytes);
        // Capture borrows locally so the closures don't conflict with
        // `table`'s mutable borrow.
        let ranges_ref = &*ranges;
        let buffer_ref = &*buffer;
        let hashes_ref = &*hashes;
        let entry = table.entry(
            hash,
            |&gid| {
                let (s, l) = ranges_ref[gid as usize];
                let stored = &buffer_ref[s as usize..s as usize + l as usize];
                stored == row_bytes
            },
            |&gid| hashes_ref[gid as usize],
        );
        let id = match entry {
            hashbrown::hash_table::Entry::Occupied(o) => *o.get(),
            hashbrown::hash_table::Entry::Vacant(v) => {
                let start = buffer.len() as u32;
                let len = row_bytes.len() as u32;
                buffer.extend_from_slice(row_bytes);
                let id = ranges.len() as u32;
                ranges.push((start, len));
                hashes.push(hash);
                v.insert(id);
                id
            }
        };
        out.push(id);
    }
    Ok(out)
}

fn build_flat_arrays(
    converter: &RowConverter,
    parser: &RowParser,
    buffer: &[u8],
    ranges: &[(u32, u32)],
) -> Result<Vec<ArrayRef>, ExecutionError> {
    let parsed: Vec<arrow::row::Row<'_>> = ranges
        .iter()
        .map(|&(s, l)| parser.parse(&buffer[s as usize..s as usize + l as usize]))
        .collect();
    converter
        .convert_rows(parsed.iter().copied())
        .map_err(|e| ExecutionError::InvalidOperation(format!("RowConverter decode: {e}")))
}

fn build_bigint_array(keys: &[i64], null_group_id: Option<u32>) -> ArrayRef {
    if let Some(null_pos) = null_group_id.map(|id| id as usize) {
        let mut b = arrow::array::Int64Builder::with_capacity(keys.len());
        for (i, &v) in keys.iter().enumerate() {
            if i == null_pos {
                b.append_null();
            } else {
                b.append_value(v);
            }
        }
        Arc::new(b.finish()) as ArrayRef
    } else {
        Arc::new(Int64Array::from_iter_values(keys.iter().copied())) as ArrayRef
    }
}

// ===========================================================================
// Generic path
// ===========================================================================

fn hash_generic_key(state: &RandomState, key: &GroupKey) -> u64 {
    let mut h = state.build_hasher();
    h.write_usize(key.0.len());
    for s in &key.0 {
        crate::group_key::hash_scalar_bytes(s, &mut h);
    }
    h.finish()
}

fn get_group_ids_generic(
    state: &RandomState,
    table: &mut hashbrown::HashTable<u32>,
    keys: &mut Vec<GroupKey>,
    group_cols: &[ArrayRef],
) -> Result<Vec<u32>, ExecutionError> {
    let n_rows = group_cols.first().map(|c| c.len()).unwrap_or(0);

    // Hoist typed downcast once per batch instead of per row.
    let typed: Vec<TypedGroupCol> = group_cols
        .iter()
        .map(TypedGroupCol::from_array)
        .collect::<Result<_, _>>()?;

    let mut out = Vec::with_capacity(n_rows);
    for row in 0..n_rows {
        let hash = hash_typed_row(state, &typed, row);
        let typed_ref = &typed;

        let entry = table.entry(
            hash,
            |&gid| typed_row_eq_key(typed_ref, row, &keys[gid as usize]),
            |&gid| hash_generic_key(state, &keys[gid as usize]),
        );

        let id = match entry {
            hashbrown::hash_table::Entry::Occupied(o) => *o.get(),
            hashbrown::hash_table::Entry::Vacant(v) => {
                let id = keys.len() as u32;
                let mut values: Vec<ScalarValue> = Vec::with_capacity(group_cols.len());
                for col in group_cols {
                    values.push(extract_scalar(col, row)?);
                }
                keys.push(GroupKey(values));
                v.insert(id);
                id
            }
        };
        out.push(id);
    }
    Ok(out)
}

fn build_generic_arrays(keys: &[GroupKey]) -> Result<Vec<ArrayRef>, ExecutionError> {
    let n = keys.len();
    if n == 0 {
        return Ok(vec![]);
    }
    let n_cols = keys[0].0.len();
    let mut cols: Vec<Vec<ScalarValue>> = vec![Vec::with_capacity(n); n_cols];
    for key in keys {
        for (col_i, v) in key.0.iter().enumerate() {
            cols[col_i].push(v.clone());
        }
    }
    let mut arrays = Vec::with_capacity(n_cols);
    for col_vals in cols {
        arrays.push(crate::operator::scalars_to_array(&col_vals, n)?);
    }
    Ok(arrays)
}

// ===========================================================================
// Typed group-column hot path (generic)
// ===========================================================================

/// Pre-downcasted group-key column. Built once per batch in
/// [`get_group_ids_generic`]; reused across all rows in the batch.
enum TypedGroupCol<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    Boolean(&'a BooleanArray),
    Date32(&'a arrow::array::Date32Array),
    Decimal128(&'a Decimal128Array, u8, i8),
    Generic(&'a ArrayRef),
}

impl<'a> TypedGroupCol<'a> {
    fn from_array(arr: &'a ArrayRef) -> Result<Self, ExecutionError> {
        Ok(match arr.data_type() {
            ArrowDataType::Int32 => {
                TypedGroupCol::Int32(arr.as_primitive::<datatypes::Int32Type>())
            }
            ArrowDataType::Int64 => {
                TypedGroupCol::Int64(arr.as_primitive::<datatypes::Int64Type>())
            }
            ArrowDataType::Float32 => {
                TypedGroupCol::Float32(arr.as_primitive::<datatypes::Float32Type>())
            }
            ArrowDataType::Float64 => {
                TypedGroupCol::Float64(arr.as_primitive::<datatypes::Float64Type>())
            }
            ArrowDataType::Utf8 => TypedGroupCol::Utf8(arr.as_string::<i32>()),
            ArrowDataType::Boolean => TypedGroupCol::Boolean(
                arr.as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Boolean downcast"),
            ),
            ArrowDataType::Date32 => {
                TypedGroupCol::Date32(arr.as_primitive::<datatypes::Date32Type>())
            }
            ArrowDataType::Decimal128(p, s) => {
                TypedGroupCol::Decimal128(arr.as_primitive::<datatypes::Decimal128Type>(), *p, *s)
            }
            _ => TypedGroupCol::Generic(arr),
        })
    }

    #[inline]
    fn is_null(&self, row: usize) -> bool {
        match self {
            TypedGroupCol::Int32(a) => a.is_null(row),
            TypedGroupCol::Int64(a) => a.is_null(row),
            TypedGroupCol::Float32(a) => a.is_null(row),
            TypedGroupCol::Float64(a) => a.is_null(row),
            TypedGroupCol::Utf8(a) => a.is_null(row),
            TypedGroupCol::Boolean(a) => a.is_null(row),
            TypedGroupCol::Date32(a) => a.is_null(row),
            TypedGroupCol::Decimal128(a, _, _) => a.is_null(row),
            TypedGroupCol::Generic(a) => a.is_null(row),
        }
    }
}

#[inline]
fn hash_typed_row(state: &RandomState, cols: &[TypedGroupCol<'_>], row: usize) -> u64 {
    let mut h = state.build_hasher();
    h.write_usize(cols.len());
    for col in cols {
        if col.is_null(row) {
            h.write_u8(TAG_FOR_TYPED.null);
            continue;
        }
        match col {
            TypedGroupCol::Int32(a) => {
                h.write_u8(TAG_FOR_TYPED.int64);
                h.write_i64(i64::from(a.value(row)));
            }
            TypedGroupCol::Int64(a) => {
                h.write_u8(TAG_FOR_TYPED.int64);
                h.write_i64(a.value(row));
            }
            TypedGroupCol::Float32(a) => {
                h.write_u8(TAG_FOR_TYPED.float32);
                h.write_u32(a.value(row).to_bits());
            }
            TypedGroupCol::Float64(a) => {
                h.write_u8(TAG_FOR_TYPED.float64);
                h.write_u64(a.value(row).to_bits());
            }
            TypedGroupCol::Utf8(a) => {
                let s = a.value(row);
                h.write_u8(TAG_FOR_TYPED.utf8);
                h.write_usize(s.len());
                h.write(s.as_bytes());
            }
            TypedGroupCol::Boolean(a) => {
                h.write_u8(TAG_FOR_TYPED.boolean);
                h.write_u8(a.value(row) as u8);
            }
            TypedGroupCol::Date32(a) => {
                h.write_u8(TAG_FOR_TYPED.date32);
                h.write_i32(a.value(row));
            }
            TypedGroupCol::Decimal128(a, p, s) => {
                h.write_u8(TAG_FOR_TYPED.decimal128);
                h.write_u8(*p);
                h.write_i8(*s);
                h.write_i128(a.value(row));
            }
            TypedGroupCol::Generic(arr) => {
                let sv = extract_scalar(arr, row).expect("typed generic hash extract");
                crate::group_key::hash_scalar_bytes(&sv, &mut h);
            }
        }
    }
    h.finish()
}

#[inline]
fn typed_row_eq_key(cols: &[TypedGroupCol<'_>], row: usize, key: &GroupKey) -> bool {
    if cols.len() != key.0.len() {
        return false;
    }
    for (col, sv) in cols.iter().zip(key.0.iter()) {
        let row_is_null = col.is_null(row);
        let key_is_null = matches!(sv, ScalarValue::Null);
        if row_is_null != key_is_null {
            return false;
        }
        if row_is_null {
            continue;
        }
        let same = match (col, sv) {
            (TypedGroupCol::Int32(a), ScalarValue::Int32(v)) => a.value(row) == *v,
            (TypedGroupCol::Int64(a), ScalarValue::Int64(v)) => a.value(row) == *v,
            (TypedGroupCol::Int32(a), ScalarValue::Int64(v)) => i64::from(a.value(row)) == *v,
            (TypedGroupCol::Int64(a), ScalarValue::Int32(v)) => a.value(row) == i64::from(*v),
            (TypedGroupCol::Float32(a), ScalarValue::Float32(v)) => {
                a.value(row).to_bits() == v.to_bits()
            }
            (TypedGroupCol::Float64(a), ScalarValue::Float64(v)) => {
                a.value(row).to_bits() == v.to_bits()
            }
            (TypedGroupCol::Utf8(a), ScalarValue::Utf8(v)) => a.value(row) == v.as_str(),
            (TypedGroupCol::Boolean(a), ScalarValue::Boolean(v)) => a.value(row) == *v,
            (TypedGroupCol::Date32(a), ScalarValue::Date32(v)) => a.value(row) == *v,
            (
                TypedGroupCol::Decimal128(a, p_col, s_col),
                ScalarValue::Decimal128 {
                    value,
                    precision,
                    scale,
                },
            ) => a.value(row) == *value && *p_col == *precision && *s_col == *scale,
            (TypedGroupCol::Generic(arr), _) => {
                let row_sv = match extract_scalar(arr, row) {
                    Ok(v) => v,
                    Err(_) => return false,
                };
                crate::group_key::scalars_equal(&row_sv, sv)
            }
            _ => false,
        };
        if !same {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::{GroupedAccumulator, GroupedSumAccumulator};
    use crate::datasource::{DataSource, InMemoryDataSource};
    use crate::operator::{ExecutionPlan, HashAggregateExec, ScanExec};
    use crate::scan_context::ScanContext;
    use arneb_common::stream::collect_stream;
    use arneb_common::types::{ColumnInfo, DataType};
    use arneb_planner::PlanExpr;
    use arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn assigns_stable_ids_within_batch() {
        for radix in [false, true] {
            let mut gbh = GroupByHash::with_radix_agg(radix);
            let col: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "b"]));
            let ids = gbh.get_group_ids(&[col]).unwrap();
            assert_eq!(ids, vec![0, 0, 1]);
            assert_eq!(gbh.num_groups(), 2);
        }
    }

    #[test]
    fn reuses_ids_across_batches() {
        for radix in [false, true] {
            let mut gbh = GroupByHash::with_radix_agg(radix);
            let col1: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "b"]));
            let _ = gbh.get_group_ids(&[col1]).unwrap();
            let col2: ArrayRef = Arc::new(StringArray::from(vec!["b", "a"]));
            let ids = gbh.get_group_ids(&[col2]).unwrap();
            assert_eq!(ids, vec![1, 0]);
            assert_eq!(gbh.num_groups(), 2);
        }
    }

    #[test]
    fn null_is_a_distinct_group() {
        for radix in [false, true] {
            let mut gbh = GroupByHash::with_radix_agg(radix);
            let col: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(1), None]));
            let ids = gbh.get_group_ids(&[col]).unwrap();
            assert_eq!(ids[0], ids[2]);
            assert_eq!(ids[1], ids[3]);
            assert_ne!(ids[0], ids[1]);
            assert_eq!(gbh.num_groups(), 2);
        }
    }

    #[test]
    fn keys_preserve_insertion_order() {
        // Originally tested `gbh.keys()` directly. Since the
        // single-Utf8 path now routes through `KeyStorage::FlatRow`
        // (RowConverter-backed) for performance, the keys are
        // observable only via `build_group_arrays`.
        for radix in [false, true] {
            let mut gbh = GroupByHash::with_radix_agg(radix);
            let col: ArrayRef = Arc::new(StringArray::from(vec!["b", "a", "b"]));
            let _ = gbh.get_group_ids(&[col]).unwrap();
            let arrays = gbh.build_group_arrays().unwrap();
            assert_eq!(arrays.len(), 1);
            let s = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray");
            assert_eq!(s.value(0), "b");
            assert_eq!(s.value(1), "a");
        }
    }

    #[test]
    fn two_column_keys_compose() {
        let mut gbh = GroupByHash::new();
        let c0: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "b"]));
        let c1: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 1]));
        let ids = gbh.get_group_ids(&[c0, c1]).unwrap();
        assert_eq!(ids, vec![0, 1, 2]);
        assert_eq!(gbh.num_groups(), 3);
    }

    #[test]
    fn empty_input_returns_empty() {
        let mut gbh = GroupByHash::new();
        let col: ArrayRef = Arc::new(Int32Array::from(Vec::<i32>::new()));
        let ids = gbh.get_group_ids(&[col]).unwrap();
        assert_eq!(ids, Vec::<u32>::new());
        assert_eq!(gbh.num_groups(), 0);
    }

    #[test]
    fn mismatched_column_lengths_errors() {
        let mut gbh = GroupByHash::new();
        let c0: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let c1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let err = gbh.get_group_ids(&[c0, c1]).unwrap_err();
        assert!(matches!(err, ExecutionError::InvalidOperation(_)));
    }

    #[tokio::test]
    async fn hash_aggregate_partitioned_spill_matches_integer_reference() {
        use crate::memory_pool::{GreedyMemoryPool, MemoryPool};
        use std::collections::BTreeMap;

        let spill_enabled = std::env::var("ARNEB_AGG_SPILL")
            .map(|v| {
                matches!(
                    v.as_str(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false);
        if !spill_enabled {
            return;
        }

        let rows = 100_000i64;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", ArrowDataType::Int64, false),
            Field::new("v", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows)),
                Arc::new(Int64Array::from_iter_values(0..rows)),
            ],
        )
        .unwrap();
        let source: Arc<dyn DataSource> = Arc::new(InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "v".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            vec![batch],
        ));
        let scan: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source,
            _table_name: "t".to_string(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(512 * 1024));
        let agg = HashAggregateExec {
            input: scan,
            group_by: vec![PlanExpr::Column {
                index: 0,
                name: "k".to_string(),
                span: None,
            }],
            aggr_exprs: vec![
                PlanExpr::Function {
                    name: "SUM".to_string(),
                    args: vec![PlanExpr::Column {
                        index: 1,
                        name: "v".to_string(),
                        span: None,
                    }],
                    distinct: false,
                    span: None,
                },
                PlanExpr::Function {
                    name: "COUNT".to_string(),
                    args: vec![],
                    distinct: false,
                    span: None,
                },
            ],
            output_schema: vec![
                ColumnInfo {
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "sum_v".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "cnt".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            output_order: None,
            estimated_groups: Some(rows as usize),
            memory_pool: pool,
        };

        let batches = collect_stream(agg.execute(0).await.unwrap()).await.unwrap();
        let mut actual = BTreeMap::new();
        for batch in batches {
            let k = batch.column(0).as_primitive::<Int64Type>();
            let sum = batch.column(1).as_primitive::<Int64Type>();
            let count = batch.column(2).as_primitive::<Int64Type>();
            for row in 0..batch.num_rows() {
                actual.insert(k.value(row), (sum.value(row), count.value(row)));
            }
        }

        assert_eq!(actual.len(), rows as usize);
        for key in 0..rows {
            assert_eq!(
                actual.get(&key).copied(),
                Some((key, 1)),
                "integer SUM and COUNT must be bit-identical for key {key}"
            );
        }
    }

    #[test]
    fn bigint_fast_path_assigns_stable_ids() {
        for radix in [false, true] {
            let mut gbh = GroupByHash::with_radix_agg(radix);
            let col: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 10, 30, 20]));
            let ids = gbh.get_group_ids(&[col]).unwrap();
            assert_eq!(ids, vec![0, 1, 0, 2, 1]);
            assert_eq!(gbh.num_groups(), 3);
            // build_group_arrays must emit the keys in insertion order.
            let arrays = gbh.build_group_arrays().unwrap();
            assert_eq!(arrays.len(), 1);
            let arr = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64Array");
            assert_eq!(arr.values(), &[10, 20, 30]);
        }
    }

    #[test]
    fn bigint_fast_path_handles_null_keys() {
        for radix in [false, true] {
            let mut gbh = GroupByHash::with_radix_agg(radix);
            let col: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(1), None]));
            let ids = gbh.get_group_ids(&[col]).unwrap();
            assert_eq!(ids[0], ids[2]);
            assert_eq!(ids[1], ids[3]);
            assert_ne!(ids[0], ids[1]);
            assert_eq!(gbh.num_groups(), 2);
            let arrays = gbh.build_group_arrays().unwrap();
            let arr = arrays[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64Array");
            // The order is insertion order. ids[0] = first non-null group,
            // ids[1] = null group.
            assert!(!arr.is_null(ids[0] as usize));
            assert_eq!(arr.value(ids[0] as usize), 1);
            assert!(arr.is_null(ids[1] as usize));
        }
    }

    #[test]
    fn radix_agg_matches_single_table_group_ids() {
        let batches: Vec<Vec<Option<i64>>> = vec![
            vec![Some(10), Some(20), None, Some(10), Some(-1), Some(20)],
            vec![Some(30), None, Some(-1), Some(40), Some(10), Some(50)],
            (0..512)
                .map(|i| {
                    if i % 97 == 0 {
                        None
                    } else if i % 11 == 0 {
                        Some((i % 37) as i64)
                    } else {
                        Some(1_000 + i as i64)
                    }
                })
                .collect(),
        ];

        let mut single = GroupByHash::with_radix_agg(false);
        let mut radix = GroupByHash::with_radix_agg(true);

        for batch in batches {
            let single_col: ArrayRef = Arc::new(Int64Array::from(batch.clone()));
            let radix_col: ArrayRef = Arc::new(Int64Array::from(batch));
            let single_ids = single.get_group_ids(&[single_col]).unwrap();
            let radix_ids = radix.get_group_ids(&[radix_col]).unwrap();
            assert_eq!(radix_ids, single_ids);
        }

        assert_eq!(radix.num_groups(), single.num_groups());
        let single_arrays = single.build_group_arrays().unwrap();
        let radix_arrays = radix.build_group_arrays().unwrap();
        assert_eq!(radix_arrays.len(), single_arrays.len());
        let single_arr = single_arrays[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        let radix_arr = radix_arrays[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        assert_eq!(radix_arr.len(), single_arr.len());
        for i in 0..single_arr.len() {
            assert_eq!(radix_arr.is_null(i), single_arr.is_null(i));
            if !single_arr.is_null(i) {
                assert_eq!(radix_arr.value(i), single_arr.value(i));
            }
        }
    }

    #[test]
    fn bigint_prefetch_matches_hashbrown_for_adversarial_batches() {
        let state =
            RandomState::with_seeds(STATE_SEEDS.0, STATE_SEEDS.1, STATE_SEEDS.2, STATE_SEEDS.3);
        // Select distinct values with identical low hash bits. Every custom
        // table size used below has a mask no wider than these 11 bits, so
        // these values exercise a single linear-probe collision cluster.
        let mut colliding = Vec::new();
        let mut candidate = i64::MIN;
        while colliding.len() < 96 {
            if state.hash_one(candidate) & 0x7ff == 0 {
                colliding.push(candidate);
            }
            candidate = candidate.wrapping_add(1);
        }

        let mut seed = 0xD1B5_4A32_D192_ED03u64;
        let mut batches = Vec::new();
        for batch_index in 0..4 {
            let mut batch = Vec::new();
            for row in 0..192 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let value = if row % 29 == 0 {
                    None
                } else if row % 5 == 0 {
                    Some(colliding[(row + batch_index * 17) % colliding.len()])
                } else if row % 7 == 0 {
                    Some((row % 13) as i64 - 6)
                } else {
                    Some((seed >> 1) as i64)
                };
                batch.push(value);
            }
            batches.push(batch);
        }

        let mut hashbrown = GroupByHash::with_agg_prefetch_for_test(false);
        let mut prefetch = GroupByHash::with_agg_prefetch_for_test(true);
        for batch in batches {
            let off_col: ArrayRef = Arc::new(Int64Array::from(batch.clone()));
            let on_col: ArrayRef = Arc::new(Int64Array::from(batch));
            assert_eq!(
                prefetch.get_group_ids(&[on_col]).unwrap(),
                hashbrown.get_group_ids(&[off_col]).unwrap()
            );
        }
        assert_eq!(prefetch.num_groups(), hashbrown.num_groups());

        let off = hashbrown.build_group_arrays().unwrap();
        let on = prefetch.build_group_arrays().unwrap();
        assert_eq!(off[0].to_data(), on[0].to_data());
    }

    #[test]
    fn bigint_prefetch_grouped_sum_matches_hashbrown() {
        let key_batches: Vec<Vec<Option<i64>>> = vec![
            vec![Some(9), None, Some(4), Some(9), Some(-2), None],
            vec![Some(4), Some(11), Some(-2), None, Some(9), Some(11)],
            (0..257)
                .map(|row| {
                    if row % 31 == 0 {
                        None
                    } else {
                        Some(((row * 37) % 83) as i64 - 41)
                    }
                })
                .collect(),
        ];
        let value_batches: Vec<Vec<i64>> = key_batches
            .iter()
            .enumerate()
            .map(|(batch, keys)| {
                keys.iter()
                    .enumerate()
                    .map(|(row, _)| (batch as i64 + 1) * 1000 - row as i64 * 3)
                    .collect()
            })
            .collect();

        let run_sum = |prefetch_enabled: bool| {
            let mut group_by = GroupByHash::with_agg_prefetch_for_test(prefetch_enabled);
            let mut sum = GroupedSumAccumulator::new();
            for (keys, values) in key_batches.iter().zip(&value_batches) {
                let column: ArrayRef = Arc::new(Int64Array::from(keys.clone()));
                let group_ids = group_by.get_group_ids(&[column]).unwrap();
                sum.ensure_capacity(group_by.num_groups());
                let values: ArrayRef = Arc::new(Int64Array::from(values.clone()));
                sum.add_input(&group_ids, &values).unwrap();
            }
            let sums = (0..group_by.num_groups())
                .map(|group_id| sum.evaluate(group_id as u32).unwrap())
                .collect::<Vec<_>>();
            (group_by.build_group_arrays().unwrap()[0].to_data(), sums)
        };

        assert_eq!(run_sum(true), run_sum(false));
    }

    #[test]
    fn capacity_hint_preserves_bigint_group_ids_and_arrays() {
        let batches: Vec<Vec<Option<i64>>> = vec![
            vec![Some(7), Some(8), None, Some(7), Some(9), None],
            vec![Some(8), Some(10), Some(7), None, Some(11)],
            vec![Some(12), Some(13), Some(12), Some(8), Some(14)],
        ];

        for radix in [false, true] {
            for hinted_capacity in [Some(2), Some(64)] {
                let mut baseline = GroupByHash::with_radix_agg(radix);
                let mut hinted = GroupByHash::with_capacity_hint_for_test(radix, hinted_capacity);

                for batch in &batches {
                    let baseline_col: ArrayRef = Arc::new(Int64Array::from(batch.clone()));
                    let hinted_col: ArrayRef = Arc::new(Int64Array::from(batch.clone()));
                    let baseline_ids = baseline.get_group_ids(&[baseline_col]).unwrap();
                    let hinted_ids = hinted.get_group_ids(&[hinted_col]).unwrap();
                    assert_eq!(hinted_ids, baseline_ids);
                }

                assert_eq!(hinted.num_groups(), baseline.num_groups());
                let baseline_arrays = baseline.build_group_arrays().unwrap();
                let hinted_arrays = hinted.build_group_arrays().unwrap();
                let baseline_arr = baseline_arrays[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64Array");
                let hinted_arr = hinted_arrays[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64Array");
                assert_eq!(hinted_arr.len(), baseline_arr.len());
                for row in 0..baseline_arr.len() {
                    assert_eq!(hinted_arr.is_null(row), baseline_arr.is_null(row));
                    if !baseline_arr.is_null(row) {
                        assert_eq!(hinted_arr.value(row), baseline_arr.value(row));
                    }
                }
            }
        }
    }

    #[test]
    fn reserve_groups_preserves_distinct_bigint_groups() {
        const N: usize = 4096;
        for radix in [false, true] {
            let mut group_by = GroupByHash::with_radix_agg(radix);
            group_by.reserve_groups(N);
            let column: ArrayRef = Arc::new(Int64Array::from_iter_values(0..N as i64));
            let ids = group_by.get_group_ids(&[column]).unwrap();

            assert_eq!(group_by.num_groups(), N);
            assert!(group_by.storage_capacity() >= N);
            assert_eq!(ids, (0..N as u32).collect::<Vec<_>>());
        }
    }

    #[test]
    fn adaptive_reserve_preserves_results_and_keeps_low_cardinality_small() {
        let high_batches: Vec<Vec<i64>> = (0..8)
            .map(|batch| (0..4).map(|row| batch * 4 + row).collect())
            .collect();
        let low_batches: Vec<Vec<i64>> = (0..8)
            .map(|_| (0..32).map(|row| (row % 4) as i64).collect())
            .collect();

        let run = |batches: &[Vec<i64>], adaptive: bool| {
            let mut group_by = GroupByHash::with_capacity_hint_for_test(false, Some(4));
            let mut all_ids = Vec::new();
            let mut rows_seen = 0usize;
            for batch in batches {
                let column: ArrayRef = Arc::new(Int64Array::from(batch.clone()));
                all_ids.extend(group_by.get_group_ids(&[column]).unwrap());
                rows_seen += batch.len();
                if adaptive {
                    group_by.adaptive_reserve_after_batch_inner(rows_seen);
                }
            }
            (group_by, all_ids)
        };

        let (high_base, high_base_ids) = run(&high_batches, false);
        let (high_adaptive, high_adaptive_ids) = run(&high_batches, true);
        assert_eq!(high_adaptive_ids, high_base_ids);
        assert_eq!(high_adaptive.num_groups(), high_base.num_groups());
        assert!(high_adaptive.storage_capacity() >= high_base.storage_capacity());

        let (low_base, low_base_ids) = run(&low_batches, false);
        let (low_adaptive, low_adaptive_ids) = run(&low_batches, true);
        assert_eq!(low_adaptive_ids, low_base_ids);
        assert_eq!(low_adaptive.num_groups(), low_base.num_groups());
        assert!(low_adaptive.storage_capacity() <= 16);
    }

    #[test]
    fn flat_row_two_col_round_trips_and_dedups() {
        // Multi-col (Utf8, Int64) → FlatRow path. Verify ID stability,
        // dedup, and output decode-back.
        let mut gbh = GroupByHash::new();
        let c0: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a", "b", "c"]));
        let c1: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 1, 3, 2]));
        let ids = gbh.get_group_ids(&[c0, c1]).unwrap();
        assert_eq!(ids, vec![0, 1, 0, 2, 3]);
        assert_eq!(gbh.num_groups(), 4);
        let arrays = gbh.build_group_arrays().unwrap();
        assert_eq!(arrays.len(), 2);
        let s = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        let i = arrays[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        // Insertion order: (a,1), (b,2), (b,3), (c,2)
        let actual: Vec<(&str, i64)> = (0..4).map(|j| (s.value(j), i.value(j))).collect();
        assert_eq!(actual, vec![("a", 1), ("b", 2), ("b", 3), ("c", 2)]);
    }

    #[test]
    fn flat_row_reuses_ids_across_batches() {
        let mut gbh = GroupByHash::new();
        let c0a: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
        let c1a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let _ = gbh.get_group_ids(&[c0a, c1a]).unwrap();
        let c0b: ArrayRef = Arc::new(StringArray::from(vec!["b", "a", "c"]));
        let c1b: ArrayRef = Arc::new(Int64Array::from(vec![2, 1, 3]));
        let ids = gbh.get_group_ids(&[c0b, c1b]).unwrap();
        // (b,2)→1, (a,1)→0, (c,3)→2
        assert_eq!(ids, vec![1, 0, 2]);
        assert_eq!(gbh.num_groups(), 3);
    }

    #[test]
    fn build_group_arrays_generic_two_col() {
        let mut gbh = GroupByHash::new();
        let c0: ArrayRef = Arc::new(StringArray::from(vec!["a", "a", "b"]));
        let c1: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 1]));
        let _ = gbh.get_group_ids(&[c0, c1]).unwrap();
        let arrays = gbh.build_group_arrays().unwrap();
        assert_eq!(arrays.len(), 2);
        let s = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        let i = arrays[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        let actual: Vec<(&str, i64)> = (0..3).map(|j| (s.value(j), i.value(j))).collect();
        // Insertion order: ("a",1), ("a",2), ("b",1)
        assert_eq!(actual, vec![("a", 1), ("a", 2), ("b", 1)]);
    }

    // Isolated agg-throughput micro-benchmark (no scan/exchange/docker confound).
    // Run: cargo test -p arneb-execution --release group_by_throughput_bench -- --ignored --nocapture
    #[test]
    #[ignore]
    fn group_by_throughput_bench() {
        const TOTAL: usize = 60_000_000; // SF10 lineitem rows
        const BATCH: usize = 8192;
        const REPEAT: i64 = 4; // ~15M distinct l_orderkeys, each ~4x (TPC-H shape)

        // Pre-build the batches once (sequential orderkeys, each repeated REPEAT times).
        let mut batches: Vec<ArrayRef> = Vec::with_capacity(TOTAL / BATCH + 1);
        let mut row: usize = 0;
        while row < TOTAL {
            let n = BATCH.min(TOTAL - row);
            let vals: Vec<i64> = (0..n).map(|i| ((row + i) as i64) / REPEAT).collect();
            batches.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
            row += n;
        }

        for &radix in &[false, true] {
            let t = std::time::Instant::now();
            let mut gbh = GroupByHash::with_radix_agg(radix);
            for b in &batches {
                let cols = [Arc::clone(b)];
                let _ids = gbh.get_group_ids(&cols).unwrap();
            }
            let elapsed = t.elapsed();
            let mrows = TOTAL as f64 / elapsed.as_secs_f64() / 1e6;
            println!(
                "[AGGBENCH] radix={radix} groups={} elapsed={:.3}s throughput={:.1}M rows/s",
                gbh.num_groups(),
                elapsed.as_secs_f64(),
                mrows
            );
        }
    }
}
