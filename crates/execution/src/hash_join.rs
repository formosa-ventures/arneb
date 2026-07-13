//! Hash join operator and supporting hash table.

#[cfg(test)]
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use arneb_common::error::ExecutionError;
use arneb_common::stream::{collect_stream, stream_from_batches, SendableRecordBatchStream};
use arneb_common::types::ColumnInfo;
use arneb_planner::PlanExpr;
use arneb_sql_parser::ast;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, DictionaryArray, RecordBatch, UInt32Array,
};
use arrow::compute;
use arrow::compute::kernels::{boolean, cmp};
use arrow::datatypes::{self, DataType as ArrowDataType, Field, Schema, UInt32Type};
use async_trait::async_trait;

use crate::datasource::column_info_to_arrow_schema;
use crate::fast_hash::FastHasher;
use crate::memory_pool::{MemoryConsumer, MemoryReservation};
use crate::operator::{
    collect_probe_within_budget, collect_stream_pool_tracked, prepend_batches, ExecutionPlan,
    ProbeCollect,
};
use crate::partitioning::Partitioning;
use crate::repartition::HashPartitioner;
use crate::spill::{PartitionedSpillFile, PartitionedSpillWriter, SpillFile, SpillWriter};
use futures::StreamExt;

// ===========================================================================
// JoinHashMap
// ===========================================================================

/// Hash table mapping join-key hashes to build-side row indices, stored
/// as a flat open-chained layout rather than one heap `Vec` per key.
///
/// `head[slot]` is the first build row whose key-hash lands in `slot`
/// (or [`JoinHashMap::EMPTY`]); `next[row]` chains to the next build row
/// in the same slot. Probing walks the chain and the caller filters
/// slot/hash collisions with a real key comparison (`TypedKeys::row_eq`),
/// so distinct keys colliding into one slot are harmless.
///
/// This replaces the old `HashMap<u64, {first:u32, rest:Vec<u32>}>`: for
/// the typical TPC-H unique-key build that was ~47 *untracked* bytes/key
/// (a 24-byte empty `Vec` header per entry plus hashbrown overhead — the
/// q09 SF30 heap profile pinned the table at ~1.4 GB). The flat layout is
/// `(slots + rows) * 4` bytes (~12 B/row) and is pool-trackable via
/// [`JoinHashMap::heap_bytes`] so it counts toward the memory budget.
#[derive(Debug)]
pub(crate) struct JoinHashMap {
    /// `head[slot]` = first build row index in the slot's chain, or EMPTY.
    head: Vec<u32>,
    /// `next[row]` = next build row in the same chain, or EMPTY. NULL-key
    /// rows are never linked, so they never appear in any chain.
    next: Vec<u32>,
    /// `slots - 1`; `slots` is a power of two so `hash & mask` picks a slot.
    mask: u64,
}

impl JoinHashMap {
    /// Sentinel for "no row" in `head`/`next`. Build sides index rows with
    /// `u32` throughout the probe path, so a build with ≤ `u32::MAX` rows
    /// never produces this value as a real index.
    pub(crate) const EMPTY: u32 = u32::MAX;

    /// Build the hash table from one batch (concat the build side
    /// upstream when there are multiple). `key_indices` selects the
    /// join-key columns within `batch`.
    pub(crate) fn build_single(
        batch: &RecordBatch,
        key_indices: &[usize],
    ) -> Result<Self, ExecutionError> {
        let n_rows = batch.num_rows();
        // Power-of-two slot count ≥ rows → load factor in (0.5, 1.0], short
        // chains, and `hash & mask` slot selection. Chaining (not open
        // addressing) means a high load just lengthens chains slightly; the
        // caller's `row_eq` already filters collisions.
        let slots = n_rows.max(1).next_power_of_two();
        let mask = (slots - 1) as u64;
        let mut head = vec![Self::EMPTY; slots];
        let mut next = vec![Self::EMPTY; n_rows];

        // Typed-column hoist: downcast each key column once per batch.
        let typed = TypedKeys::new(batch, key_indices)?;
        for (row, next_slot) in next.iter_mut().enumerate() {
            if typed.row_has_null(row) {
                // A NULL key equi-matches nothing — leave it unlinked.
                continue;
            }
            let slot = (typed.hash_row(row) & mask) as usize;
            *next_slot = head[slot];
            head[slot] = row as u32;
        }

        Ok(Self { head, next, mask })
    }

    /// An empty table — every probe misses. Used for the no-rows build side
    /// (e.g. an empty right partition) without allocating per-key state.
    pub(crate) fn empty() -> Self {
        Self {
            head: vec![Self::EMPTY; 1],
            next: Vec::new(),
            mask: 0,
        }
    }

    /// First build row in the chain for `hash`, or [`Self::EMPTY`] when the
    /// slot is empty. Walk the chain with [`Self::chain_next`].
    #[inline]
    pub(crate) fn chain_head(&self, hash: u64) -> u32 {
        // `mask == head.len() - 1`, so the index is always in bounds.
        self.head[(hash & self.mask) as usize]
    }

    /// Next build row in the same chain as `row`, or [`Self::EMPTY`].
    #[inline]
    pub(crate) fn chain_next(&self, row: u32) -> u32 {
        self.next[row as usize]
    }

    /// Heap bytes held by the flat arrays. Accessor for the pool-tracking
    /// follow-up: the flat table is small and deterministic, so the build
    /// reservation can register it (the old hashbrown table was untracked).
    #[allow(dead_code)] // wired in by the hash-map pool-track follow-up
    pub(crate) fn heap_bytes(&self) -> usize {
        (self.head.len() + self.next.len()) * std::mem::size_of::<u32>()
    }
}

#[cfg(test)]
impl JoinHashMap {
    /// Slot-array length (power of two). Test-only.
    fn head_len(&self) -> usize {
        self.head.len()
    }

    /// Every build row reachable by walking all slot chains. Test-only:
    /// asserts NULL-key rows stay unlinked and each non-null row is linked
    /// exactly once.
    fn reachable_rows(&self) -> Vec<u32> {
        let mut out = Vec::new();
        for &h in &self.head {
            let mut r = h;
            while r != Self::EMPTY {
                out.push(r);
                r = self.next[r as usize];
            }
        }
        out
    }
}

// ===========================================================================
// MultiBatchBuild — concat-free build for a large multi-batch build side.
// ===========================================================================
//
// q09 SF30 OOM root cause (2026-06-11, inuse_space heap profile): the grace
// single-build path concatenated the entire ~5 GB build into ONE Arrow batch
// (`right_combined = concat_batches(batches)`, hash_join.rs:3285). `concat`
// holds the input batches AND the output batch live simultaneously → a ~2×
// spike (~10 GB) that hit the 11 GB cgroup cap and OOM-killed the worker.
// `arrow_select::concat::concat_primitives/dictionaries` was 5.3 GB of the
// 6.9 GB live peak; the JoinHashMap (hashbrown) was ~1.4 GB more.
//
// MultiBatchBuild builds the hash table over the build batches IN PLACE (no
// concat), in the same flat open-chained layout as JoinHashMap. Chains index
// a GLOBAL build-row id across all batches; `offsets` maps a global id back to
// `(batch, row)`. The probe gathers the right columns with `compute::interleave`
// across the batches, so the wide payload never doubles. Used for the INNER,
// no-residual grace single-build path (q09's shape); other join shapes keep the
// single-batch concat path.

/// Build-side hash table over multiple build batches without concatenating
/// them into one batch. Same flat `head`/`next` layout as [`JoinHashMap`] (see
/// its doc) but chains index a global build-row id; [`MultiBatchBuild::locate`]
/// maps it back to `(batch, row)`.
pub(crate) struct MultiBatchBuild {
    batches: Vec<RecordBatch>,
    /// Cumulative per-batch row offsets, `len == batches.len() + 1`.
    /// `offsets[b]` is the first global row id of batch `b`; the last entry is
    /// the total build-row count.
    offsets: Vec<usize>,
    /// `head[slot]` = first global build-row id in the slot chain, or EMPTY.
    head: Vec<u32>,
    /// `next[global]` = next global build-row id in the chain, or EMPTY.
    /// NULL-key rows are never linked.
    next: Vec<u32>,
    /// `tags[global]` = low 8 bits of the build row hash, parallel to `next`.
    tags: Vec<u8>,
    /// True when any hash bucket has more than one linked build row.
    has_collisions: bool,
    /// `slots - 1` (power-of-two slot count).
    mask: u64,
    key_indices: Vec<usize>,
}

/// Runtime knob (bytes) for [`coalesce_build_batches`]. Default 128 MiB; `0`
/// disables coalescing. See [`coalesce_build_batches`] for why this is the
/// q07/deep-join latency lever.
fn build_coalesce_bytes() -> usize {
    std::env::var("ARNEB_BUILD_COALESCE_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(128 * 1024 * 1024)
}

fn dict_probe_build_enabled() -> bool {
    #[cfg(test)]
    {
        match DICT_PROBE_BUILD_TEST_OVERRIDE.load(Ordering::SeqCst) {
            1 => return false,
            2 => return true,
            _ => {}
        }
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_DICT_PROBE_BUILD")
            .ok()
            .is_some_and(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "on" | "ON"));
        tracing::info!(
            target: "arneb::profile",
            knob = "ARNEB_DICT_PROBE_BUILD",
            enabled,
            "HashJoinExec dict probe-build gate"
        );
        enabled
    })
}

#[cfg(test)]
static DICT_PROBE_BUILD_TEST_OVERRIDE: AtomicU8 = AtomicU8::new(0);

#[cfg(test)]
fn set_dict_probe_build_for_test(enabled: Option<bool>) {
    let value = match enabled {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    };
    DICT_PROBE_BUILD_TEST_OVERRIDE.store(value, Ordering::SeqCst);
}

/// Concatenate adjacent build batches into chunks of at least `target_bytes`
/// (Arrow memory size) before building a [`MultiBatchBuild`].
///
/// The multi-batch probe ([`probe_one_left_batch_multi_inner`]) is
/// O(n_build_batches) PER LEFT BATCH: it rebuilds every build batch's
/// `TypedKeys` and `compute::interleave`s across every build batch on each
/// probe batch. A SF30 deep-join build arrives as ~20K tiny (~1K-row) exchange
/// batches, so that term dominated the probe (profiled q07: 112 s for a 27M-row
/// probe at 20,664 build batches). Coalescing into a handful of large chunks
/// cuts the batch count ~1000x. Unlike the single-batch concat path (which
/// doubled q09's 5 GB build live and hit the cgroup cap), this is BOUNDED: the
/// transient peak is one chunk (`target_bytes`), so it keeps the concat-free
/// memory win while removing the per-batch latency tax. `target_bytes == 0` or
/// ≤1 input batch returns the input unchanged.
fn coalesce_build_batches(
    batches: Vec<RecordBatch>,
    target_bytes: usize,
) -> Result<Vec<RecordBatch>, ExecutionError> {
    if target_bytes == 0 || batches.len() <= 1 {
        return Ok(batches);
    }
    let schema = batches[0].schema();
    let mut out: Vec<RecordBatch> = Vec::new();
    let mut group: Vec<RecordBatch> = Vec::new();
    let mut group_bytes = 0usize;
    for b in batches {
        if b.num_rows() == 0 {
            continue;
        }
        group_bytes += b
            .columns()
            .iter()
            .map(|c| c.get_array_memory_size())
            .sum::<usize>();
        group.push(b);
        if group_bytes >= target_bytes {
            out.push(if group.len() == 1 {
                group.pop().unwrap()
            } else {
                compute::concat_batches(&schema, group.iter())?
            });
            group.clear();
            group_bytes = 0;
        }
    }
    if !group.is_empty() {
        out.push(if group.len() == 1 {
            group.pop().unwrap()
        } else {
            compute::concat_batches(&schema, group.iter())?
        });
    }
    Ok(out)
}

impl MultiBatchBuild {
    /// Build over `batches` (no full concat). `key_indices` selects the join-key
    /// columns. Rows with a NULL key are skipped (never equi-match), matching
    /// `JoinHashMap::build_single`. Bounded-coalesces tiny input batches first
    /// (see [`coalesce_build_batches`]) so the probe's per-batch O(n_batches)
    /// term stays small.
    pub(crate) fn build(
        batches: Vec<RecordBatch>,
        key_indices: &[usize],
    ) -> Result<Self, ExecutionError> {
        let n_in = batches.len();
        let batches = coalesce_build_batches(batches, build_coalesce_bytes())?;
        if n_in != batches.len() {
            tracing::debug!(
                target: "arneb::profile",
                op = "MultiBatchBuild.coalesce",
                batches_in = n_in,
                batches_out = batches.len(),
                "coalesced build batches",
            );
        }
        let mut offsets = Vec::with_capacity(batches.len() + 1);
        let mut acc = 0usize;
        offsets.push(0);
        for batch in &batches {
            acc += batch.num_rows();
            offsets.push(acc);
        }
        let n_rows = acc;
        let slots = n_rows.max(1).next_power_of_two();
        let mask = (slots - 1) as u64;
        let mut head = vec![JoinHashMap::EMPTY; slots];
        let mut next = vec![JoinHashMap::EMPTY; n_rows];
        let mut tags = vec![0; n_rows];
        let mut has_collisions = false;

        for (b, batch) in batches.iter().enumerate() {
            let typed = TypedKeys::new(batch, key_indices)?;
            let base = offsets[b] as u32;
            for row in 0..batch.num_rows() {
                if typed.row_has_null(row) {
                    continue;
                }
                let g = base + row as u32;
                let hash = typed.hash_row(row);
                let slot = (hash & mask) as usize;
                tags[g as usize] = hash as u8;
                has_collisions |= head[slot] != JoinHashMap::EMPTY;
                next[g as usize] = head[slot];
                head[slot] = g;
            }
        }
        Ok(Self {
            batches,
            offsets,
            head,
            next,
            tags,
            has_collisions,
            mask,
            key_indices: key_indices.to_vec(),
        })
    }

    /// Total build rows across all batches.
    pub(crate) fn total_rows(&self) -> usize {
        *self.offsets.last().unwrap_or(&0)
    }

    /// Number of columns on the build (right) side.
    fn num_columns(&self) -> usize {
        self.batches.first().map(|b| b.num_columns()).unwrap_or(0)
    }

    /// First global build-row id in the chain for `hash`, or [`JoinHashMap::EMPTY`].
    #[inline]
    fn chain_head(&self, hash: u64) -> u32 {
        self.head[(hash & self.mask) as usize]
    }

    /// Next global build-row id in the same chain, or [`JoinHashMap::EMPTY`].
    #[inline]
    fn chain_next(&self, g: u32) -> u32 {
        self.next[g as usize]
    }

    /// Low-8-bit hash tag for a global build-row id.
    #[inline]
    fn tag(&self, g: u32) -> u8 {
        self.tags[g as usize]
    }

    /// Map a global build-row id to `(batch, row)`.
    #[inline]
    fn locate(&self, g: u32) -> (usize, usize) {
        let g = g as usize;
        let b = self.offsets.partition_point(|&o| o <= g) - 1;
        (b, g - self.offsets[b])
    }

    /// Software-prefetch the `head` slot a probe of `hash` will read, to hide
    /// the random-access cache miss. Cell-safe: a CPU hint, no result effect.
    /// No-op on non-x86_64 (stable Rust has no portable prefetch intrinsic).
    // Crate is `#![deny(unsafe_code)]`; this one CPU-hint intrinsic is the sole,
    // bounded exception (in-bounds pointer, prefetch never dereferences).
    #[allow(unsafe_code)]
    #[inline]
    fn prefetch_slot(&self, hash: u64) {
        #[cfg(target_arch = "x86_64")]
        {
            let slot = (hash & self.mask) as usize;
            // SAFETY: `slot` is masked into `0..=mask` and `head.len() == mask + 1`,
            // so the pointer is in-bounds; `_mm_prefetch` only hints, never deref.
            unsafe {
                core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(
                    self.head.as_ptr().add(slot) as *const i8,
                );
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            let _ = hash;
        }
    }

    /// Heap bytes held by the flat arrays + offsets. Accessor for the
    /// pool-tracking follow-up (see [`JoinHashMap::heap_bytes`]).
    #[allow(dead_code)] // wired in by the hash-map pool-track follow-up
    pub(crate) fn heap_bytes(&self) -> usize {
        (self.head.len() + self.next.len()) * std::mem::size_of::<u32>()
            + self.tags.len() * std::mem::size_of::<u8>()
            + self.offsets.len() * std::mem::size_of::<usize>()
    }
}

/// INNER probe of one left batch against a [`MultiBatchBuild`]. Output rows
/// are gathered with `compute::take` (left — one batch) + `compute::interleave`
/// (right — across the build batches), so the wide build never concatenates.
/// INNER + no-residual only (the grace single-build shape); returns `None`
/// when no rows match. `output_schema` is the joined left++right schema.
fn probe_one_left_batch_multi_inner(
    left_batch: &RecordBatch,
    build: &MultiBatchBuild,
    left_keys: &[usize],
    output_schema: &Arc<Schema>,
) -> Result<Option<RecordBatch>, ExecutionError> {
    let left_rows = left_batch.num_rows();
    if left_rows == 0 || build.batches.is_empty() {
        return Ok(None);
    }
    let left_typed = TypedKeys::new(left_batch, left_keys)?;
    // Per-batch build key columns. Rebuilt per left batch (cheap downcasts)
    // so the caller can hold `build` across an async-stream yield without a
    // self-referential `TypedKeys` borrow.
    let build_typed: Vec<TypedKeys<'_>> = build
        .batches
        .iter()
        .map(|b| TypedKeys::new(b, &build.key_indices))
        .collect::<Result<_, _>>()?;

    let mut cand_left: Vec<u32> = Vec::new();
    let mut cand_right: Vec<(usize, usize)> = Vec::new();
    // Precompute hashes when EITHER vectorized hashing OR prefetch is on —
    // prefetch needs the look-ahead row's hash available up front.
    let prefetch = probe_prefetch_enabled();
    let vec_probe = vec_probe_enabled() && vec_probe_key_types_supported(left_batch, left_keys);
    let left_hashes = (vec_probe || vectorized_probe_enabled() || prefetch)
        .then(|| left_typed.hash_batch(left_rows));
    // Software-prefetch distance: how many probe rows ahead to warm the build
    // hash slot. 8 hides the ~hundred-cycle cache miss without over-fetching.
    const PREFETCH_DIST: usize = 8;
    if vec_probe {
        for l_row in 0..left_rows {
            if prefetch {
                if let Some(hashes) = left_hashes.as_ref() {
                    let pf = l_row + PREFETCH_DIST;
                    if pf < left_rows {
                        build.prefetch_slot(hashes[pf]);
                    }
                }
            }
            if left_typed.row_has_null(l_row) {
                continue;
            }
            let hash = left_hashes
                .as_ref()
                .map_or_else(|| left_typed.hash_row(l_row), |hashes| hashes[l_row]);
            let tag = hash as u8;
            let mut g = build.chain_head(hash);
            if !build.has_collisions {
                if g != JoinHashMap::EMPTY && build.tag(g) == tag {
                    let (b, r) = build.locate(g);
                    cand_left.push(l_row as u32);
                    cand_right.push((b, r));
                }
                continue;
            }
            while g != JoinHashMap::EMPTY {
                if build.tag(g) == tag {
                    let (b, r) = build.locate(g);
                    cand_left.push(l_row as u32);
                    cand_right.push((b, r));
                }
                g = build.chain_next(g);
            }
        }
        if cand_left.is_empty() {
            return Ok(None);
        }

        let mut eq_mask: Option<BooleanArray> = None;
        let cand_left_idx = UInt32Array::from(cand_left.clone());
        for (&left_key, &right_key) in left_keys.iter().zip(build.key_indices.iter()) {
            let left_key_values = compute::take(left_batch.column(left_key), &cand_left_idx, None)?;
            let right_arrays: Vec<&dyn Array> = build
                .batches
                .iter()
                .map(|b| b.column(right_key).as_ref())
                .collect();
            let right_key_values = compute::interleave(&right_arrays, &cand_right)?;
            let l: &dyn arrow::array::Datum = &left_key_values;
            let r: &dyn arrow::array::Datum = &right_key_values;
            let key_eq = cmp::eq(l, r)?;
            eq_mask = Some(match eq_mask {
                Some(prev) => boolean::and(&prev, &key_eq)?,
                None => key_eq,
            });
        }
        if let Some(mask) = eq_mask {
            let mut matched_left = Vec::with_capacity(cand_left.len());
            let mut matched_right = Vec::with_capacity(cand_right.len());
            for i in 0..mask.len() {
                if !mask.is_null(i) && mask.value(i) {
                    matched_left.push(cand_left[i]);
                    matched_right.push(cand_right[i]);
                }
            }
            cand_left = matched_left;
            cand_right = matched_right;
        }
    } else {
        for l_row in 0..left_rows {
            if prefetch {
                if let Some(hashes) = left_hashes.as_ref() {
                    let pf = l_row + PREFETCH_DIST;
                    if pf < left_rows {
                        build.prefetch_slot(hashes[pf]);
                    }
                }
            }
            if left_typed.row_has_null(l_row) {
                continue;
            }
            let hash = left_hashes
                .as_ref()
                .map_or_else(|| left_typed.hash_row(l_row), |hashes| hashes[l_row]);
            let mut g = build.chain_head(hash);
            while g != JoinHashMap::EMPTY {
                let (b, r) = build.locate(g);
                if left_typed.row_eq(l_row, &build_typed[b], r) {
                    cand_left.push(l_row as u32);
                    cand_right.push((b, r));
                }
                g = build.chain_next(g);
            }
        }
    }
    if cand_left.is_empty() {
        return Ok(None);
    }

    let left_idx = UInt32Array::from(cand_left);
    let mut columns: Vec<ArrayRef> =
        Vec::with_capacity(left_batch.num_columns() + build.num_columns());
    for col_i in 0..left_batch.num_columns() {
        columns.push(compute::take(left_batch.column(col_i), &left_idx, None)?);
    }
    // Right columns: interleave across the build batches (no concat → no
    // input+output doubling on the wide payload).
    for col_i in 0..build.num_columns() {
        let arrays: Vec<&dyn Array> = build
            .batches
            .iter()
            .map(|b| b.column(col_i).as_ref())
            .collect();
        columns.push(compute::interleave(&arrays, &cand_right)?);
    }
    Ok(Some(RecordBatch::try_new(output_schema.clone(), columns)?))
}

fn vec_probe_key_types_supported(batch: &RecordBatch, key_indices: &[usize]) -> bool {
    key_indices.iter().all(|&i| {
        !matches!(
            batch.column(i).data_type(),
            ArrowDataType::Float32 | ArrowDataType::Float64
        )
    })
}

/// Pre-downcasted key columns for one batch. Lifts the data-type
/// match + downcast OUT of the per-row hot loop. Reduces probe-loop
/// per-row work from `O(keys × type_match + downcast + value)` to
/// `O(keys × value)`.
///
/// Used by [`precompute_hashes`] (for hashing the probe-side rows up
/// front) and [`TypedKeys::row_eq`] (for fast probe-vs-build row
/// comparison). The build side gets the same treatment.
enum TypedCol<'a> {
    Int32(&'a arrow::array::Int32Array),
    Int64(&'a arrow::array::Int64Array),
    Utf8(&'a arrow::array::StringArray),
    Boolean(&'a BooleanArray),
    Float32(&'a arrow::array::Float32Array),
    Float64(&'a arrow::array::Float64Array),
    Date32(&'a arrow::array::Date32Array),
    Decimal128(&'a arrow::array::Decimal128Array),
}

impl<'a> TypedCol<'a> {
    fn from_array(arr: &'a ArrayRef) -> Result<Self, ExecutionError> {
        Ok(match arr.data_type() {
            ArrowDataType::Int32 => TypedCol::Int32(arr.as_primitive::<datatypes::Int32Type>()),
            ArrowDataType::Int64 => TypedCol::Int64(arr.as_primitive::<datatypes::Int64Type>()),
            ArrowDataType::Utf8 => TypedCol::Utf8(arr.as_string::<i32>()),
            ArrowDataType::Boolean => {
                TypedCol::Boolean(arr.as_any().downcast_ref::<BooleanArray>().unwrap())
            }
            ArrowDataType::Float32 => {
                TypedCol::Float32(arr.as_primitive::<datatypes::Float32Type>())
            }
            ArrowDataType::Float64 => {
                TypedCol::Float64(arr.as_primitive::<datatypes::Float64Type>())
            }
            ArrowDataType::Date32 => TypedCol::Date32(arr.as_primitive::<datatypes::Date32Type>()),
            ArrowDataType::Decimal128(_, _) => {
                TypedCol::Decimal128(arr.as_primitive::<datatypes::Decimal128Type>())
            }
            dt => {
                return Err(ExecutionError::InvalidOperation(format!(
                    "unsupported hash join key type: {dt:?}"
                )));
            }
        })
    }

    #[inline]
    fn is_null(&self, row: usize) -> bool {
        match self {
            TypedCol::Int32(a) => a.is_null(row),
            TypedCol::Int64(a) => a.is_null(row),
            TypedCol::Utf8(a) => a.is_null(row),
            TypedCol::Boolean(a) => a.is_null(row),
            TypedCol::Float32(a) => a.is_null(row),
            TypedCol::Float64(a) => a.is_null(row),
            TypedCol::Date32(a) => a.is_null(row),
            TypedCol::Decimal128(a) => a.is_null(row),
        }
    }

    #[inline]
    fn hash_row<H: std::hash::Hasher>(&self, row: usize, h: &mut H) {
        use std::hash::Hash;
        match self {
            TypedCol::Int32(a) => (a.value(row) as i64).hash(h),
            TypedCol::Int64(a) => a.value(row).hash(h),
            TypedCol::Utf8(a) => a.value(row).hash(h),
            TypedCol::Boolean(a) => a.value(row).hash(h),
            TypedCol::Float32(a) => a.value(row).to_bits().hash(h),
            TypedCol::Float64(a) => a.value(row).to_bits().hash(h),
            TypedCol::Date32(a) => a.value(row).hash(h),
            TypedCol::Decimal128(a) => a.value(row).hash(h),
        }
    }
}

/// Bundle of pre-downcasted key columns for one batch. Build via
/// [`TypedKeys::new`]; reuse across all rows in the batch.
struct TypedKeys<'a> {
    cols: Vec<TypedCol<'a>>,
}

impl<'a> TypedKeys<'a> {
    fn new(batch: &'a RecordBatch, key_indices: &[usize]) -> Result<Self, ExecutionError> {
        let cols = key_indices
            .iter()
            .map(|&i| TypedCol::from_array(batch.column(i)))
            .collect::<Result<_, _>>()?;
        Ok(Self { cols })
    }

    #[inline]
    fn row_has_null(&self, row: usize) -> bool {
        self.cols.iter().any(|c| c.is_null(row))
    }

    #[inline]
    fn hash_row(&self, row: usize) -> u64 {
        use std::hash::Hasher;
        let mut hasher = FastHasher::default();
        for c in &self.cols {
            c.hash_row(row, &mut hasher);
        }
        hasher.finish()
    }

    fn hash_batch(&self, n_rows: usize) -> Vec<u64> {
        use std::hash::{Hash, Hasher};

        if self.cols.len() == 1 {
            return match self.cols[0] {
                TypedCol::Int32(a) => (0..n_rows)
                    .map(|row| {
                        let mut hasher = FastHasher::default();
                        (a.value(row) as i64).hash(&mut hasher);
                        hasher.finish()
                    })
                    .collect(),
                TypedCol::Int64(a) => (0..n_rows)
                    .map(|row| {
                        let mut hasher = FastHasher::default();
                        a.value(row).hash(&mut hasher);
                        hasher.finish()
                    })
                    .collect(),
                TypedCol::Date32(a) => (0..n_rows)
                    .map(|row| {
                        let mut hasher = FastHasher::default();
                        a.value(row).hash(&mut hasher);
                        hasher.finish()
                    })
                    .collect(),
                _ => self.hash_batch_general(n_rows),
            };
        }

        self.hash_batch_general(n_rows)
    }

    fn hash_batch_general(&self, n_rows: usize) -> Vec<u64> {
        use std::hash::{Hash, Hasher};

        let mut hashers: Vec<FastHasher> = (0..n_rows).map(|_| FastHasher::default()).collect();
        for c in &self.cols {
            match c {
                TypedCol::Int32(a) => {
                    for (row, hasher) in hashers.iter_mut().enumerate() {
                        (a.value(row) as i64).hash(hasher);
                    }
                }
                TypedCol::Int64(a) => {
                    for (row, hasher) in hashers.iter_mut().enumerate() {
                        a.value(row).hash(hasher);
                    }
                }
                TypedCol::Utf8(a) => {
                    for (row, hasher) in hashers.iter_mut().enumerate() {
                        a.value(row).hash(hasher);
                    }
                }
                TypedCol::Boolean(a) => {
                    for (row, hasher) in hashers.iter_mut().enumerate() {
                        a.value(row).hash(hasher);
                    }
                }
                TypedCol::Float32(a) => {
                    for (row, hasher) in hashers.iter_mut().enumerate() {
                        a.value(row).to_bits().hash(hasher);
                    }
                }
                TypedCol::Float64(a) => {
                    for (row, hasher) in hashers.iter_mut().enumerate() {
                        a.value(row).to_bits().hash(hasher);
                    }
                }
                TypedCol::Date32(a) => {
                    for (row, hasher) in hashers.iter_mut().enumerate() {
                        a.value(row).hash(hasher);
                    }
                }
                TypedCol::Decimal128(a) => {
                    for (row, hasher) in hashers.iter_mut().enumerate() {
                        a.value(row).hash(hasher);
                    }
                }
            }
        }
        hashers.into_iter().map(|h| h.finish()).collect()
    }

    /// Compare `row` of `self` to `other_row` of `other`. Returns true
    /// if every key column matches by value. Assumes type-aligned
    /// columns (which the planner guarantees via `extract_equi_join_keys`).
    #[inline]
    fn row_eq(&self, row: usize, other: &TypedKeys<'_>, other_row: usize) -> bool {
        for (a, b) in self.cols.iter().zip(other.cols.iter()) {
            if !typed_col_eq(a, row, b, other_row) {
                return false;
            }
        }
        true
    }
}

#[inline]
fn typed_col_eq(l: &TypedCol<'_>, l_row: usize, r: &TypedCol<'_>, r_row: usize) -> bool {
    match (l, r) {
        (TypedCol::Int32(a), TypedCol::Int32(b)) => a.value(l_row) == b.value(r_row),
        (TypedCol::Int64(a), TypedCol::Int64(b)) => a.value(l_row) == b.value(r_row),
        (TypedCol::Int32(a), TypedCol::Int64(b)) => i64::from(a.value(l_row)) == b.value(r_row),
        (TypedCol::Int64(a), TypedCol::Int32(b)) => a.value(l_row) == i64::from(b.value(r_row)),
        (TypedCol::Utf8(a), TypedCol::Utf8(b)) => a.value(l_row) == b.value(r_row),
        (TypedCol::Boolean(a), TypedCol::Boolean(b)) => a.value(l_row) == b.value(r_row),
        (TypedCol::Float32(a), TypedCol::Float32(b)) => {
            a.value(l_row).to_bits() == b.value(r_row).to_bits()
        }
        (TypedCol::Float64(a), TypedCol::Float64(b)) => {
            a.value(l_row).to_bits() == b.value(r_row).to_bits()
        }
        (TypedCol::Date32(a), TypedCol::Date32(b)) => a.value(l_row) == b.value(r_row),
        (TypedCol::Decimal128(a), TypedCol::Decimal128(b)) => a.value(l_row) == b.value(r_row),
        // Type mismatch — shouldn't occur for valid plans.
        _ => false,
    }
}

/// Maximum distinct values per dynamic filter. Matches Trino's
/// default. Step DF3 lifted this from the v1 1000-cap once
/// Per-type cap for dynamic-filter distinct-value collection. Bigger
/// caps unlock DF injection on deeper join chains (Q03/Q05/Q08/Q09/
/// Q10/Q12 in TPC-H all have build sides > 50K) but cost set-build
/// memory and time.
///
/// Sized for TPC-H SF1 worst cases:
/// - `Int64` (orderkey, custkey, partkey, suppkey, linenumber): 1M
///   handles `orders` (1.5M raw → ~650K post-filter on Q03) and
///   `lineitem` keys.
/// - `Int32`/`Date32` (smaller dimensions, dates): 200K is plenty.
///
/// `Utf8` is intentionally absent — string equality through HashSet
/// is fine at eval time, but collecting distinct strings has higher
/// memory overhead and most TPC-H filtering uses integer keys.
pub(crate) fn dynamic_filter_cap(dt: &ArrowDataType) -> Option<usize> {
    match dt {
        ArrowDataType::Int64 => Some(1_000_000),
        ArrowDataType::Int32 | ArrowDataType::Date32 => Some(200_000),
        _ => None,
    }
}

/// After the build-side hash table for an upper-level (probe-parallel)
/// join is materialised, derive `left_key IN (build-side-key values)`
/// filters and push them into the left subtree. The
/// `inject_dynamic_filter` trait method routes each filter to the
/// `ScanExec` whose schema contains the referenced column (via name).
///
/// Cap is per-type ([`dynamic_filter_cap`]) — if the build side has
/// more distinct values, skip emission for that column (no filter;
/// query continues normally).
///
/// Timing: caller is `ensure_built`, which is the single-flight
/// barrier that runs BEFORE `left.execute(partition)` for any probe
/// partition. So by the time `left.execute` triggers the downstream
/// chain, the dynamic filter has already been injected into every
/// matching `ScanExec.dynamic_filters` Mutex along the left subtree.
fn inject_inlist_dynamic_filters(
    left: &dyn ExecutionPlan,
    right_batch: &RecordBatch,
    right_keys: &[usize],
    left_keys: &[usize],
    left_schema: &[ColumnInfo],
    df_targets: &[Vec<usize>],
) {
    for (i, &right_col_idx) in right_keys.iter().enumerate() {
        if left_keys.get(i).is_none() {
            continue;
        }
        let arr = right_batch.column(right_col_idx);
        let Some(cap) = dynamic_filter_cap(arr.data_type()) else {
            continue;
        };
        let Some(values) = distinct_scalar_values(arr, cap) else {
            // Over cap — skip this column.
            continue;
        };
        if values.is_empty() {
            // Empty build side → could push `false` to scan, but the
            // existing single-flight build already handles empty
            // right via `handle_empty_right_partition`. Skip.
            continue;
        }
        let literals: Vec<PlanExpr> = values
            .into_iter()
            .map(|v| PlanExpr::Literal {
                value: v,
                span: None,
            })
            .collect();

        // Inject the build-key `IN (...)` filter at every probe-side column
        // join-EQUAL to this key — its equivalence class within the probe
        // subtree (`HashJoinExec::df_targets[i]`, computed at plan time via
        // `properties::equivalent_output_columns`). Routing is by INDEX
        // descent (`inject_dynamic_filter(filter, target_idx)`), never by
        // name: a self-join twin that merely shares the key's name (e.g.
        // TPC-H Q08 `n2.n_regionkey` vs the `n1.n_regionkey` key) is not a
        // class member and is never pruned, while a transitively-equal
        // cross-table sibling (Q18 `lineitem.l_orderkey`) is a member and
        // still collapses the probe build. This replaces the prior two
        // name-based injections (direct key + right-key "dual" sibling).
        let targets = df_targets.get(i).map(Vec::as_slice).unwrap_or(&[]);
        for &target_idx in targets {
            let name = left_schema
                .get(target_idx)
                .map(|c| c.name.clone())
                .unwrap_or_default();
            left.inject_dynamic_filter(
                PlanExpr::InList {
                    expr: Box::new(PlanExpr::Column {
                        index: target_idx,
                        name,
                        span: None,
                    }),
                    list: literals.clone(),
                    negated: false,
                    span: None,
                },
                target_idx,
            );
        }
    }
}

/// Returns up to `max` distinct non-null scalar values from a single
/// Arrow column. Returns `None` if the type is unsupported OR the
/// distinct count would exceed `max`. The check is best-effort: it
/// short-circuits as soon as the cap is exceeded so we don't pay for
/// hashing millions of rows when the result would be discarded.
/// Incremental DF (dynamic filter) collector — accumulates distinct
/// values from one column of incoming build batches across multiple
/// calls. Used by Grace Hash Join (Phase 3b.5e, 2026-05-21) so that
/// DF injection survives the loss of a fully-materialised
/// `right_combined`: the build pass routes rows into per-partition
/// spill files but also folds each batch's key values into this
/// collector. After build completes the collector's `finish_domain()`
/// is plugged into the same dynamic-filter injection/publish path.
#[derive(Debug)]
pub(crate) enum DfDistinctCollector {
    Int32 {
        set: crate::fast_hash::FastHashSet<i32>,
        cap: usize,
        bloom: Option<arneb_common::BloomFilter>,
    },
    Int64 {
        set: crate::fast_hash::FastHashSet<i64>,
        cap: usize,
        bloom: Option<arneb_common::BloomFilter>,
    },
    Date32 {
        set: crate::fast_hash::FastHashSet<i32>,
        cap: usize,
        bloom: Option<arneb_common::BloomFilter>,
    },
    /// Set grew past `cap` and bloom DF is enabled.
    Bloom(arneb_common::BloomFilter),
    /// Set grew past `cap` — DF gives up for this column.
    OverCap,
    /// Column type not supported by DF — skip.
    Unsupported,
}

impl DfDistinctCollector {
    /// Build a fresh collector for the given column type. Returns
    /// [`DfDistinctCollector::Unsupported`] for types DF doesn't know
    /// how to handle (Utf8, Bool, etc.).
    pub(crate) fn for_type(dt: &ArrowDataType, cap: usize) -> Self {
        let bloom = arneb_common::bloom_dynamic_filter_enabled()
            .then(arneb_common::BloomFilter::with_fixed_params);
        match dt {
            ArrowDataType::Int32 => DfDistinctCollector::Int32 {
                set: crate::fast_hash::FastHashSet::default(),
                cap,
                bloom,
            },
            ArrowDataType::Int64 => DfDistinctCollector::Int64 {
                set: crate::fast_hash::FastHashSet::default(),
                cap,
                bloom,
            },
            ArrowDataType::Date32 => DfDistinctCollector::Date32 {
                set: crate::fast_hash::FastHashSet::default(),
                cap,
                bloom,
            },
            _ => DfDistinctCollector::Unsupported,
        }
    }

    /// Accumulate values from `arr` into the collector. It keeps exact
    /// distinct values up to `cap`; with bloom DF enabled, over-cap input
    /// transitions to an incremental Bloom instead of dropping later rows.
    pub(crate) fn accumulate(&mut self, arr: &ArrayRef) {
        match self {
            DfDistinctCollector::Int32 { set, cap, bloom } => {
                let a = arr.as_primitive::<datatypes::Int32Type>();
                for i in 0..a.len() {
                    if a.is_null(i) {
                        continue;
                    }
                    let value = arneb_common::types::ScalarValue::Int32(a.value(i));
                    if let Some(bloom) = bloom.as_mut() {
                        bloom.insert(&value);
                    }
                    set.insert(a.value(i));
                    if set.len() > *cap {
                        *self = bloom
                            .take()
                            .map(DfDistinctCollector::Bloom)
                            .unwrap_or(DfDistinctCollector::OverCap);
                        self.accumulate(arr);
                        return;
                    }
                }
            }
            DfDistinctCollector::Int64 { set, cap, bloom } => {
                let a = arr.as_primitive::<datatypes::Int64Type>();
                for i in 0..a.len() {
                    if a.is_null(i) {
                        continue;
                    }
                    let value = arneb_common::types::ScalarValue::Int64(a.value(i));
                    if let Some(bloom) = bloom.as_mut() {
                        bloom.insert(&value);
                    }
                    set.insert(a.value(i));
                    if set.len() > *cap {
                        *self = bloom
                            .take()
                            .map(DfDistinctCollector::Bloom)
                            .unwrap_or(DfDistinctCollector::OverCap);
                        self.accumulate(arr);
                        return;
                    }
                }
            }
            DfDistinctCollector::Date32 { set, cap, bloom } => {
                let a = arr.as_primitive::<datatypes::Date32Type>();
                for i in 0..a.len() {
                    if a.is_null(i) {
                        continue;
                    }
                    let value = arneb_common::types::ScalarValue::Date32(a.value(i));
                    if let Some(bloom) = bloom.as_mut() {
                        bloom.insert(&value);
                    }
                    set.insert(a.value(i));
                    if set.len() > *cap {
                        *self = bloom
                            .take()
                            .map(DfDistinctCollector::Bloom)
                            .unwrap_or(DfDistinctCollector::OverCap);
                        self.accumulate(arr);
                        return;
                    }
                }
            }
            DfDistinctCollector::Bloom(bloom) => match arr.data_type() {
                ArrowDataType::Int32 => {
                    let a = arr.as_primitive::<datatypes::Int32Type>();
                    for i in 0..a.len() {
                        if !a.is_null(i) {
                            bloom.insert(&arneb_common::types::ScalarValue::Int32(a.value(i)));
                        }
                    }
                }
                ArrowDataType::Int64 => {
                    let a = arr.as_primitive::<datatypes::Int64Type>();
                    for i in 0..a.len() {
                        if !a.is_null(i) {
                            bloom.insert(&arneb_common::types::ScalarValue::Int64(a.value(i)));
                        }
                    }
                }
                ArrowDataType::Date32 => {
                    let a = arr.as_primitive::<datatypes::Date32Type>();
                    for i in 0..a.len() {
                        if !a.is_null(i) {
                            bloom.insert(&arneb_common::types::ScalarValue::Date32(a.value(i)));
                        }
                    }
                }
                _ => {}
            },
            DfDistinctCollector::OverCap | DfDistinctCollector::Unsupported => {}
        }
    }

    /// Finish collecting and produce a domain for DF publishing/injection.
    /// Empty exact sets remain `DistinctValues([])` so the cross-fragment
    /// partition counter advances. Over-cap collectors without bloom enabled
    /// degrade to `All`, matching `build_partition_domain_for_column`.
    pub(crate) fn finish_domain(self) -> arneb_common::Domain {
        use arneb_common::types::ScalarValue;
        match self {
            DfDistinctCollector::Int32 { set, .. } => arneb_common::Domain::DistinctValues(
                set.into_iter().map(ScalarValue::Int32).collect(),
            ),
            DfDistinctCollector::Int64 { set, .. } => arneb_common::Domain::DistinctValues(
                set.into_iter().map(ScalarValue::Int64).collect(),
            ),
            DfDistinctCollector::Date32 { set, .. } => arneb_common::Domain::DistinctValues(
                set.into_iter().map(ScalarValue::Date32).collect(),
            ),
            DfDistinctCollector::Bloom(bloom) => arneb_common::Domain::Bloom(bloom),
            DfDistinctCollector::OverCap | DfDistinctCollector::Unsupported => {
                arneb_common::Domain::All
            }
        }
    }
}

pub(crate) fn distinct_scalar_values(
    arr: &ArrayRef,
    max: usize,
) -> Option<Vec<arneb_common::types::ScalarValue>> {
    use crate::fast_hash::FastHashSet;
    use arneb_common::types::ScalarValue;
    if arr.is_empty() || arr.null_count() == arr.len() {
        return Some(Vec::new());
    }
    match arr.data_type() {
        ArrowDataType::Int32 => {
            let a = arr.as_primitive::<datatypes::Int32Type>();
            let mut set: FastHashSet<i32> = FastHashSet::default();
            for i in 0..a.len() {
                if a.is_null(i) {
                    continue;
                }
                set.insert(a.value(i));
                if set.len() > max {
                    return None;
                }
            }
            Some(set.into_iter().map(ScalarValue::Int32).collect())
        }
        ArrowDataType::Int64 => {
            let a = arr.as_primitive::<datatypes::Int64Type>();
            let mut set: FastHashSet<i64> = FastHashSet::default();
            for i in 0..a.len() {
                if a.is_null(i) {
                    continue;
                }
                set.insert(a.value(i));
                if set.len() > max {
                    return None;
                }
            }
            Some(set.into_iter().map(ScalarValue::Int64).collect())
        }
        ArrowDataType::Date32 => {
            let a = arr.as_primitive::<datatypes::Date32Type>();
            let mut set: FastHashSet<i32> = FastHashSet::default();
            for i in 0..a.len() {
                if a.is_null(i) {
                    continue;
                }
                set.insert(a.value(i));
                if set.len() > max {
                    return None;
                }
            }
            Some(set.into_iter().map(ScalarValue::Date32).collect())
        }
        _ => None,
    }
}

// HashJoinExec
// ===========================================================================

/// Hash join operator supporting INNER, LEFT, RIGHT, and FULL equi-joins.
///
/// Build side is always the right input. The build phase collects all right-side
/// batches and builds a hash table. The probe phase iterates over left-side rows,
/// looking up matches in the hash table.
#[derive(Debug)]
pub(crate) struct HashJoinExec {
    pub(crate) left: Arc<dyn ExecutionPlan>,
    pub(crate) right: Arc<dyn ExecutionPlan>,
    pub(crate) join_type: ast::JoinType,
    /// Column indices in the left input that form the join key.
    pub(crate) left_keys: Vec<usize>,
    /// Column indices in the right input that form the join key.
    pub(crate) right_keys: Vec<usize>,
    /// Optional non-equi predicate evaluated on each equi-match candidate
    /// before it is accepted. Column indices reference the joined layout
    /// (`left` columns followed by `right` columns).
    pub(crate) residual: Option<PlanExpr>,
    /// Lazy-built shared build-side state across probe partitions. The
    /// right input is collected and hashed exactly once; every probe
    /// partition shares a `Arc<BuildState>` via this cell. Empty until
    /// the first call to [`execute`].
    pub(crate) build_state: tokio::sync::OnceCell<Arc<BuildState>>,
    /// Peak bytes reserved by the build-side materialised state
    /// (mostly `right_combined`'s buffer memory). Populated by
    /// `ensure_built` once the build hash table is in place. Mirrors
    /// Trino's `HashBuilderOperator.java:329`
    /// `localUserMemoryContext.setBytes(partition.get().getInMemorySizeInBytes())`.
    pub(crate) peak_build_bytes: std::sync::atomic::AtomicUsize,
    /// Memory pool the build-side reservation is registered with.
    /// Phase 3a (2026-05-21): plumbed in; build phase calls
    /// `try_grow(build_bytes)` after collect. Returns
    /// `ResourceExhausted` instead of letting the kernel OOM-kill
    /// the worker. Phase 3b will replace fail-fast with on-disk
    /// chunked-with-spill build + multi-pass probe.
    pub(crate) memory_pool: Arc<dyn crate::memory_pool::MemoryPool>,
    /// A1.5 (2026-05-27): cross-fragment DF producers this join
    /// emits. Each entry says "I produce DF id X from build-side
    /// column at index `build_index`, which will be consumed by a
    /// probe-side scan downstream on a different fragment". Populated
    /// by the physical planner from `LogicalPlan::Join.dynamic_filter_ids`.
    /// Empty when the analyzer skipped this join (e.g. too-large build
    /// or non-eligible join type).
    pub(crate) dynamic_filter_producers: Vec<arneb_planner::DynamicFilterProducer>,
    /// A1.5: worker-side hook installed by `ExecutionContext`. `None`
    /// on coord / standalone / tests — the build phase then skips the
    /// Domain build entirely. Behind the `dynamic_filtering_enabled`
    /// flag on the operator.
    pub(crate) dynamic_filter_publisher: Option<crate::DynamicFilterPublisherRef>,
    /// A1.5: gate for the producer-side path, mirroring the consumer
    /// side flag. Same semantics: when `false`, the build phase skips
    /// the cross-fragment publish even if producers are annotated.
    pub(crate) dynamic_filtering_enabled: bool,
    /// Same-fragment dynamic-filter targets: `df_targets[k]` is the set of
    /// LEFT (probe) child output-schema column indices that are join-equal
    /// to `left_keys[k]` (its equivalence class within the probe subtree),
    /// computed at physical-planning time via
    /// `properties::equivalent_output_columns`. The build-side `IN (...)`
    /// filter for key `k` is injected ONLY at these indices (by index
    /// descent), never by column name — so a self-join twin that merely
    /// shares the key's name (e.g. TPC-H Q08 `n2.n_regionkey`) is never
    /// pruned, while a transitively-equal cross-table sibling (Q18
    /// `lineitem.l_orderkey`) still is. Empty → no same-fragment injection.
    pub(crate) df_targets: Vec<Vec<usize>>,
}

/// Build-side state shared across probe partitions. Materialised once
/// per `HashJoinExec` instance via [`HashJoinExec::ensure_built`].
///
/// Phase 3b (2026-05-21): two variants now exist. `Single` is the
/// existing fast path — one in-memory `right_combined` + one
/// `JoinHashMap`. `Multipass` carries chunks (some in memory, some
/// spilled to disk) for the case where the build side exceeded the
/// memory budget; the probe walks each chunk in turn.
#[derive(Debug)]
pub(crate) struct BuildState {
    pub(crate) side: BuildSide,
}

#[derive(Debug)]
pub(crate) enum BuildSide {
    /// All right batches fit within budget — concatenated to one
    /// RecordBatch + one JoinHashMap. Used by `probe` directly.
    Single {
        /// `None` if the right input produced no rows.
        right_combined: Option<RecordBatch>,
        /// Hash table over `right_combined`. Empty when None.
        hash_map: Arc<JoinHashMap>,
    },
    /// Build exceeded budget; chunks held individually. Probe must
    /// load each chunk, build its own JoinHashMap, scan left,
    /// concatenate outputs.
    ///
    /// `chunks` is held in an `Arc<Vec<_>>` so the streaming probe
    /// path (Phase 3b.4, 2026-05-21) can move ownership into the
    /// generator without violating the shared-build invariant that
    /// multiple probe partitions read the same chunks concurrently.
    Multipass {
        chunks: Arc<Vec<BuildChunk>>,
        right_schema: Arc<Schema>,
    },
    /// Grace Hash Join build (Phase 3b.5, 2026-05-21): build was
    /// hash-partitioned into `N` buckets, pre-built per-partition hash
    /// tables, and (optionally) per-partition spill files. Shareable
    /// across `N_probe` probe partitions — each probe partition reads
    /// the same `in_mem_hash_maps` (read-only after construction) and
    /// opens its own reader on `partitions.partition(p)` for spilled
    /// build partitions. Constructed once by `ensure_built` under the
    /// `OnceCell` single-flight guard.
    ///
    /// Phase M.2c (2026-05-22): replaces the per-partition independent
    /// Grace build (M.2b) that duplicated build work N times for the
    /// shared-build `execute()` path. With pre-built hash tables and
    /// Arc-shared spill files, all probe partitions share a single
    /// build phase — the structural fix that closes Q09 at 8 GB
    /// OrbStack (M.2b reduced Q05 peak 30% but Q09 still OOMed on
    /// 4× partsupp builds).
    Partitioned {
        /// Per-partition pre-built (RecordBatch, JoinHashMap) pairs.
        /// `None` slot means that partition's build was empty OR was
        /// spilled to disk (see `partitions`).
        in_mem_hash_maps: Arc<Vec<Option<(RecordBatch, JoinHashMap)>>>,
        /// Spilled per-partition files. Each probe partition opens its
        /// own reader via `.partition(p).open_reader()` (immutable —
        /// no `take_partition` in the shared path).
        partitions: Arc<PartitionedSpillFile>,
        right_schema: Arc<Schema>,
        n_partitions: usize,
    },
}

#[derive(Debug)]
pub(crate) enum BuildChunk {
    InMemory(Vec<RecordBatch>),
    Spilled(SpillFile),
}

/// Outcome of streaming a right-side build input through
/// [`build_with_spill`]. `Single` means everything fit within budget;
/// the caller can `concat_batches` + build a single `JoinHashMap` as
/// before. `Multipass` means at least one chunk spilled to disk; the
/// caller must walk chunks one at a time (`execute_multipass_inner`).
pub(crate) enum BuildChunksResult {
    Single {
        batches: Vec<RecordBatch>,
        /// `None` when the stream produced no batches at all.
        schema: Option<Arc<Schema>>,
        total_bytes: usize,
        total_rows: usize,
        /// Outstanding reservation for `batches`. Drops on scope exit;
        /// callers that need long-lived accounting must hold it.
        reservation: MemoryReservation,
    },
    Multipass {
        chunks: Vec<BuildChunk>,
        schema: Arc<Schema>,
        total_bytes: usize,
        total_rows: usize,
    },
    /// Grace Hash Join (Phase 3b.5, 2026-05-21) outcome of
    /// [`build_with_partitioned_spill`]: build was hash-partitioned;
    /// `in_mem[p]` holds surviving in-memory batches (or `None` if
    /// partition `p` was spilled); `partitions` holds the on-disk
    /// files for spilled partitions. Both sides reuse `partitioner`
    /// to keep the bucketing consistent.
    #[allow(dead_code)] // Wired in by Phase 3b.5d (execute_grace_inner)
    Partitioned {
        in_mem: Vec<Option<Vec<RecordBatch>>>,
        partitions: PartitionedSpillFile,
        schema: Arc<Schema>,
        total_bytes: usize,
        partition_rows: Vec<usize>,
        partitioner: Arc<HashPartitioner>,
    },
}

/// Stream a right-side input into the build side with on-disk spill on
/// memory pressure. Used by all three HashJoinExec build paths
/// (`ensure_built`, `execute_per_partition`, `execute_single`) — Phase
/// 3b.2 (2026-05-21) extracted this so the spill loop lives in one
/// place.
///
/// Algorithm: for each incoming batch, try to grow the build
/// reservation; if it fails, flush the currently in-memory batches to
/// a single spill chunk, shrink the reservation, then re-grow for the
/// new batch. Result is either `Single` (no spill) or `Multipass`
/// (spilled + maybe trailing in-memory chunk).
pub(crate) async fn build_with_spill(
    mut right_stream: SendableRecordBatchStream,
    memory_pool: Arc<dyn crate::memory_pool::MemoryPool>,
    consumer_name: &'static str,
) -> Result<BuildChunksResult, ExecutionError> {
    let consumer = MemoryConsumer::new(consumer_name).with_can_spill(true);
    let mut build_reservation: MemoryReservation = consumer.register(Arc::clone(&memory_pool));

    let mut in_mem: Vec<RecordBatch> = Vec::new();
    let mut spilled_chunks: Vec<SpillFile> = Vec::new();
    let mut right_schema: Option<Arc<Schema>> = None;
    let mut total_build_bytes: usize = 0;
    let mut total_build_rows: usize = 0;

    while let Some(batch_res) = right_stream.next().await {
        let batch = batch_res.map_err(|e| {
            ExecutionError::InvalidOperation(format!("hash join right collect: {e}"))
        })?;
        if right_schema.is_none() {
            right_schema = Some(batch.schema());
        }
        if batch.num_rows() == 0 {
            continue;
        }
        let batch_bytes = crate::operator::record_batch_bytes(&batch);
        total_build_bytes += batch_bytes;
        total_build_rows += batch.num_rows();

        match build_reservation.try_grow(batch_bytes) {
            Ok(()) => {
                in_mem.push(batch);
            }
            Err(_) => {
                if in_mem.is_empty() {
                    return Err(ExecutionError::ResourceExhausted(format!(
                        "single right batch ({batch_bytes} bytes) exceeds memory budget for {consumer_name}"
                    )));
                }
                let schema = right_schema.clone().expect("schema set above");
                let spilled_bytes: usize =
                    in_mem.iter().map(crate::operator::record_batch_bytes).sum();
                let mut writer = SpillWriter::new(schema, "hash_join_build")?;
                for b in &in_mem {
                    writer.write(b)?;
                }
                let file = writer.finish()?;
                tracing::info!(
                    target: "arneb::mem",
                    consumer = consumer_name,
                    chunk_idx = spilled_chunks.len(),
                    spilled_bytes,
                    n_batches = in_mem.len(),
                    path = %file.path().display(),
                    "build-side chunk spilled to disk",
                );
                spilled_chunks.push(file);
                in_mem.clear();
                build_reservation.shrink(spilled_bytes);
                build_reservation.try_grow(batch_bytes)?;
                in_mem.push(batch);
            }
        }
    }

    if !spilled_chunks.is_empty() {
        let schema = right_schema.expect("at least one batch spilled implies schema set");
        let mut chunks: Vec<BuildChunk> = Vec::with_capacity(spilled_chunks.len() + 1);
        for s in spilled_chunks {
            chunks.push(BuildChunk::Spilled(s));
        }
        if !in_mem.is_empty() {
            chunks.push(BuildChunk::InMemory(in_mem));
        }
        // Multipass probe re-reserves per chunk as it loads them back.
        build_reservation.free();
        Ok(BuildChunksResult::Multipass {
            chunks,
            schema,
            total_bytes: total_build_bytes,
            total_rows: total_build_rows,
        })
    } else {
        Ok(BuildChunksResult::Single {
            batches: in_mem,
            schema: right_schema,
            total_bytes: total_build_bytes,
            total_rows: total_build_rows,
            reservation: build_reservation,
        })
    }
}

/// Grace Hash Join build (Phase 3b.5c, 2026-05-21). Stream the right
/// input batch-by-batch, route each row to one of `partitioner.n` hash
/// partitions, and when memory exhausts spill the LARGEST in-memory
/// partition to disk. Returns [`BuildChunksResult::Single`] if the
/// input was trivially small (fits without spill), otherwise
/// [`BuildChunksResult::Partitioned`].
///
/// Algorithm matches Trino's `HashBuilderOperator` revocable-spill
/// pattern except we partition during the FIRST pass over the input
/// (since arneb doesn't have a pre-existing exchange to partition
/// upstream). The probe side reuses `partitioner` to align rows to
/// the same buckets — without this both sides would hash to different
/// partition numbers and matches would silently disappear.
/// Outcome of [`build_with_partitioned_spill`]. The build result plus
/// per-right-key DF collectors so callers can still inject dynamic
/// filters after partitioning (Phase 3b.5e, 2026-05-21).
pub(crate) struct PartitionedBuildOutcome {
    pub(crate) result: BuildChunksResult,
    pub(crate) df_collectors: Vec<DfDistinctCollector>,
}

#[allow(dead_code)] // Routed in by Phase 3b.5d (execute_grace_inner)
pub(crate) async fn build_with_partitioned_spill(
    right_stream: SendableRecordBatchStream,
    memory_pool: Arc<dyn crate::memory_pool::MemoryPool>,
    consumer_name: &'static str,
    partitioner: Arc<HashPartitioner>,
) -> Result<BuildChunksResult, ExecutionError> {
    // Compatibility wrapper for callers that don't need DF side-channel.
    // 3b.5e splits the work into a builder that ALSO collects DF and a
    // thin wrapper that throws it away.
    Ok(build_with_partitioned_spill_collecting_df(
        right_stream,
        memory_pool,
        consumer_name,
        partitioner,
        &[],
    )
    .await?
    .result)
}

/// Build + DF side-channel variant of [`build_with_partitioned_spill`].
/// `right_key_indices` selects which build columns to accumulate
/// distinct values for; pass `&[]` to skip collection.
#[allow(dead_code)] // Routed in by Phase 3b.5d (execute_grace_single)
pub(crate) async fn build_with_partitioned_spill_collecting_df(
    mut right_stream: SendableRecordBatchStream,
    memory_pool: Arc<dyn crate::memory_pool::MemoryPool>,
    consumer_name: &'static str,
    partitioner: Arc<HashPartitioner>,
    right_key_indices: &[usize],
) -> Result<PartitionedBuildOutcome, ExecutionError> {
    let n_partitions = partitioner.n_partitions();
    let consumer = MemoryConsumer::new(consumer_name).with_can_spill(true);
    let mut build_reservation: MemoryReservation = consumer.register(Arc::clone(&memory_pool));

    // Per-partition state. `in_mem[p] = None` means partition p has been
    // spilled and any further batches for it go straight to disk.
    let mut in_mem: Vec<Option<Vec<RecordBatch>>> =
        (0..n_partitions).map(|_| Some(Vec::new())).collect();
    let mut in_mem_bytes: Vec<usize> = vec![0usize; n_partitions];
    let mut partition_rows: Vec<usize> = vec![0usize; n_partitions];
    let mut right_schema: Option<Arc<Schema>> = None;
    let mut total_build_bytes: usize = 0;
    let mut spill_writer: Option<PartitionedSpillWriter> = None;

    // Phase 3b.5e: per-right-key DF collectors. Lazy-init on the first
    // batch so we can pick the right type per join key column.
    let mut df_collectors: Option<Vec<DfDistinctCollector>> = None;

    while let Some(batch_res) = right_stream.next().await {
        let batch = batch_res.map_err(|e| {
            ExecutionError::InvalidOperation(format!("hash join right collect: {e}"))
        })?;
        if right_schema.is_none() {
            right_schema = Some(batch.schema());
        }
        if batch.num_rows() == 0 {
            continue;
        }

        // Phase 3b.5e: fold this batch's distinct key values into the
        // side-channel collectors so DF still has data to inject after
        // the build is hash-partitioned (no fully-materialised
        // right_combined exists in Grace HJ).
        if !right_key_indices.is_empty() {
            let schema_ref = batch.schema();
            let collectors = df_collectors.get_or_insert_with(|| {
                right_key_indices
                    .iter()
                    .map(|&col_idx| {
                        let dt = schema_ref.field(col_idx).data_type();
                        DfDistinctCollector::for_type(dt, DEFAULT_DYNAMIC_FILTER_CAP_PRIMITIVE)
                    })
                    .collect()
            });
            for (i, &col_idx) in right_key_indices.iter().enumerate() {
                collectors[i].accumulate(batch.column(col_idx));
            }
        }

        // Compute partition assignment per row, then group rows into
        // per-partition sub-batches via `take`. This is the same shape
        // as `HashPartitioner::split`, inlined so we can interleave
        // memory budget checks per partition.
        let assignments = partitioner.assignments(&batch)?;
        let mut buckets: Vec<Vec<u32>> = (0..n_partitions).map(|_| Vec::new()).collect();
        for (row, &p) in assignments.iter().enumerate() {
            buckets[p as usize].push(row as u32);
        }

        let schema = batch.schema();
        for (p, indices) in buckets.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let idx_array = UInt32Array::from(indices);
            let columns: Vec<ArrayRef> = (0..batch.num_columns())
                .map(|i| compute::take(batch.column(i), &idx_array, None))
                .collect::<Result<_, _>>()?;
            let sub_batch = RecordBatch::try_new(schema.clone(), columns)?;
            let sub_bytes = crate::operator::record_batch_bytes(&sub_batch);
            total_build_bytes += sub_bytes;
            partition_rows[p] += sub_batch.num_rows();

            // If this partition already spilled, write directly.
            if in_mem[p].is_none() {
                let writer = spill_writer.get_or_insert_with(|| {
                    PartitionedSpillWriter::new(
                        schema.clone(),
                        n_partitions,
                        "hash_join_grace_build",
                    )
                });
                writer.write_partition(p, &sub_batch)?;
                continue;
            }

            // Otherwise try to keep in memory. On budget exhaustion,
            // spill the largest in-memory partition (possibly p itself,
            // possibly another), shrink reservation, then retry.
            loop {
                match build_reservation.try_grow(sub_bytes) {
                    Ok(()) => {
                        in_mem[p].as_mut().unwrap().push(sub_batch);
                        in_mem_bytes[p] += sub_bytes;
                        break;
                    }
                    Err(_) => {
                        // Pick the largest in-memory partition (by bytes)
                        // that still has batches.
                        let victim = in_mem
                            .iter()
                            .enumerate()
                            .filter_map(|(i, slot)| {
                                slot.as_ref()
                                    .filter(|v| !v.is_empty())
                                    .map(|_| (i, in_mem_bytes[i]))
                            })
                            .max_by_key(|(_, bytes)| *bytes)
                            .map(|(i, _)| i);
                        let Some(victim) = victim else {
                            return Err(ExecutionError::ResourceExhausted(format!(
                                "{consumer_name}: single sub-batch ({sub_bytes} bytes) exceeds memory budget; no partition to spill"
                            )));
                        };

                        let victim_bytes = in_mem_bytes[victim];
                        let victim_batches = in_mem[victim].take().unwrap();
                        let writer = spill_writer.get_or_insert_with(|| {
                            PartitionedSpillWriter::new(
                                schema.clone(),
                                n_partitions,
                                "hash_join_grace_build",
                            )
                        });
                        for b in &victim_batches {
                            writer.write_partition(victim, b)?;
                        }
                        tracing::info!(
                            target: "arneb::mem",
                            consumer = consumer_name,
                            victim_partition = victim,
                            spilled_bytes = victim_bytes,
                            n_batches = victim_batches.len(),
                            "grace build: spilled in-memory partition",
                        );
                        drop(victim_batches);
                        build_reservation.shrink(victim_bytes);
                        in_mem_bytes[victim] = 0;
                        // `in_mem[victim]` is now `None` (taken above), so
                        // future batches for it spill directly. Continue
                        // the loop — retry try_grow for the current
                        // sub_batch. If victim was p itself, the second
                        // iteration falls into the spilled-direct path.
                        if victim == p {
                            // Falls through: the outer loop's next iter
                            // will see in_mem[p] == None and go direct.
                            // But we still own `sub_batch` here — write
                            // it directly to avoid an extra iteration.
                            spill_writer
                                .as_mut()
                                .unwrap()
                                .write_partition(p, &sub_batch)?;
                            // total_build_bytes already includes sub_bytes;
                            // no reservation needed (we just spilled p).
                            break;
                        }
                        // Else loop again to retry try_grow.
                    }
                }
            }
        }
    }

    let any_spilled = in_mem.iter().any(|s| s.is_none());
    let df_collectors_final = df_collectors.unwrap_or_default();
    if !any_spilled {
        // Cache-fit decision (2026-05-30). The build fit entirely in
        // memory (nothing spilled). A SMALL build flattens to the
        // Single fast path — one `JoinHashMap`, no per-partition probe
        // routing overhead. A LARGE build flattened into one hash table
        // overflows CPU cache: every probe row then pays a cache miss +
        // collision-chain walk (Q09 SF10 measured 0.19–0.38 µs/row on
        // 64–225 MB tables). Keeping it PARTITIONED — each partition
        // ≈ total / N, sized to stay L2/L3-resident — lets the
        // per-partition probe (`execute_grace_inner` /
        // `execute_grace_shared`) run cache-hot. No disk involved: the
        // empty `PartitionedSpillFile` makes the spilled-partition Pass
        // 2 a no-op; all build data stays in `in_mem`.
        //
        // COMPOSITE-KEY GATE (`right_key_indices.len() >= 2`). The SF3/
        // SF5/SF10 22q-warm A/B showed cache-fit is a net win ONLY for
        // multi-column-key joins: there the per-row probe does an N-col
        // key comparison that randomly accesses the build batch's
        // columns, so a cache-resident partition speeds up the
        // comparison itself (Q09's `lineitem⋈partsupp` on
        // `(l_partkey,l_suppkey)` dropped 23% at SF10). Single-key joins
        // get NO comparison benefit — only the per-partition routing
        // overhead — so they REGRESS (q03/q05/q21 ran +16–31%). Gating
        // on key count keeps the win and drops the single-key losses.
        if cache_fit_enabled()
            && right_key_indices.len() >= 2
            && total_build_bytes > cache_fit_threshold_bytes()
        {
            let schema = right_schema
                .clone()
                .expect("non-empty build (bytes > threshold) implies schema set");
            let empty_partitions =
                PartitionedSpillWriter::new(schema.clone(), n_partitions, "hash_join_cache_fit")
                    .finish()?;
            tracing::info!(
                target: "arneb::mem",
                consumer = consumer_name,
                build_bytes = total_build_bytes,
                n_partitions,
                threshold_bytes = cache_fit_threshold_bytes(),
                "cache-fit: keeping build partitioned in memory (no spill)",
            );
            // Release the build reservation like the Single / spilled
            // paths — the data lives in `in_mem`, so peak RSS is
            // unchanged; the per-partition probe re-derives its state.
            build_reservation.free();
            return Ok(PartitionedBuildOutcome {
                result: BuildChunksResult::Partitioned {
                    in_mem,
                    partitions: empty_partitions,
                    schema,
                    total_bytes: total_build_bytes,
                    partition_rows,
                    partitioner,
                },
                df_collectors: df_collectors_final,
            });
        }

        // Below threshold (or cache-fit disabled). Flatten the
        // per-partition in_mem vectors back into a single
        // Vec<RecordBatch> so the caller can take the Single fast path
        // (concat + JoinHashMap).
        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for batches in in_mem.into_iter().flatten() {
            all_batches.extend(batches);
        }
        return Ok(PartitionedBuildOutcome {
            result: BuildChunksResult::Single {
                batches: all_batches,
                schema: right_schema,
                total_bytes: total_build_bytes,
                total_rows: partition_rows.iter().sum(),
                reservation: build_reservation,
            },
            df_collectors: df_collectors_final,
        });
    }

    // At least one partition spilled. Seal the writer.
    let schema_final = right_schema.expect("at least one batch implies schema set");
    let writer = spill_writer.expect("any_spilled implies a writer exists");
    let partitions = writer.finish()?;
    // Free the build reservation — Pass 2 re-reserves per partition.
    build_reservation.free();

    Ok(PartitionedBuildOutcome {
        result: BuildChunksResult::Partitioned {
            in_mem,
            partitions,
            schema: schema_final,
            total_bytes: total_build_bytes,
            partition_rows,
            partitioner,
        },
        df_collectors: df_collectors_final,
    })
}

/// Default cap used by the partitioned-build DF side-channel. Matches
/// the legacy `dynamic_filter_cap` choice for primitive types — set
/// at 50K distinct values per column before DF is dropped for that
/// column.
pub(crate) const DEFAULT_DYNAMIC_FILTER_CAP_PRIMITIVE: usize = 50_000;

#[async_trait]
impl ExecutionPlan for HashJoinExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        let mut schema = self.left.schema();
        schema.extend(self.right.schema());
        schema
    }

    fn output_partitioning(&self) -> Partitioning {
        // Hash-co-partitioned: output preserves the left's hash partitioning,
        // letting downstream joins / aggregates inherit the same shuffle.
        // Probe-side parallel (legacy): also preserves left's partitioning
        // because each probe partition is independent.
        // RIGHT/FULL collapse to one output partition (V1 limitation).
        if matches!(self.join_type, ast::JoinType::Inner | ast::JoinType::Left) {
            self.left.output_partitioning()
        } else {
            Partitioning::UnknownPartitioning(1)
        }
    }

    fn required_input_partitioning(&self) -> Vec<Partitioning> {
        // Left accepts the input as-is (parallel probe or hash). Right
        // must be coalesced to a single partition for the legacy shared-
        // build path, or hash-co-partitioned with left for parallel
        // build. The planner decides via `build_join_inputs`.
        vec![
            self.left.output_partitioning(),
            self.right.output_partitioning(),
        ]
    }

    fn inject_dynamic_filter(&self, filter: PlanExpr, target_index: usize) {
        // Output schema = left ++ right. Descend into the OWNING side only,
        // remapping the target index into that child's schema. This routes by
        // provenance, not by name, so a same-named twin on the other side (or
        // elsewhere in the subtree) is never reached.
        let left_width = self.left.schema().len();
        if target_index < left_width {
            self.left.inject_dynamic_filter(filter, target_index);
        } else {
            self.right
                .inject_dynamic_filter(filter, target_index - left_width);
        }
    }

    fn peak_bytes_reserved(&self) -> usize {
        self.peak_build_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream, ExecutionError> {
        let parallel = matches!(self.join_type, ast::JoinType::Inner | ast::JoinType::Left);
        let n_left = self.left.output_partitioning().partition_count();

        if std::env::var_os("ARNEB_TRACE_DFPUB").is_some() {
            let prod_ids: Vec<u32> = self
                .dynamic_filter_producers
                .iter()
                .map(|p| p.id.0)
                .collect();
            eprintln!(
                "[DFEXEC] part={partition} jt={:?} n_left={n_left} parallel={parallel} copart={} df_enabled={} producers={:?}",
                self.join_type,
                self.is_hash_copartitioned(),
                self.dynamic_filtering_enabled,
                prod_ids,
            );
        }

        if !parallel || n_left <= 1 {
            // Single-partition path: build right + left fully, then probe.
            // RIGHT/FULL go here too because they need a global right_matched
            // bitmap that spans the entire probe input.
            if partition != 0 {
                return Err(ExecutionError::InvalidOperation(format!(
                    "HashJoinExec single-partition path: partition {partition} out of range"
                )));
            }
            return self.execute_single(0).await;
        }

        // Trino-style hash-co-partitioned mode: when the planner has
        // wrapped both inputs in `RepartitionExec(Hash(...))` so they
        // share the same `N` and the keys are aligned, each partition
        // can independently build its own hash table from
        // `right.execute(partition)` and probe with `left.execute(partition)`
        // — no shared OnceCell, no global build serialisation.
        if self.is_hash_copartitioned() {
            return self.execute_per_partition(partition).await;
        }

        // Probe-side parallel path: build is shared across probe partitions.
        //
        // Step-SB streaming-probe-with-output-buffer was implemented
        // (see `StreamingProbeBatched`, `probe_batch_pure`) but bench
        // showed flat-to-slightly-worse: Q09 +33%, total +9%. The
        // streaming wrapper's per-batch concat overhead and state-
        // machine pin-project costs ate the pipelining win, which at
        // SF1 amounts to only ~200ms per deep join. Code retained as
        // `#[allow(dead_code)]` for a future re-attempt at SF10+
        // (where pipelining savings should dominate overhead).
        // Build FIRST, THEN left scan, so that dynamic filters derived
        // from build keys (injected during `ensure_built`) reach
        // left's ScanExec before its first `execute(partition)` call.
        // The earlier `tokio::join!(build_fut, left_collect_fut)`
        // saved ~250ms on Q02 but conflicts with dynamic filter
        // injection — Trino-style runtime-filter pushdown pays off
        // far more than the build/collect overlap on multi-join.
        // Step PB v2 attempt (2026-05-16): tried gating on "both
        // children are leaf-scan subtrees" hoping to limit overlap to
        // the innermost L1 join. Reality: in multi-join queries like
        // Q07/Q08/Q09 (5-way) every join level has a small leaf-scan
        // build (nation/supplier), so the leaf gate fired everywhere
        // and replayed PB v1's task-count regression (Q07 +44%, Q12
        // +30%). Reverted. Keeping sequential.

        // Build first — `ensure_built` is single-flight via OnceCell.
        // When Grace HJ is enabled (Phase M.2c), the build returns
        // `BuildSide::Partitioned` with `Arc`-shared in-memory hash
        // tables + spill files — all N probe partitions share one
        // build, eliminating the M.2b 4× build duplication that caused
        // Q09 to OOM. For Single/Multipass we still collect left like
        // before (no streaming probe in the non-Grace path yet).
        let build = self.ensure_built().await?;
        if std::env::var_os("ARNEB_TRACE_DFPUB").is_some() {
            let side = match &build.side {
                BuildSide::Single { .. } => "Single",
                BuildSide::Multipass { .. } => "Multipass",
                BuildSide::Partitioned { .. } => "Partitioned",
            };
            eprintln!(
                "[DFPUB] part={partition} n_left={n_left} build_side={side} df_enabled={} producers={}",
                self.dynamic_filtering_enabled,
                self.dynamic_filter_producers.len()
            );
        }
        // Broadcast DF publish for the PARALLEL-probe Single-build path
        // (n_left>1): the build is a shared broadcast table and the probe
        // (fact) is data-parallel. Publish the build-key domain BEFORE the
        // probe's `left.execute(partition)` so the fact scan's
        // DynamicFilterConsumer resolves and prunes during scan. The
        // n_left<=1 path publishes inside `execute_single`; this covers the
        // n_left>1 case (TPC-H q05's lineitem-probe / broadcast-dim joins).
        // Publish once per worker task, not once per probe partition. The build
        // is OnceCell-shared/replicated across this task's probe partitions, so
        // partition 0 carries the full build-key domain and reports with the
        // task partition index expected by the coordinator.
        if let BuildSide::Single {
            right_combined: Some(rb),
            ..
        } = &build.side
        {
            self.publish_broadcast_cross_fragment_dfs(partition as u32, rb)
                .await;
        }
        let left_stream = self.left.execute(partition).await?;

        if let BuildSide::Partitioned {
            in_mem_hash_maps,
            partitions,
            right_schema,
            n_partitions,
        } = &build.side
        {
            return self
                .execute_grace_shared(
                    Arc::clone(in_mem_hash_maps),
                    Arc::clone(partitions),
                    *n_partitions,
                    right_schema.clone(),
                    left_stream,
                )
                .await;
        }

        match &build.side {
            BuildSide::Single {
                right_combined,
                hash_map,
            } => {
                let right_batch = match right_combined {
                    Some(b) if b.num_rows() > 0 => b,
                    _ => {
                        return self.handle_empty_right_partition(partition).await;
                    }
                };
                if stream_hash_probe_enabled() {
                    return self.stream_single_probe(
                        left_stream,
                        Arc::new(right_batch.clone()),
                        Arc::clone(hash_map),
                    );
                }

                let left_batches = collect_stream(left_stream).await.map_err(|e| {
                    ExecutionError::InvalidOperation(format!("hash join left collect: {e}"))
                })?;
                let left_combined = if left_batches.is_empty() {
                    None
                } else if left_batches.len() == 1 {
                    Some(left_batches.into_iter().next().unwrap())
                } else {
                    Some(compute::concat_batches(
                        &left_batches[0].schema(),
                        left_batches.iter(),
                    )?)
                };
                let left_batch = match &left_combined {
                    Some(b) if b.num_rows() > 0 => b,
                    _ => {
                        let schema = column_info_to_arrow_schema(&self.schema());
                        return Ok(stream_from_batches(schema, vec![]));
                    }
                };
                let result = self.probe(left_batch, right_batch, hash_map)?;
                let schema = result
                    .first()
                    .map(|b| b.schema())
                    .unwrap_or_else(|| column_info_to_arrow_schema(&self.schema()));
                Ok(stream_from_batches(schema, result))
            }
            BuildSide::Multipass {
                chunks,
                right_schema,
            } => {
                let left_batches = collect_stream(left_stream).await.map_err(|e| {
                    ExecutionError::InvalidOperation(format!("hash join left collect: {e}"))
                })?;
                self.execute_multipass_inner(Arc::clone(chunks), right_schema.clone(), left_batches)
                    .await
            }
            BuildSide::Partitioned { .. } => {
                unreachable!("BuildSide::Partitioned handled above before collect_stream",)
            }
        }
    }

    fn display_name(&self) -> &str {
        "HashJoinExec"
    }
}

impl HashJoinExec {
    fn join_type_label(&self) -> &'static str {
        match self.join_type {
            ast::JoinType::Inner => "Inner",
            ast::JoinType::Left => "Left",
            ast::JoinType::Right => "Right",
            ast::JoinType::Full => "Full",
            ast::JoinType::Cross => "Cross",
        }
    }

    fn probe_driver(&self) -> Self {
        Self {
            left: Arc::clone(&self.left),
            right: Arc::clone(&self.right),
            join_type: self.join_type,
            left_keys: self.left_keys.clone(),
            right_keys: self.right_keys.clone(),
            residual: self.residual.clone(),
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::clone(&self.memory_pool),
            dynamic_filter_producers: self.dynamic_filter_producers.clone(),
            dynamic_filter_publisher: self.dynamic_filter_publisher.clone(),
            dynamic_filtering_enabled: self.dynamic_filtering_enabled,
            df_targets: self.df_targets.clone(),
        }
    }

    fn stream_single_probe(
        &self,
        mut left_stream: SendableRecordBatchStream,
        right_batch: Arc<RecordBatch>,
        hash_map: Arc<JoinHashMap>,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let output_schema = column_info_to_arrow_schema(&self.schema());
        let probe_exec = Arc::new(self.probe_driver());

        let inner = async_stream::try_stream! {
            while let Some(batch_res) = left_stream.next().await {
                let left_batch = batch_res.map_err(|e| {
                    ExecutionError::InvalidOperation(format!(
                        "hash join streaming probe left stream: {e}"
                    ))
                })?;
                if left_batch.num_rows() == 0 {
                    continue;
                }
                for out in probe_exec.probe(&left_batch, &right_batch, &hash_map)? {
                    yield out;
                }
            }
        };

        Ok(Box::pin(AsyncBatchStream {
            schema: output_schema,
            inner: Box::pin(inner),
        }))
    }

    fn emit_build_profile(
        &self,
        partition: usize,
        build_rows: usize,
        build_bytes: usize,
        build_strategy: &'static str,
    ) {
        tracing::info!(
            target: "arneb::profile",
            op = "HashJoinExec.build",
            partition,
            build_rows = build_rows as u64,
            build_bytes = build_bytes as u64,
            build_strategy,
            join_type = self.join_type_label(),
            "hash join build finished"
        );
    }

    fn emit_single_build_profile(
        &self,
        partition: usize,
        right_combined: Option<&RecordBatch>,
        hash_map: &JoinHashMap,
    ) {
        let build_rows = right_combined.map(|b| b.num_rows()).unwrap_or(0);
        let build_bytes = right_combined
            .map(crate::operator::record_batch_bytes)
            .unwrap_or(0)
            .saturating_add(hash_map.heap_bytes());
        self.emit_build_profile(partition, build_rows, build_bytes, "Single");
    }

    fn emit_partitioned_build_profile_from_maps(
        &self,
        in_mem_hash_maps: &[Option<(RecordBatch, JoinHashMap)>],
        partition_rows: &[usize],
    ) {
        for (partition, slot) in in_mem_hash_maps.iter().enumerate() {
            let build_rows = partition_rows.get(partition).copied().unwrap_or_else(|| {
                slot.as_ref()
                    .map(|(batch, _)| batch.num_rows())
                    .unwrap_or(0)
            });
            let build_bytes = slot
                .as_ref()
                .map(|(batch, hash_map)| {
                    crate::operator::record_batch_bytes(batch).saturating_add(hash_map.heap_bytes())
                })
                .unwrap_or(0);
            self.emit_build_profile(partition, build_rows, build_bytes, "Partitioned");
        }
    }

    fn emit_partitioned_build_profile_from_batches(
        &self,
        in_mem: &[Option<Vec<RecordBatch>>],
        partition_rows: &[usize],
    ) {
        for (partition, batches_opt) in in_mem.iter().enumerate() {
            let build_rows = partition_rows
                .get(partition)
                .copied()
                .unwrap_or_else(|| batches_opt.iter().flatten().map(|b| b.num_rows()).sum());
            let build_bytes = batches_opt
                .iter()
                .flatten()
                .map(crate::operator::record_batch_bytes)
                .sum();
            self.emit_build_profile(partition, build_rows, build_bytes, "Partitioned");
        }
    }

    /// Construct an `Arc<HashJoinExec>` and, when called from inside
    /// a tokio runtime (production query path), eagerly spawn the
    /// build phase as a background task. By the time the query
    /// orchestrator calls `execute(partition)`, the OnceCell-backed
    /// build is already populated or in flight — saving the per-
    /// partition serial barrier on right-side scan + hash-table
    /// construction.
    ///
    /// Falls back to lazy-build (no spawn) when there is no current
    /// runtime — preserves existing behaviour for synchronous unit
    /// tests that construct `HashJoinExec` directly via struct
    /// literals.
    ///
    /// Replaces the failed PB-v1 (always-join in execute) and PB-v2
    /// (leaf-leaf gate) approaches: those added a task-count
    /// multiplier per join level inside the hot path. This spawns
    /// ONCE per join at plan construction, outside the execute call
    /// stack, so the probe path stays as cheap as before.
    pub(crate) fn new_arc(self) -> Arc<Self> {
        // Eager prewarm via `tokio::Handle::current().spawn(ensure_built)`
        // was attempted 2026-05-16 (Step PB v3). Like v1 (always-overlap
        // in execute) and v2 (leaf-leaf gated), it regressed: Q03 511 →
        // 611ms, Q07 558 → 664ms, Q12 460 → 529ms. Root cause: arneb
        // runs a single tokio runtime with `num_cpus` workers. Any
        // background spawn competes with the main query's tasks for
        // those same workers — added context switches outweigh the
        // build/scan overlap. Trino avoids this with separate JVM
        // thread pools per stage. Closing the join-pipelining wall-
        // time gap on arneb needs a driver model (2-3 weeks), not
        // ad-hoc spawning. Kept as plain `Arc::new` for now.
        Arc::new(self)
    }

    /// True when the planner has wrapped both sides in
    /// `RepartitionExec(Hash(...))` with the same `N` AND the
    /// expression count on each side matches the join key count. The
    /// stronger contract — that each side's `i`-th hash expression
    /// reads from the column referenced by `left_keys[i]` /
    /// `right_keys[i]` — is upheld by the planner (`build_join_inputs`);
    /// we rely on that here rather than re-walking the `PlanExpr` tree.
    fn is_hash_copartitioned(&self) -> bool {
        let l = self.left.output_partitioning();
        let r = self.right.output_partitioning();
        if !matches!(self.join_type, ast::JoinType::Inner) {
            return false;
        }
        match (&l, &r) {
            (Partitioning::Hash(le, ln), Partitioning::Hash(re, rn)) => {
                ln == rn
                    && le.len() == self.left_keys.len()
                    && re.len() == self.right_keys.len()
                    && *ln > 1
            }
            _ => false,
        }
    }

    /// Hash-co-partitioned execution: each partition independently
    /// drives its own build + probe. No shared OnceCell. Critical for
    /// multi-join queries where the legacy shared-build serializes
    /// every join level on a single core (see TPC-H Q07/Q08).
    ///
    /// Phase 3b.2 (2026-05-21): INNER joins without residual stream
    /// the build through `build_with_spill`. INNER + residual falls
    /// back to the pre-spill collect-and-concat path (multipass
    /// doesn't yet handle residual without changes to per-chunk probe).
    /// is_hash_copartitioned already rules out non-INNER join types.
    async fn execute_per_partition(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let t_pp = Instant::now();
        let right_stream = self.right.execute(partition).await?;

        if !self.multipass_supported() {
            return self
                .execute_per_partition_no_spill(partition, right_stream)
                .await;
        }

        // Phase M.2 (2026-05-21): route INNER per-partition joins through
        // Grace HJ when `ARNEB_GRACE_HJ=1`. Avoids the `collect_stream(left)`
        // below which materialises the per-partition left input (~150 MB
        // per partition for Q09 SF1 lineitem) into a `Vec<RecordBatch>`
        // before probing. `execute_grace_single` streams the left side
        // batch-by-batch and routes rows to matching partition hash
        // tables / spill files, keeping per-partition peak bounded.
        if self.grace_enabled() {
            let left_stream = self.left.execute(partition).await?;
            return self.execute_grace_single(left_stream, right_stream).await;
        }

        let t_build = Instant::now();
        let build_result = build_with_spill(
            right_stream,
            Arc::clone(&self.memory_pool),
            "HashJoinExec.per_partition_build",
        )
        .await?;
        let build_ms = t_build.elapsed().as_millis() as u64;

        // Probe-side: collect this partition's left input.
        let t_left = Instant::now();
        let left_stream = self.left.execute(partition).await?;
        let left_batches = collect_stream(left_stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("hash join left collect (p={partition}): {e}"))
        })?;
        let left_collect_ms = t_left.elapsed().as_millis() as u64;
        let left_rows: u64 = left_batches.iter().map(|b| b.num_rows() as u64).sum();
        tracing::info!(
            target: "arneb::profile",
            op = "HashJoinExec.execute_per_partition",
            join_type = ?self.join_type,
            partition,
            build_ms,
            left_collect_ms,
            left_rows,
            startup_ms = t_pp.elapsed().as_millis() as u64,
            "HashJoinExec per-partition build+left ready"
        );

        let schema = column_info_to_arrow_schema(&self.schema());

        match build_result {
            BuildChunksResult::Multipass {
                chunks,
                schema: right_schema,
                total_bytes,
                total_rows,
            } => {
                self.gate_multipass(chunks.len(), total_bytes)?;
                self.emit_build_profile(partition, total_rows, total_bytes, "Multipass");
                tracing::info!(
                    target: "arneb::mem",
                    operator = "HashJoinExec",
                    path = "execute_per_partition",
                    partition,
                    build_bytes = total_bytes,
                    pool_reserved = self.memory_pool.reserved(),
                    spilled_chunks = chunks.len(),
                    "per-partition build complete (multipass)",
                );
                self.execute_multipass_inner(Arc::new(chunks), right_schema, left_batches)
                    .await
            }
            BuildChunksResult::Single {
                batches,
                schema: right_schema,
                total_bytes: _,
                total_rows: _,
                reservation,
            } => {
                let right_combined = if batches.is_empty() {
                    None
                } else if batches.len() == 1 {
                    Some(batches.into_iter().next().unwrap())
                } else {
                    let rs = right_schema
                        .clone()
                        .expect("schema set when batches non-empty");
                    Some(compute::concat_batches(&rs, batches.iter())?)
                };

                // A1.7 (2026-05-27): non-Grace per-partition build
                // path. Publish cross-fragment DFs here before probe
                // runs so probe-side scans on the downstream stage
                // can apply the filter.
                self.emit_cross_fragment_dfs(right_combined.as_ref());

                let left_combined = if left_batches.is_empty() {
                    None
                } else if left_batches.len() == 1 {
                    Some(left_batches.into_iter().next().unwrap())
                } else {
                    Some(compute::concat_batches(
                        &left_batches[0].schema(),
                        left_batches.iter(),
                    )?)
                };

                let right_batch = match &right_combined {
                    Some(b) if b.num_rows() > 0 => b,
                    _ => {
                        // INNER with empty right partition → empty output.
                        self.emit_build_profile(partition, 0, 0, "Single");
                        return Ok(stream_from_batches(schema, vec![]));
                    }
                };
                let left_batch = match &left_combined {
                    Some(b) if b.num_rows() > 0 => b,
                    _ => {
                        return Ok(stream_from_batches(schema, vec![]));
                    }
                };

                let hash_map = JoinHashMap::build_single(right_batch, &self.right_keys)?;
                self.emit_single_build_profile(partition, right_combined.as_ref(), &hash_map);
                let result = self.probe(left_batch, right_batch, &hash_map)?;
                let _ = reservation;
                let out_schema = result.first().map(|b| b.schema()).unwrap_or(schema);
                Ok(stream_from_batches(out_schema, result))
            }
            BuildChunksResult::Partitioned { .. } => {
                // Phase 3b.5c stub: `build_with_partitioned_spill` is only
                // called when the planner picks Grace HJ. `execute_per_partition`
                // currently uses `build_with_spill` so this arm is unreachable
                // from this call site. Kept exhaustive for future routing.
                Err(ExecutionError::InvalidOperation(
                    "Partitioned build not yet wired into execute_per_partition (Phase 3b.5d pending)".into(),
                ))
            }
        }
    }

    /// Per-partition pre-spill fallback. Used for INNER + residual
    /// joins until multipass learns to apply residuals per chunk —
    /// preserves pre-3b.2 correctness instead of failing fast on
    /// budget exhaustion.
    async fn execute_per_partition_no_spill(
        &self,
        partition: usize,
        right_stream: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let right_batches = collect_stream(right_stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "hash join right collect (p={partition}): {e}"
            ))
        })?;
        let right_combined = if right_batches.is_empty() {
            None
        } else if right_batches.len() == 1 {
            Some(right_batches.into_iter().next().unwrap())
        } else {
            Some(compute::concat_batches(
                &right_batches[0].schema(),
                right_batches.iter(),
            )?)
        };

        // A1.7 (2026-05-27): non-Grace per-partition no-spill build
        // path (residual / LEFT joins). Publish before probe so the
        // downstream consumer scan can apply the filter.
        self.emit_cross_fragment_dfs(right_combined.as_ref());

        let left_stream = self.left.execute(partition).await?;
        let left_batches = collect_stream(left_stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("hash join left collect (p={partition}): {e}"))
        })?;
        let left_combined = if left_batches.is_empty() {
            None
        } else if left_batches.len() == 1 {
            Some(left_batches.into_iter().next().unwrap())
        } else {
            Some(compute::concat_batches(
                &left_batches[0].schema(),
                left_batches.iter(),
            )?)
        };

        let schema = column_info_to_arrow_schema(&self.schema());
        let right_batch = match &right_combined {
            Some(b) if b.num_rows() > 0 => b,
            _ => {
                self.emit_build_profile(partition, 0, 0, "Single");
                return Ok(stream_from_batches(schema, vec![]));
            }
        };
        let left_batch = match &left_combined {
            Some(b) if b.num_rows() > 0 => b,
            _ => return Ok(stream_from_batches(schema, vec![])),
        };

        let hash_map = JoinHashMap::build_single(right_batch, &self.right_keys)?;
        let result = self.probe(left_batch, right_batch, &hash_map)?;
        let out_schema = result.first().map(|b| b.schema()).unwrap_or(schema);
        Ok(stream_from_batches(out_schema, result))
    }

    /// Multi-pass probe path used when the build side overflowed the
    /// memory budget and at least one chunk was spilled to disk
    /// (Phase 3b, 2026-05-21). INNER joins only; the build side gates
    /// LEFT/RIGHT/FULL + residual at the ensure_built level.
    ///
    /// Algorithm: for each chunk (in-memory or spilled), load it,
    /// concat into one RecordBatch, build a JoinHashMap, scan all
    /// left batches once, emit the joined rows, drop the chunk's
    /// state. The total output is the concatenation of every chunk's
    /// emissions — by INNER semantics, a left row that matches in
    /// chunks 2 AND 5 produces its joined row twice (once per chunk),
    /// which is correct because every right_row match is a distinct
    /// output row.
    async fn execute_multipass_inner(
        &self,
        chunks: Arc<Vec<BuildChunk>>,
        right_schema: Arc<Schema>,
        left_batches: Vec<RecordBatch>,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let output_schema = column_info_to_arrow_schema(&self.schema());
        if left_batches.is_empty() {
            return Ok(stream_from_batches(output_schema, vec![]));
        }
        // Collapse the left side once (multi-pass re-scans it per chunk).
        let left_combined = if left_batches.len() == 1 {
            left_batches.into_iter().next().unwrap()
        } else {
            compute::concat_batches(&left_batches[0].schema(), left_batches.iter())?
        };
        if left_combined.num_rows() == 0 {
            return Ok(stream_from_batches(output_schema, vec![]));
        }

        // Phase 3b.4 (2026-05-21): yield batches per-chunk + per-output-slice
        // instead of accumulating into a single `Vec<RecordBatch>` for all
        // chunks. The Stream's poll_next drives one chunk's probe + one
        // 1024-row output slice at a time; downstream back-pressure (driver
        // loop polling slowly when memory-blocked) keeps peak RAM at
        // ~chunk-load + candidate-indices instead of all chunks × full
        // joined output. Eliminates the ~N× output-side OOM that killed
        // TPC-H Q09 at SF1.
        let left_keys = self.left_keys.clone();
        let right_keys = self.right_keys.clone();
        let memory_pool = Arc::clone(&self.memory_pool);
        let output_schema_for_stream = output_schema.clone();

        let inner = async_stream::try_stream! {
            let t_multipass = Instant::now();
            let mut multipass_total_load_ms: u64 = 0;
            let mut multipass_total_probe_ms: u64 = 0;
            let load_consumer = MemoryConsumer::new("HashJoinExec.spill_load")
                .with_can_spill(false);
            let mut load_reservation = load_consumer.register(memory_pool);

            for (chunk_idx, chunk) in chunks.iter().enumerate() {
                let t_chunk_load = Instant::now();
                let chunk_batches: Vec<RecordBatch> = match chunk {
                    BuildChunk::InMemory(b) => b.clone(),
                    BuildChunk::Spilled(file) => {
                        let reader = file.open_reader()?;
                        let mut acc = Vec::new();
                        for b in reader {
                            let batch = b?;
                            let bytes = crate::operator::record_batch_bytes(&batch);
                            load_reservation.try_grow(bytes)?;
                            acc.push(batch);
                        }
                        acc
                    }
                };
                if chunk_batches.is_empty() {
                    continue;
                }
                let loaded_bytes: usize = chunk_batches
                    .iter()
                    .map(crate::operator::record_batch_bytes)
                    .sum();
                if matches!(chunk, BuildChunk::InMemory(_)) {
                    load_reservation.try_grow(loaded_bytes)?;
                }

                let right_batch = compute::concat_batches(&right_schema, chunk_batches.iter())
                    .map_err(|e| ExecutionError::InvalidOperation(format!(
                        "multipass concat (chunk {chunk_idx}): {e}"
                    )))?;
                if right_batch.num_rows() == 0 {
                    load_reservation.shrink(loaded_bytes);
                    continue;
                }
                let hash_map = JoinHashMap::build_single(&right_batch, &right_keys)?;
                let chunk_load_ms = t_chunk_load.elapsed().as_millis() as u64;
                multipass_total_load_ms += chunk_load_ms;
                tracing::info!(
                    target: "arneb::mem",
                    operator = "HashJoinExec",
                    chunk_idx,
                    right_rows = right_batch.num_rows(),
                    loaded_bytes,
                    "multipass: probing left against build chunk (streaming)",
                );

                // Collect equi-match candidates once per chunk (~24 MB
                // for 6M-row TPC-H probe). Then slice into 1024-row
                // output batches and yield each — bounded peak RAM.
                let t_chunk_probe = Instant::now();
                let (cand_left, cand_right) = multipass_collect_candidates(
                    &left_combined,
                    &right_batch,
                    &hash_map,
                    &left_keys,
                    &right_keys,
                )?;

                if !cand_left.is_empty() {
                    for slice_start in (0..cand_left.len()).step_by(MULTIPASS_OUTPUT_BATCH_ROWS) {
                        let slice_end = (slice_start + MULTIPASS_OUTPUT_BATCH_ROWS).min(cand_left.len());
                        let out_batch = build_joined_slice(
                            &left_combined,
                            &right_batch,
                            &cand_left[slice_start..slice_end],
                            &cand_right[slice_start..slice_end],
                            &output_schema_for_stream,
                        )?;
                        yield out_batch;
                    }
                }
                multipass_total_probe_ms += t_chunk_probe.elapsed().as_millis() as u64;

                // Drop chunk state so the load_reservation shrinks
                // before the next chunk's try_grow.
                drop(hash_map);
                drop(right_batch);
                drop(chunk_batches);
                load_reservation.shrink(loaded_bytes);
            }

            tracing::info!(
                target: "arneb::profile",
                op = "HashJoinExec.execute_multipass_inner",
                n_chunks = chunks.len() as u64,
                load_ms = multipass_total_load_ms,
                probe_ms = multipass_total_probe_ms,
                total_ms = t_multipass.elapsed().as_millis() as u64,
                "HashJoinExec multipass done"
            );
        };

        Ok(Box::pin(AsyncBatchStream {
            schema: output_schema,
            inner: Box::pin(inner),
        }))
    }

    /// Grace Hash Join execute (Phase 3b.5d, 2026-05-21). Two-pass
    /// algorithm matching Trino's `DefaultPageJoiner` + `SpillingJoinProcessor`:
    ///
    /// **Pass 1** — stream the probe input batch-by-batch. Compute the
    /// partition id per row; rows whose build partition is in memory
    /// are probed against that partition's hash table and emitted
    /// immediately, rows whose build partition was spilled get written
    /// to a probe-side per-partition spill file.
    ///
    /// Phase M.2c (2026-05-22): Arc-shared Grace HJ probe — used by
    /// the parallel `execute()` shared-build path where N probe
    /// partitions all share one build constructed under
    /// `ensure_built`'s `OnceCell` guard.
    ///
    /// Differs from `execute_grace_inner`:
    /// - In-memory hash tables are pre-built once (during build phase)
    ///   and passed in as `Arc<Vec<Option<(RecordBatch, JoinHashMap)>>>`.
    /// - Spill files are `Arc<PartitionedSpillFile>` — read via
    ///   `.partition(p).open_reader()` (immutable, multi-reader safe).
    /// - Each probe partition writes its OWN per-partition probe spill
    ///   files (probe spills are not shared across probe partitions).
    async fn execute_grace_shared(
        &self,
        in_mem_hash_maps: Arc<Vec<Option<(RecordBatch, JoinHashMap)>>>,
        partitions: Arc<PartitionedSpillFile>,
        n_partitions: usize,
        right_schema: Arc<Schema>,
        left_stream: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let output_schema = column_info_to_arrow_schema(&self.schema());
        let probe_partitioner =
            Arc::new(HashPartitioner::new(self.left_key_exprs(), n_partitions)?);

        let left_keys = self.left_keys.clone();
        let right_keys = self.right_keys.clone();
        let output_schema_for_stream = output_schema.clone();
        let memory_pool = Arc::clone(&self.memory_pool);

        let inner = async_stream::try_stream! {
            // Pass 1: drain probe stream. Route per row to matching
            // build partition. In-memory partitions probe immediately;
            // spilled partitions buffer to a probe-side spill file.
            let mut probe_spill: Option<PartitionedSpillWriter> = None;
            let mut left_stream = left_stream;
            while let Some(batch_res) = left_stream.next().await {
                let left_batch = batch_res.map_err(|e| {
                    ExecutionError::InvalidOperation(format!(
                        "grace shared probe stream: {e}"
                    ))
                })?;
                if left_batch.num_rows() == 0 {
                    continue;
                }

                let assignments = probe_partitioner.assignments(&left_batch)?;
                let mut buckets: Vec<Vec<u32>> =
                    (0..n_partitions).map(|_| Vec::new()).collect();
                for (row, &p) in assignments.iter().enumerate() {
                    buckets[p as usize].push(row as u32);
                }

                let left_schema = left_batch.schema();
                // Phase Z.1b (2026-05-22): hoist left-side TypedKeys
                // construction out of the per-partition loop. The same
                // left_batch is probed against every non-empty partition;
                // there's no reason to rebuild TypedKeys 16× per batch.
                let left_typed = TypedKeys::new(&left_batch, &left_keys)?;
                let left_hashes =
                    vectorized_probe_enabled().then(|| left_typed.hash_batch(left_batch.num_rows()));
                for (p, indices) in buckets.into_iter().enumerate() {
                    if indices.is_empty() {
                        continue;
                    }

                    match in_mem_hash_maps[p].as_ref() {
                        Some((right_batch, hash_map)) => {
                            // Phase Z.1 (2026-05-22): probe in-memory
                            // partitions directly against the original
                            // `left_batch` using the per-partition row
                            // indices. No intermediate sub-batch
                            // materialization — `compute::take` only
                            // happens once per output slice inside
                            // `build_joined_slice` (necessary to emit
                            // the joined row). Drops Pass 1 peak by
                            // ~10× vs the materialised-sub-batch flow
                            // for Q05/Q09's in-memory build partitions.
                            let (cand_left, cand_right) = multipass_collect_candidates_subset_with_left(
                                &left_typed,
                                left_hashes.as_deref(),
                                &indices,
                                right_batch,
                                hash_map,
                                &right_keys,
                            )?;
                            if cand_left.is_empty() {
                                continue;
                            }
                            for slice_start in
                                (0..cand_left.len()).step_by(MULTIPASS_OUTPUT_BATCH_ROWS)
                            {
                                let slice_end = (slice_start + MULTIPASS_OUTPUT_BATCH_ROWS)
                                    .min(cand_left.len());
                                let out = build_joined_slice(
                                    &left_batch,
                                    right_batch,
                                    &cand_left[slice_start..slice_end],
                                    &cand_right[slice_start..slice_end],
                                    &output_schema_for_stream,
                                )?;
                                yield out;
                            }
                        }
                        None => {
                            // Build partition was spilled OR empty.
                            // Truly spilled → write probe rows to disk
                            // (necessary materialization for IPC); empty
                            // → drop (INNER produces no matches).
                            if partitions.has_partition(p) {
                                let idx_array = UInt32Array::from(indices);
                                let cols: Vec<ArrayRef> = (0..left_batch.num_columns())
                                    .map(|i| {
                                        compute::take(left_batch.column(i), &idx_array, None)
                                    })
                                    .collect::<Result<_, _>>()?;
                                let probe_sub =
                                    RecordBatch::try_new(left_schema.clone(), cols)?;
                                let writer = probe_spill.get_or_insert_with(|| {
                                    PartitionedSpillWriter::new(
                                        left_schema.clone(),
                                        n_partitions,
                                        "hash_join_grace_shared_probe",
                                    )
                                });
                                writer.write_partition(p, &probe_sub)?;
                            }
                        }
                    }
                }
            }

            // Pass 2: for each spilled build partition, open its
            // reader (immutable on Arc<PartitionedSpillFile>), build a
            // hash table, then stream the probe partition file. Each
            // probe partition does this independently — no contention
            // between concurrent probe partitions on the shared build
            // spill files (just N file readers on the same on-disk
            // files; Linux page cache handles overlap).
            let mut probe_partitions_sealed = match probe_spill {
                Some(w) => Some(w.finish()?),
                None => None,
            };
            let load_consumer = MemoryConsumer::new("HashJoinExec.grace_shared_load")
                .with_can_spill(false);
            let mut load_reservation = load_consumer.register(memory_pool);

            for p in 0..n_partitions {
                if !partitions.has_partition(p) {
                    continue;
                }
                let Some(probe_p_file) = probe_partitions_sealed
                    .as_mut()
                    .and_then(|pp| pp.take_partition(p))
                else {
                    continue;
                };
                let build_p_file = partitions
                    .partition(p)
                    .expect("has_partition(p) → partition(p) must succeed");

                // Load build partition into memory.
                let build_reader = build_p_file.open_reader()?;
                let mut build_batches: Vec<RecordBatch> = Vec::new();
                let mut build_loaded_bytes: usize = 0;
                for b in build_reader {
                    let batch = b?;
                    let bytes = crate::operator::record_batch_bytes(&batch);
                    load_reservation.try_grow(bytes)?;
                    build_loaded_bytes += bytes;
                    build_batches.push(batch);
                }
                if build_batches.is_empty() {
                    load_reservation.shrink(build_loaded_bytes);
                    continue;
                }
                let combined = if build_batches.len() == 1 {
                    build_batches.into_iter().next().unwrap()
                } else {
                    compute::concat_batches(&right_schema, build_batches.iter())?
                };
                if combined.num_rows() == 0 {
                    load_reservation.shrink(build_loaded_bytes);
                    continue;
                }
                let hash_map = JoinHashMap::build_single(&combined, &right_keys)?;

                tracing::info!(
                    target: "arneb::mem",
                    operator = "HashJoinExec",
                    path = "execute_grace_shared",
                    grace_partition = p,
                    right_rows = combined.num_rows(),
                    "grace pass-2: loaded build partition + opening probe partition",
                );

                // Stream the probe partition file through probe().
                let probe_reader = probe_p_file.open_reader()?;
                for probe_batch_res in probe_reader {
                    let probe_batch = probe_batch_res?;
                    if probe_batch.num_rows() == 0 {
                        continue;
                    }
                    let (cand_left, cand_right) = multipass_collect_candidates(
                        &probe_batch,
                        &combined,
                        &hash_map,
                        &left_keys,
                        &right_keys,
                    )?;
                    if cand_left.is_empty() {
                        continue;
                    }
                    for slice_start in
                        (0..cand_left.len()).step_by(MULTIPASS_OUTPUT_BATCH_ROWS)
                    {
                        let slice_end =
                            (slice_start + MULTIPASS_OUTPUT_BATCH_ROWS).min(cand_left.len());
                        let out = build_joined_slice(
                            &probe_batch,
                            &combined,
                            &cand_left[slice_start..slice_end],
                            &cand_right[slice_start..slice_end],
                            &output_schema_for_stream,
                        )?;
                        yield out;
                    }
                }

                // Drop this build partition's hash + batch + probe file
                // before processing the next — keeps Pass 2 peak bounded.
                drop(hash_map);
                drop(combined);
                drop(probe_p_file);
                load_reservation.shrink(build_loaded_bytes);
            }
        };

        Ok(Box::pin(AsyncBatchStream {
            schema: output_schema,
            inner: Box::pin(inner),
        }))
    }

    /// **Pass 2** — for each spilled (build_p, probe_p) pair: load the
    /// build partition into memory, build its hash table, stream the
    /// probe partition file through `probe(...)`, emit. Drop the
    /// partition's state before moving to the next.
    ///
    /// Memory peak: one in-memory build partition plus one probe
    /// batch plus per-partition spill writer buffers (one open batch
    /// per spilled partition). For Q09-scale data with 16 partitions
    /// and a 1.2 GB build, each partition is ~75 MB — fits comfortably
    /// alongside per-partition spill load.
    #[allow(clippy::too_many_arguments)]
    async fn execute_grace_inner(
        &self,
        in_mem: Vec<Option<Vec<RecordBatch>>>,
        mut partitions: PartitionedSpillFile,
        n_partitions: usize,
        right_schema: Arc<Schema>,
        left_stream: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let output_schema = column_info_to_arrow_schema(&self.schema());

        // The probe side needs a partitioner built from the LEFT join
        // keys (since they evaluate against probe batches). The build-
        // side partitioner used a Vec<PlanExpr> over the RIGHT keys.
        // Both must agree on `partition_id` for matching key values —
        // same hash function (ahash seed (0,0,0,0)), same modulus N.
        let probe_partitioner =
            Arc::new(HashPartitioner::new(self.left_key_exprs(), n_partitions)?);

        let left_keys = self.left_keys.clone();
        let right_keys = self.right_keys.clone();
        let output_schema_for_stream = output_schema.clone();

        let inner = async_stream::try_stream! {
            let t_grace = Instant::now();
            // Pre-build per-partition hash tables for the in-memory
            // build partitions. `in_mem_hash_maps[p]` is None if that
            // partition was spilled OR empty.
            let mut in_mem_hash_maps: Vec<Option<(RecordBatch, JoinHashMap)>> =
                Vec::with_capacity(n_partitions);
            for batches_opt in in_mem.iter() {
                match batches_opt {
                    Some(batches) if !batches.is_empty() => {
                        let combined = if batches.len() == 1 {
                            batches[0].clone()
                        } else {
                            compute::concat_batches(&right_schema, batches.iter())?
                        };
                        if combined.num_rows() == 0 {
                            in_mem_hash_maps.push(None);
                            continue;
                        }
                        let hm = JoinHashMap::build_single(&combined, &right_keys)?;
                        in_mem_hash_maps.push(Some((combined, hm)));
                    }
                    _ => in_mem_hash_maps.push(None),
                }
            }
            let in_mem_build_ms = t_grace.elapsed().as_millis() as u64;
            let in_mem_partitions = in_mem_hash_maps.iter().filter(|h| h.is_some()).count() as u64;

            // Pass 1: drain probe stream. Route per row.
            let t_pass1 = Instant::now();
            let mut pass1_probe_rows: u64 = 0;
            let mut probe_spill: Option<PartitionedSpillWriter> = None;
            let mut left_stream = left_stream;
            while let Some(batch_res) = left_stream.next().await {
                let left_batch = batch_res.map_err(|e| {
                    ExecutionError::InvalidOperation(format!(
                        "grace probe input stream: {e}"
                    ))
                })?;
                if left_batch.num_rows() == 0 {
                    continue;
                }
                pass1_probe_rows += left_batch.num_rows() as u64;

                let assignments = probe_partitioner.assignments(&left_batch)?;
                let mut buckets: Vec<Vec<u32>> =
                    (0..n_partitions).map(|_| Vec::new()).collect();
                for (row, &p) in assignments.iter().enumerate() {
                    buckets[p as usize].push(row as u32);
                }

                let left_schema = left_batch.schema();
                // Phase Z.1b: hoist left-side TypedKeys construction
                // — same as execute_grace_shared. Saves 16× rebuild.
                let left_typed = TypedKeys::new(&left_batch, &left_keys)?;
                let left_hashes =
                    vectorized_probe_enabled().then(|| left_typed.hash_batch(left_batch.num_rows()));
                for (p, indices) in buckets.into_iter().enumerate() {
                    if indices.is_empty() {
                        continue;
                    }

                    match in_mem_hash_maps[p].as_ref() {
                        Some((right_batch, hash_map)) => {
                            // Phase Z.1 (2026-05-22): probe directly
                            // against the unmaterialised `left_batch`
                            // via row indices. `compute::take` only
                            // fires once per output slice inside
                            // `build_joined_slice` (necessary to emit
                            // the joined row), never N times per batch
                            // at routing time.
                            let (cand_left, cand_right) = multipass_collect_candidates_subset_with_left(
                                &left_typed,
                                left_hashes.as_deref(),
                                &indices,
                                right_batch,
                                hash_map,
                                &right_keys,
                            )?;
                            if cand_left.is_empty() {
                                continue;
                            }
                            for slice_start in
                                (0..cand_left.len()).step_by(MULTIPASS_OUTPUT_BATCH_ROWS)
                            {
                                let slice_end = (slice_start + MULTIPASS_OUTPUT_BATCH_ROWS)
                                    .min(cand_left.len());
                                let out = build_joined_slice(
                                    &left_batch,
                                    right_batch,
                                    &cand_left[slice_start..slice_end],
                                    &cand_right[slice_start..slice_end],
                                    &output_schema_for_stream,
                                )?;
                                yield out;
                            }
                        }
                        None => {
                            // Build partition was spilled OR empty.
                            // If empty (None and in_mem[p] was Some([])),
                            // INNER join produces no matches → drop.
                            if in_mem[p].is_none() {
                                // Truly spilled — materialize the sub
                                // batch for IPC write (necessary — Arrow
                                // IPC cannot encode raw indices).
                                let idx_array = UInt32Array::from(indices);
                                let cols: Vec<ArrayRef> = (0..left_batch.num_columns())
                                    .map(|i| {
                                        compute::take(left_batch.column(i), &idx_array, None)
                                    })
                                    .collect::<Result<_, _>>()?;
                                let probe_sub =
                                    RecordBatch::try_new(left_schema.clone(), cols)?;
                                let writer = probe_spill.get_or_insert_with(|| {
                                    PartitionedSpillWriter::new(
                                        left_schema.clone(),
                                        n_partitions,
                                        "hash_join_grace_probe",
                                    )
                                });
                                writer.write_partition(p, &probe_sub)?;
                            }
                            // Else: build partition was empty → INNER has no match → drop.
                        }
                    }
                }
            }

            let pass1_ms = t_pass1.elapsed().as_millis() as u64;

            // Pass 2: for each (spilled_build_p, spilled_probe_p) pair.
            let t_pass2 = Instant::now();
            let mut pass2_partitions: u64 = 0;
            let probe_partitions_sealed = match probe_spill {
                Some(w) => Some(w.finish()?),
                None => None,
            };
            let mut probe_partitions_taken = probe_partitions_sealed;
            for p in 0..n_partitions {
                if !partitions.has_partition(p) {
                    continue;
                }
                pass2_partitions += 1;
                let Some(probe_p_file) = probe_partitions_taken
                    .as_mut()
                    .and_then(|pp| pp.take_partition(p))
                else {
                    // Build partition spilled but no probe rows landed
                    // there — INNER join produces nothing for this pair.
                    let _ = partitions.take_partition(p);
                    continue;
                };
                let build_p_file = partitions
                    .take_partition(p)
                    .expect("has_partition(p) → take must succeed");

                // Load build partition into memory.
                let build_reader = build_p_file.open_reader()?;
                let mut build_batches: Vec<RecordBatch> = Vec::new();
                for b in build_reader {
                    build_batches.push(b?);
                }
                drop(build_p_file);
                if build_batches.is_empty() {
                    continue;
                }
                let combined = if build_batches.len() == 1 {
                    build_batches.into_iter().next().unwrap()
                } else {
                    compute::concat_batches(&right_schema, build_batches.iter())?
                };
                if combined.num_rows() == 0 {
                    continue;
                }
                let hash_map = JoinHashMap::build_single(&combined, &right_keys)?;

                tracing::info!(
                    target: "arneb::mem",
                    operator = "HashJoinExec",
                    grace_partition = p,
                    right_rows = combined.num_rows(),
                    "grace pass-2: loaded build partition + opening probe partition",
                );

                // Stream the probe partition file through probe().
                let probe_reader = probe_p_file.open_reader()?;
                for probe_batch_res in probe_reader {
                    let probe_batch = probe_batch_res?;
                    if probe_batch.num_rows() == 0 {
                        continue;
                    }
                    let (cand_left, cand_right) = multipass_collect_candidates(
                        &probe_batch,
                        &combined,
                        &hash_map,
                        &left_keys,
                        &right_keys,
                    )?;
                    if cand_left.is_empty() {
                        continue;
                    }
                    for slice_start in
                        (0..cand_left.len()).step_by(MULTIPASS_OUTPUT_BATCH_ROWS)
                    {
                        let slice_end =
                            (slice_start + MULTIPASS_OUTPUT_BATCH_ROWS).min(cand_left.len());
                        let out = build_joined_slice(
                            &probe_batch,
                            &combined,
                            &cand_left[slice_start..slice_end],
                            &cand_right[slice_start..slice_end],
                            &output_schema_for_stream,
                        )?;
                        yield out;
                    }
                }
                // Drop partition state before next p.
                drop(hash_map);
                drop(combined);
                drop(probe_p_file);
            }

            tracing::info!(
                target: "arneb::profile",
                op = "HashJoinExec.execute_grace_inner",
                n_partitions = n_partitions as u64,
                in_mem_partitions,
                in_mem_build_ms,
                pass1_ms,
                pass1_probe_rows,
                pass2_ms = t_pass2.elapsed().as_millis() as u64,
                pass2_partitions,
                total_ms = t_grace.elapsed().as_millis() as u64,
                "HashJoinExec grace done"
            );
        };

        Ok(Box::pin(AsyncBatchStream {
            schema: output_schema,
            inner: Box::pin(inner),
        }))
    }

    /// Parallel cache-fit probe (intra-worker probe parallelism). The
    /// no-spill counterpart of `execute_grace_inner`: the build is fully
    /// in memory (16 cache-resident partitions), so instead of draining
    /// the probe input on a single core, collect it, split into
    /// `threads` row-chunks, and probe them concurrently on the blocking
    /// pool against the shared `Arc` build. Each chunk returns owned
    /// output batches — no inter-task channel, no streaming backpressure
    /// → deadlock-free. Closes the Q09 gap where the 14-core VM idled
    /// while one worker's 30M-row probe ran on ~1 core.
    ///
    /// Only valid for the no-spill case (every `in_mem[p]` populated or
    /// empty, never spilled). Output order is irrelevant (the join feeds
    /// an aggregate), so chunks merge arbitrarily.
    async fn execute_grace_inmem_parallel(
        &self,
        in_mem: Vec<Option<Vec<RecordBatch>>>,
        n_partitions: usize,
        right_schema: Arc<Schema>,
        left_batches: Vec<RecordBatch>,
        threads: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let output_schema = column_info_to_arrow_schema(&self.schema());

        // Pre-build the per-partition (RecordBatch, JoinHashMap) pairs
        // once; share read-only across the probe chunks via Arc.
        let mut hms: Vec<Option<(RecordBatch, JoinHashMap)>> = Vec::with_capacity(n_partitions);
        for batches_opt in in_mem.into_iter() {
            match batches_opt {
                Some(batches) if !batches.is_empty() => {
                    let combined = if batches.len() == 1 {
                        batches.into_iter().next().unwrap()
                    } else {
                        compute::concat_batches(&right_schema, batches.iter())?
                    };
                    if combined.num_rows() == 0 {
                        hms.push(None);
                        continue;
                    }
                    let hm = JoinHashMap::build_single(&combined, &self.right_keys)?;
                    hms.push(Some((combined, hm)));
                }
                _ => hms.push(None),
            }
        }
        let in_mem_hash_maps = Arc::new(hms);
        let probe_partitioner =
            Arc::new(HashPartitioner::new(self.left_key_exprs(), n_partitions)?);
        let left_keys = Arc::new(self.left_keys.clone());
        let right_keys = Arc::new(self.right_keys.clone());

        // `left_batches` is already collected by the caller (which counted
        // its rows to decide parallel-vs-sequential).
        // Finer chunks than threads so `buffer_unordered(threads)` keeps
        // only ~threads chunk-outputs in flight (drained continuously by
        // the downstream consumer) instead of materialising the whole
        // join output at once.
        let chunks = split_batches_even(left_batches, threads * PROBE_CHUNK_FACTOR);
        let os = output_schema.clone();

        let inner = async_stream::try_stream! {
            let t_probe = Instant::now();
            let mut tasks = futures::stream::iter(chunks.into_iter().map(|chunk| {
                let imhm = Arc::clone(&in_mem_hash_maps);
                let pp = Arc::clone(&probe_partitioner);
                let lk = Arc::clone(&left_keys);
                let rk = Arc::clone(&right_keys);
                let osc = os.clone();
                tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, ExecutionError> {
                    let mut out = Vec::new();
                    for b in &chunk {
                        probe_partition_batch_inmem(b, &imhm, &pp, &lk, &rk, &osc, n_partitions, &mut out)?;
                    }
                    Ok(out)
                })
            }))
            .buffer_unordered(threads);

            let mut out_batches = 0u64;
            while let Some(res) = tasks.next().await {
                let part = res.map_err(|e| {
                    ExecutionError::InvalidOperation(format!("grace parallel probe task join: {e}"))
                })??;
                for b in part {
                    out_batches += 1;
                    yield b;
                }
            }
            tracing::info!(
                target: "arneb::profile",
                op = "HashJoinExec.execute_grace_inmem_parallel",
                threads,
                n_partitions = n_partitions as u64,
                out_batches,
                probe_ms = t_probe.elapsed().as_millis() as u64,
                "parallel cache-fit probe done"
            );
        };
        Ok(Box::pin(AsyncBatchStream {
            schema: output_schema,
            inner: Box::pin(inner),
        }))
    }

    /// Build (or fetch the cached) `BuildState`. Single-flight: only
    /// the first concurrent caller builds; the rest await the result.
    ///
    /// Phase 3b (2026-05-21): build phase now streams batches and
    /// spills to disk on memory pool exhaustion. After EOF:
    ///   - All right batches in memory (no spill): existing fast
    ///     path — concat + JoinHashMap → `BuildSide::Single`.
    ///   - One or more chunks spilled: `BuildSide::Multipass`
    ///     carries the chunks; probe walks them one at a time.
    ///
    /// Residual + spill returns `ResourceExhausted` (multipass
    /// residual support deferred to a follow-up like Phase 2b.2).
    /// LEFT/RIGHT/FULL + spill also returns `ResourceExhausted`
    /// (outer joins need cross-chunk `right_matched` state — also
    /// deferred).
    async fn ensure_built(&self) -> Result<Arc<BuildState>, ExecutionError> {
        self.build_state
            .get_or_try_init(|| async {
                let right_stream = self.right.execute(0).await?;

                // Non-INNER (or residual) builds can't currently be
                // probed across spilled chunks — fall back to the
                // pre-spill collect-and-concat path so we don't trip
                // gate_multipass on queries like TPC-H Q13 that fit
                // in container memory but exceed the pool budget.
                if !self.multipass_supported() {
                    let right_batches = collect_stream(right_stream).await.map_err(|e| {
                        ExecutionError::InvalidOperation(format!(
                            "hash join right collect: {e}"
                        ))
                    })?;
                    return self.build_state_from_collected(right_batches);
                }

                // Phase M.2c (2026-05-22): when Grace HJ is enabled,
                // hash-partition the build side once under the OnceCell
                // single-flight guard. All N probe partitions then
                // share the pre-built per-partition hash tables and
                // spill files via `BuildSide::Partitioned` — no
                // 4× build duplication that M.2b suffered on Q09's
                // 6-way joins (the per-partition independent Grace
                // dispatch path).
                if self.grace_enabled() {
                    return self.build_grace_shared(right_stream).await;
                }

                let result = build_with_spill(
                    right_stream,
                    Arc::clone(&self.memory_pool),
                    "HashJoinExec.build",
                )
                .await?;

                match result {
                    BuildChunksResult::Multipass {
                        chunks,
                        schema,
                        total_bytes,
                        total_rows,
                    } => {
                        self.peak_build_bytes
                            .store(total_bytes, std::sync::atomic::Ordering::Relaxed);
                        self.gate_multipass(chunks.len(), total_bytes)?;
                        self.emit_build_profile(0, total_rows, total_bytes, "Multipass");
                        tracing::info!(
                            target: "arneb::mem",
                            operator = "HashJoinExec",
                            join_type = ?self.join_type,
                            has_residual = self.residual.is_some(),
                            build_bytes = total_bytes,
                            pool_reserved = self.memory_pool.reserved(),
                            spilled_chunks = chunks.len(),
                            "hash join build complete (multipass)",
                        );
                        Ok::<Arc<BuildState>, ExecutionError>(Arc::new(BuildState {
                            side: BuildSide::Multipass {
                                chunks: Arc::new(chunks),
                                right_schema: schema,
                            },
                        }))
                    }
                    BuildChunksResult::Single {
                        batches,
                        schema,
                        total_bytes,
                        total_rows,
                        reservation,
                    } => {
                        self.peak_build_bytes
                            .store(total_bytes, std::sync::atomic::Ordering::Relaxed);
                        tracing::info!(
                            target: "arneb::mem",
                            operator = "HashJoinExec",
                            join_type = ?self.join_type,
                            right_rows = total_rows,
                            has_residual = self.residual.is_some(),
                            build_bytes = total_bytes,
                            pool_reserved = self.memory_pool.reserved(),
                            spilled_chunks = 0,
                            "hash join build complete",
                        );

                        let right_combined = if batches.is_empty() {
                            None
                        } else if batches.len() == 1 {
                            Some(batches.into_iter().next().unwrap())
                        } else {
                            let s = schema.clone().expect("schema set when batches non-empty");
                            Some(compute::concat_batches(&s, batches.iter())?)
                        };
                        let _ = reservation;
                        self.build_state_inner(right_combined)
                    }
                    BuildChunksResult::Partitioned { .. } => {
                        // Phase 3b.5c stub: `ensure_built` calls `build_with_spill`
                        // (the non-partitioning helper), so this arm is
                        // unreachable here. Kept exhaustive for future routing.
                        Err(ExecutionError::InvalidOperation(
                            "Partitioned build not yet wired into ensure_built (Phase 3b.5d pending)".into(),
                        ))
                    }
                }
            })
            .await
            .cloned()
    }

    /// Pre-spill fallback: take all right batches in memory and
    /// finish building the `BuildState::Single` (concat + hash map +
    /// DF injection). Used by `ensure_built` when the join shape
    /// can't multipass.
    fn build_state_from_collected(
        &self,
        right_batches: Vec<RecordBatch>,
    ) -> Result<Arc<BuildState>, ExecutionError> {
        let total_bytes: usize = right_batches
            .iter()
            .map(crate::operator::record_batch_bytes)
            .sum();
        self.peak_build_bytes
            .store(total_bytes, std::sync::atomic::Ordering::Relaxed);
        let right_combined = if right_batches.is_empty() {
            None
        } else if right_batches.len() == 1 {
            Some(right_batches.into_iter().next().unwrap())
        } else {
            Some(compute::concat_batches(
                &right_batches[0].schema(),
                right_batches.iter(),
            )?)
        };
        tracing::info!(
            target: "arneb::mem",
            operator = "HashJoinExec",
            join_type = ?self.join_type,
            right_rows = right_combined.as_ref().map(|b| b.num_rows()).unwrap_or(0),
            has_residual = self.residual.is_some(),
            build_bytes = total_bytes,
            spilled_chunks = 0,
            multipass_supported = false,
            "hash join build complete (no-spill fallback)",
        );
        self.build_state_inner(right_combined)
    }

    /// A1.7 (2026-05-27): publish a cross-fragment Domain to the
    /// coordinator for each declared `DynamicFilterProducer` from
    /// the build side of this partition. Called from every build
    /// path (build_state_inner, execute_per_partition's `Single`
    /// arm, execute_per_partition_no_spill, execute_single_finish,
    /// execute_single_no_spill). Gated on the feature flag +
    /// publisher presence + left_filterable + `ARNEB_DISABLE_DF`
    /// kill-switch.
    ///
    /// `right_combined` is the materialised right-side batch (None
    /// or zero-row → empty DistinctValues so the coord's per-
    /// partition counter still advances and the consumer doesn't
    /// time out on an empty-result query).
    fn emit_cross_fragment_dfs(&self, right_combined: Option<&RecordBatch>) {
        let left_filterable = matches!(self.join_type, ast::JoinType::Inner | ast::JoinType::Right);
        let df_disabled = std::env::var_os("ARNEB_DISABLE_DF").is_some();
        if !self.dynamic_filtering_enabled
            || !left_filterable
            || df_disabled
            || self.dynamic_filter_producers.is_empty()
            || !broadcast_df_enabled()
        {
            return;
        }
        let Some(publisher) = &self.dynamic_filter_publisher else {
            return;
        };
        let domains: Vec<(arneb_common::DynamicFilterId, arneb_common::Domain)> =
            match right_combined {
                Some(b) if b.num_rows() > 0 => self
                    .dynamic_filter_producers
                    .iter()
                    .map(|p| {
                        let arr = b.column(p.build_index);
                        (
                            p.id,
                            crate::dynamic_filter_publisher::build_partition_domain_for_column(arr),
                        )
                    })
                    .collect(),
                _ => self
                    .dynamic_filter_producers
                    .iter()
                    .map(|p| (p.id, arneb_common::Domain::DistinctValues(Vec::new())))
                    .collect(),
            };
        let publisher = publisher.clone();
        tokio::spawn(async move {
            for (id, domain) in domains {
                publisher.publish(id, domain).await;
            }
        });
    }

    /// Shared inner helper: from a (possibly empty) `right_combined`
    /// batch, build the JoinHashMap, run DF injection, and return
    /// the `BuildState::Single`. Called by both the spill and
    /// no-spill ensure_built paths.
    fn build_state_inner(
        &self,
        right_combined: Option<RecordBatch>,
    ) -> Result<Arc<BuildState>, ExecutionError> {
        let hash_map = match &right_combined {
            Some(b) if b.num_rows() > 0 => JoinHashMap::build_single(b, &self.right_keys)?,
            _ => JoinHashMap::empty(),
        };
        let hash_map = Arc::new(hash_map);

        // Step DF2 (2026-05-15): only INNER and RIGHT joins allow
        // filtering the left side; LEFT/FULL preserve unmatched left
        // rows so DF would incorrectly drop them.
        let left_filterable = matches!(self.join_type, ast::JoinType::Inner | ast::JoinType::Right);
        let df_disabled = std::env::var_os("ARNEB_DISABLE_DF").is_some();
        if left_filterable && !df_disabled {
            if let Some(right_batch) = &right_combined {
                let left_schema = self.left.schema();
                inject_inlist_dynamic_filters(
                    self.left.as_ref(),
                    right_batch,
                    &self.right_keys,
                    &self.left_keys,
                    &left_schema,
                    &self.df_targets,
                );
            }
        }

        // A1.5/A1.7 (2026-05-27): cross-fragment DF emit. Extracted
        // to a helper so non-Grace build paths (execute_per_partition's
        // BuildChunksResult::Single arm, execute_per_partition_no_spill,
        // execute_single_finish, execute_single_no_spill) can call
        // it too without code duplication. The helper internally
        // gates on flag + producers + publisher + left_filterable +
        // the ARNEB_DISABLE_DF kill-switch.
        self.emit_cross_fragment_dfs(right_combined.as_ref());
        self.emit_single_build_profile(0, right_combined.as_ref(), &hash_map);

        Ok(Arc::new(BuildState {
            side: BuildSide::Single {
                right_combined,
                hash_map,
            },
        }))
    }

    /// Phase M.2c (2026-05-22): Grace HJ build under `ensure_built`'s
    /// single-flight `OnceCell`. Hash-partitions the right side once,
    /// pre-builds per-partition `JoinHashMap`s for the in-memory
    /// buckets, and returns `BuildSide::Partitioned` carrying the
    /// `Arc`-shared state. All N probe partitions share this state via
    /// `execute_grace_shared`, replacing M.2b's per-partition
    /// independent build (which duplicated build work N times and
    /// caused Q09 to OOM on partsupp's 4× build).
    async fn build_grace_shared(
        &self,
        right_stream: SendableRecordBatchStream,
    ) -> Result<Arc<BuildState>, ExecutionError> {
        let n_partitions = grace_partition_count();
        let build_partitioner =
            Arc::new(HashPartitioner::new(self.right_key_exprs(), n_partitions)?);
        let outcome = build_with_partitioned_spill_collecting_df(
            right_stream,
            Arc::clone(&self.memory_pool),
            "HashJoinExec.grace_build",
            build_partitioner,
            &self.right_keys,
        )
        .await?;
        let build_result = outcome.result;
        let df_collectors = outcome.df_collectors;

        // Inject DF before probe runs (matches execute_grace_single's
        // contract — INNER + no residual is the only join shape Grace
        // is gated on, so left_filterable is always true here).
        let left_filterable = matches!(self.join_type, ast::JoinType::Inner | ast::JoinType::Right);
        let df_disabled = std::env::var_os("ARNEB_DISABLE_DF").is_some();
        if left_filterable && !df_disabled && !df_collectors.is_empty() {
            let publish_cross_fragment = !matches!(build_result, BuildChunksResult::Single { .. });
            self.inject_grace_dynamic_filters(df_collectors, publish_cross_fragment);
        }

        match build_result {
            BuildChunksResult::Partitioned {
                in_mem,
                partitions,
                schema: right_schema,
                total_bytes,
                partition_rows,
                partitioner: _,
            } => {
                self.peak_build_bytes
                    .store(total_bytes, std::sync::atomic::Ordering::Relaxed);

                // Pre-build per-partition (combined, hash_map) pairs
                // once. Probe partitions then look up immutably — no
                // re-build per probe partition. Spilled partitions get
                // `None` here; their hash tables are constructed on
                // demand per probe partition during Pass 2.
                let mut in_mem_hash_maps: Vec<Option<(RecordBatch, JoinHashMap)>> =
                    Vec::with_capacity(n_partitions);
                for batches_opt in in_mem.into_iter() {
                    match batches_opt {
                        Some(batches) if !batches.is_empty() => {
                            let combined = if batches.len() == 1 {
                                batches.into_iter().next().unwrap()
                            } else {
                                compute::concat_batches(&right_schema, batches.iter())?
                            };
                            if combined.num_rows() == 0 {
                                in_mem_hash_maps.push(None);
                                continue;
                            }
                            let hm = JoinHashMap::build_single(&combined, &self.right_keys)?;
                            in_mem_hash_maps.push(Some((combined, hm)));
                        }
                        _ => in_mem_hash_maps.push(None),
                    }
                }
                self.emit_partitioned_build_profile_from_maps(&in_mem_hash_maps, &partition_rows);

                tracing::info!(
                    target: "arneb::mem",
                    operator = "HashJoinExec",
                    path = "build_grace_shared",
                    build_bytes = total_bytes,
                    pool_reserved = self.memory_pool.reserved(),
                    spilled_partitions = (0..n_partitions)
                        .filter(|&p| partitions.has_partition(p))
                        .count(),
                    n_partitions,
                    "grace shared build complete",
                );

                Ok(Arc::new(BuildState {
                    side: BuildSide::Partitioned {
                        in_mem_hash_maps: Arc::new(in_mem_hash_maps),
                        partitions: Arc::new(partitions),
                        right_schema,
                        n_partitions,
                    },
                }))
            }
            BuildChunksResult::Single {
                batches,
                schema,
                total_bytes,
                total_rows: _,
                reservation,
            } => {
                // Build fit in memory — no spill, no Grace machinery
                // needed. Fall through to the standard Single path.
                self.peak_build_bytes
                    .store(total_bytes, std::sync::atomic::Ordering::Relaxed);
                let right_combined = if batches.is_empty() {
                    None
                } else if batches.len() == 1 {
                    Some(batches.into_iter().next().unwrap())
                } else {
                    let s = schema.clone().expect("schema set when batches non-empty");
                    Some(compute::concat_batches(&s, batches.iter())?)
                };
                let _ = reservation;
                self.build_state_inner(right_combined)
            }
            BuildChunksResult::Multipass { .. } => Err(ExecutionError::InvalidOperation(
                "build_with_partitioned_spill returned Multipass; expected Partitioned or Single"
                    .into(),
            )),
        }
    }

    /// Gate outer joins + residual against the multipass build path
    /// until follow-up phases land cross-chunk `right_matched` state +
    /// residual-aware multipass.
    fn gate_multipass(&self, n_chunks: usize, total_bytes: usize) -> Result<(), ExecutionError> {
        if self.residual.is_some() {
            return Err(ExecutionError::ResourceExhausted(format!(
                "HashJoinExec: build exceeded memory budget but residual + spill \
                 is not yet supported. spilled_chunks={n_chunks}, build_bytes={total_bytes}",
            )));
        }
        if !matches!(self.join_type, ast::JoinType::Inner) {
            return Err(ExecutionError::ResourceExhausted(format!(
                "HashJoinExec: build exceeded memory budget but spill + {:?} \
                 outer joins are not yet supported. spilled_chunks={n_chunks}, build_bytes={total_bytes}",
                self.join_type,
            )));
        }
        Ok(())
    }

    /// LEFT join with empty right: each partition emits its own left
    /// rows padded with NULLs on the right side. INNER returns empty.
    async fn handle_empty_right_partition(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let schema = column_info_to_arrow_schema(&self.schema());
        if !matches!(self.join_type, ast::JoinType::Left) {
            return Ok(stream_from_batches(schema, vec![]));
        }
        let left_stream = self.left.execute(partition).await?;
        let left_batches = collect_stream(left_stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("hash join left collect: {e}"))
        })?;
        if left_batches.is_empty() {
            return Ok(stream_from_batches(schema, vec![]));
        }
        let left_combined = if left_batches.len() == 1 {
            left_batches.into_iter().next().unwrap()
        } else {
            compute::concat_batches(&left_batches[0].schema(), left_batches.iter())?
        };
        if left_combined.num_rows() == 0 {
            return Ok(stream_from_batches(schema, vec![]));
        }
        self.handle_empty_right(Some(&left_combined))
    }

    /// True when the multipass spill path can produce correct results
    /// for this join shape. Multipass uses per-chunk hash-table build
    /// + `self.probe`, which:
    ///   - Correctly handles equi-match emission for INNER joins.
    ///   - Cannot reconstruct cross-chunk `left_matched`/`right_matched`
    ///     state for LEFT/RIGHT/FULL outer joins.
    ///   - Could in theory handle a residual via `probe`, but the
    ///     existing gate is conservative — and crucially, falling
    ///     back to no-spill preserves pre-3b.3 behaviour for queries
    ///     like TPC-H Q13 (LEFT + residual) instead of regressing
    ///     them to ResourceExhausted.
    fn multipass_supported(&self) -> bool {
        matches!(self.join_type, ast::JoinType::Inner) && self.residual.is_none()
    }

    /// True when Grace Hash Join's partitioned-spill path should run
    /// instead of the chunk-multipass path. INNER + no-residual + the
    /// env opt-in `ARNEB_GRACE_HJ=1`. Default-off until Phase 3b.5g
    /// flips the switch.
    fn grace_enabled(&self) -> bool {
        self.multipass_supported() && std::env::var_os("ARNEB_GRACE_HJ").is_some()
    }

    /// Build `Vec<PlanExpr>` of column-references for the right-side
    /// join keys. Used to construct the build-side `HashPartitioner`.
    fn right_key_exprs(&self) -> Vec<PlanExpr> {
        let right_schema = self.right.schema();
        self.right_keys
            .iter()
            .map(|&idx| PlanExpr::Column {
                index: idx,
                name: right_schema[idx].name.clone(),
                span: None,
            })
            .collect()
    }

    /// Build `Vec<PlanExpr>` of column-references for the left-side
    /// join keys. Used to construct the probe-side `HashPartitioner`,
    /// which must produce the same partition id as the build-side
    /// partitioner for matching key values (same hash function, same
    /// modulus, same key value flow).
    fn left_key_exprs(&self) -> Vec<PlanExpr> {
        let left_schema = self.left.schema();
        self.left_keys
            .iter()
            .map(|&idx| PlanExpr::Column {
                index: idx,
                name: left_schema[idx].name.clone(),
                span: None,
            })
            .collect()
    }

    /// Single-partition entry point (RIGHT/FULL or N=1 left).
    ///
    /// Phase 3b.3 (2026-05-21): INNER joins without residual stream the
    /// right side through `build_with_spill` so large builds spill to
    /// disk instead of OOM-killing the worker. Other join shapes fall
    /// back to the pre-3b.3 collect-then-concat path — multipass can't
    /// reconstruct outer-join `matched` state across chunks, and
    /// failing fast there would regress queries that fit in container
    /// memory but not the spill pool budget (e.g. TPC-H Q13).
    async fn execute_single(
        &self,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let t_execute = Instant::now();
        let left_stream = self.left.execute(0).await?;
        let right_stream = self.right.execute(0).await?;

        if !self.multipass_supported() {
            return self
                .execute_single_no_spill(left_stream, right_stream)
                .await;
        }

        // Phase 3b.5d (2026-05-21): when ARNEB_GRACE_HJ=1, route INNER
        // joins through the partitioned-spill builder + Grace Hash Join
        // execute. Default off — flips on in 3b.5g once distributed
        // Q05/Q07/Q09 are verified.
        if self.grace_enabled() {
            return self.execute_grace_single(left_stream, right_stream).await;
        }

        let t_build = Instant::now();
        let build_result = build_with_spill(
            right_stream,
            Arc::clone(&self.memory_pool),
            "HashJoinExec.single_build",
        )
        .await?;
        let build_ms = t_build.elapsed().as_millis() as u64;

        let t_left = Instant::now();
        // exec-memory-accounting D3: track the probe-side collect against the
        // pool (was untracked `collect_stream`). `left_reservation` is held
        // alive through the probe via `hold_reservations` at the end.
        let (left_batches, left_reservation) = collect_stream_pool_tracked(
            left_stream,
            Arc::clone(&self.memory_pool),
            "HashJoinExec.single_left",
        )
        .await?;
        let left_collect_ms = t_left.elapsed().as_millis() as u64;
        let left_rows: u64 = left_batches.iter().map(|b| b.num_rows() as u64).sum();
        tracing::info!(
            target: "arneb::profile",
            op = "HashJoinExec.execute_single",
            join_type = ?self.join_type,
            build_ms,
            left_collect_ms,
            left_rows,
            startup_ms = t_execute.elapsed().as_millis() as u64,
            "HashJoinExec single build+left ready"
        );

        // exec-memory-accounting D3: collect reservations to HOLD through the
        // probe. Only the `Single` arm needs this — that is q18's path, where
        // the build is concatenated into one ~5 GB batch (`right_combined`) and
        // re-probed; previously the build reservation was dropped here and the
        // probe collect was untracked, so that working set was held UNTRACKED
        // through the probe (the SF30 worker-OOM retainer). The `Multipass` arm
        // already spilled the build to disk (bounded), so it keeps the pre-D3
        // behaviour and releases the probe reservation before re-probing.
        let mut held: Vec<MemoryReservation> = Vec::new();
        let stream = match build_result {
            BuildChunksResult::Multipass {
                chunks,
                schema: right_schema,
                total_bytes,
                total_rows,
            } => {
                self.gate_multipass(chunks.len(), total_bytes)?;
                self.emit_build_profile(0, total_rows, total_bytes, "Multipass");
                tracing::info!(
                    target: "arneb::mem",
                    operator = "HashJoinExec",
                    path = "execute_single",
                    build_bytes = total_bytes,
                    pool_reserved = self.memory_pool.reserved(),
                    spilled_chunks = chunks.len(),
                    "single-partition build complete (multipass)",
                );
                // Build is on disk; don't hold the probe reservation across the
                // multi-pass re-probe (would shrink the per-pass load budget).
                drop(left_reservation);
                self.execute_multipass_inner(Arc::new(chunks), right_schema, left_batches)
                    .await?
            }
            BuildChunksResult::Single {
                batches,
                schema: right_schema,
                total_bytes: _,
                total_rows: _,
                reservation,
            } => {
                // Hold BOTH the probe collect and the build through the probe
                // (covers the concatenated `right_combined`). This is the q18 fix.
                held.push(left_reservation);
                held.push(reservation);
                let right_combined = if batches.is_empty() {
                    None
                } else if batches.len() == 1 {
                    Some(batches.into_iter().next().unwrap())
                } else {
                    let rs = right_schema
                        .clone()
                        .expect("schema set when batches non-empty");
                    Some(compute::concat_batches(&rs, batches.iter())?)
                };
                // A1.7 (2026-05-27): execute_single non-Grace path —
                // publish before `execute_single_finish` runs probe.
                if let Some(right_batch) = &right_combined {
                    self.publish_broadcast_cross_fragment_dfs(0, right_batch)
                        .await;
                }
                self.execute_single_finish(left_batches, right_combined)?
            }
            BuildChunksResult::Partitioned { .. } => {
                // Phase 3b.5c stub: execute_single uses `build_with_spill`
                // (non-partitioning), so this arm is unreachable here.
                // Kept exhaustive for future routing.
                return Err(ExecutionError::InvalidOperation(
                    "Partitioned build not yet wired into execute_single (Phase 3b.5d pending)"
                        .into(),
                ));
            }
        };
        if held.is_empty() {
            Ok(stream)
        } else {
            Ok(hold_reservations(stream, held))
        }
    }

    /// Inject InList dynamic filters into the left subtree using the
    /// distinct values accumulated during partitioned build (Phase
    /// 3b.5e). Mirrors `inject_inlist_dynamic_filters` exactly, but
    /// operates on already-collected `ScalarValue` lists instead of
    /// rebuilding them from a fully materialised right batch.
    /// A1.5 (2026-05-27): emit one Domain per declared
    /// `DynamicFilterProducer` from the Grace HJ build phase, using
    /// the same per-partition collected distinct values that the in-
    /// fragment InList path uses. Each producer's `build_index` is
    /// looked up in `right_keys` to find the matching collector slot.
    fn publish_grace_cross_fragment_dfs(&self, domains_per_slot: &[arneb_common::Domain]) {
        let df_disabled = std::env::var_os("ARNEB_DISABLE_DF").is_some();
        if !self.dynamic_filtering_enabled
            || df_disabled
            || self.dynamic_filter_producers.is_empty()
        {
            return;
        }
        let Some(publisher) = &self.dynamic_filter_publisher else {
            return;
        };
        let mut domains: Vec<(arneb_common::DynamicFilterId, arneb_common::Domain)> =
            Vec::with_capacity(self.dynamic_filter_producers.len());
        for producer in &self.dynamic_filter_producers {
            let domain = match self
                .right_keys
                .iter()
                .position(|c| *c == producer.build_index)
                .and_then(|slot| domains_per_slot.get(slot))
            {
                Some(domain) => domain.clone(),
                // Do not silently skip a producer. Publishing `All` keeps the
                // coordinator rendezvous moving without risking false negatives.
                None => arneb_common::Domain::All,
            };
            if std::env::var_os("ARNEB_TRACE_DFPUB").is_some() {
                let domain_variant = match &domain {
                    arneb_common::Domain::DistinctValues(values) if values.is_empty() => {
                        "DistinctValues(empty)"
                    }
                    arneb_common::Domain::DistinctValues(_) => "DistinctValues",
                    arneb_common::Domain::Range { .. } => "Range",
                    arneb_common::Domain::Bloom(_) => "Bloom",
                    arneb_common::Domain::All => "All",
                };
                eprintln!(
                    "[DFPUBDOM] grace df_id={:?} build_index={} domain={}",
                    producer.id, producer.build_index, domain_variant
                );
            }
            domains.push((producer.id, domain));
        }
        if !domains.is_empty() {
            let publisher = publisher.clone();
            tokio::spawn(async move {
                for (id, domain) in domains {
                    publisher.publish(id, domain).await;
                }
            });
        }
    }

    async fn publish_broadcast_cross_fragment_dfs(
        &self,
        probe_partition: u32,
        right_combined: &RecordBatch,
    ) {
        let df_disabled = std::env::var_os("ARNEB_DISABLE_DF").is_some();
        if !self.dynamic_filtering_enabled
            || df_disabled
            || self.dynamic_filter_producers.is_empty()
            || !broadcast_df_enabled()
        {
            return;
        }
        let Some(publisher) = &self.dynamic_filter_publisher else {
            return;
        };
        let task_idx = publisher.task_partition_idx();
        let published = probe_partition == 0;
        if std::env::var_os("ARNEB_TRACE_DFPUB").is_some() {
            eprintln!(
                "[DFPUB] broadcast_publish_decision task_idx={} partition_idx={} probe_partition={} published={}",
                task_idx,
                task_idx,
                probe_partition,
                if published { "yes" } else { "no" }
            );
        }
        if !published {
            return;
        }
        // The build batch (`right_combined`) carries the FULL right-child
        // schema (same as the grace path's `emit_cross_fragment_dfs`), so the
        // DF column is `right_combined.column(producer.build_index)` —
        // `build_index` is the column index into that schema, NOT the slot
        // position among `right_keys`. (Earlier this used the right_keys slot
        // index, which extracted the wrong column → empty/garbage domain.)
        if right_combined.num_rows() == 0 {
            return;
        }
        for producer in &self.dynamic_filter_producers {
            let col = right_combined.column(producer.build_index).clone();
            let domain = crate::dynamic_filter_publisher::build_partition_domain_for_column(&col);
            if std::env::var_os("ARNEB_TRACE_DFPUB").is_some() {
                eprintln!(
                    "[DFPUBDOM] df_id={:?} build_index={} right_cols={} build_rows={} col_len={} domain_empty={}",
                    producer.id,
                    producer.build_index,
                    right_combined.num_columns(),
                    right_combined.num_rows(),
                    col.len(),
                    matches!(&domain, arneb_common::Domain::DistinctValues(v) if v.is_empty())
                );
            }
            let id = producer.id;
            publisher.publish_partition(id, task_idx, domain).await;
        }
    }

    fn inject_grace_dynamic_filters(
        &self,
        collectors: Vec<DfDistinctCollector>,
        publish_cross_fragment: bool,
    ) {
        // A1.5: finalise the collected distinct values once; reuse
        // them for both the same-fragment InList push (existing
        // behaviour) and the cross-fragment publish to coord.
        let domains_per_slot: Vec<arneb_common::Domain> =
            collectors.into_iter().map(|c| c.finish_domain()).collect();
        if publish_cross_fragment {
            self.publish_grace_cross_fragment_dfs(&domains_per_slot);
        }

        let left_schema = self.left.schema();
        for (i, domain) in domains_per_slot.into_iter().enumerate() {
            let arneb_common::Domain::DistinctValues(values) = domain else {
                continue;
            };
            if values.is_empty() {
                continue;
            }
            let literals: Vec<PlanExpr> = values
                .into_iter()
                .map(|v| PlanExpr::Literal {
                    value: v,
                    span: None,
                })
                .collect();

            // Provenance-targeted, same as `inject_inlist_dynamic_filters`:
            // inject at every probe-side column join-equal to this key
            // (`df_targets[i]`), by index descent, never by name. Subsumes the
            // prior direct-key + right-key "dual" injections.
            let targets = self.df_targets.get(i).map(Vec::as_slice).unwrap_or(&[]);
            for &target_idx in targets {
                let name = left_schema
                    .get(target_idx)
                    .map(|c| c.name.clone())
                    .unwrap_or_default();
                self.left.inject_dynamic_filter(
                    PlanExpr::InList {
                        expr: Box::new(PlanExpr::Column {
                            index: target_idx,
                            name,
                            span: None,
                        }),
                        list: literals.clone(),
                        negated: false,
                        span: None,
                    },
                    target_idx,
                );
            }
        }
    }

    /// Grace Hash Join entry point for the single-partition execute
    /// path (Phase 3b.5d). Builds with `build_with_partitioned_spill`,
    /// then dispatches to `execute_grace_inner` for the streaming Pass
    /// 1 + Pass 2 join. Falls through to the non-grace `Single` fast
    /// path if the build fit entirely in memory.
    async fn execute_grace_single(
        &self,
        left_stream: SendableRecordBatchStream,
        right_stream: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let n_partitions = grace_partition_count();
        let build_partitioner =
            Arc::new(HashPartitioner::new(self.right_key_exprs(), n_partitions)?);
        let outcome = build_with_partitioned_spill_collecting_df(
            right_stream,
            Arc::clone(&self.memory_pool),
            "HashJoinExec.grace_build",
            build_partitioner,
            &self.right_keys,
        )
        .await?;
        let build_result = outcome.result;
        let df_collectors = outcome.df_collectors;

        // Phase 3b.5e: inject DF before probe runs. Only fires when
        // the join shape allows filtering the left side (INNER/RIGHT;
        // Grace HJ today is gated on INNER + no residual so this is
        // always true here, but the check matches the non-grace path's
        // contract).
        let left_filterable = matches!(self.join_type, ast::JoinType::Inner | ast::JoinType::Right);
        let df_disabled = std::env::var_os("ARNEB_DISABLE_DF").is_some();
        if left_filterable && !df_disabled && !df_collectors.is_empty() {
            let publish_cross_fragment = !matches!(build_result, BuildChunksResult::Single { .. });
            self.inject_grace_dynamic_filters(df_collectors, publish_cross_fragment);
        }

        match build_result {
            BuildChunksResult::Partitioned {
                in_mem,
                partitions,
                schema: right_schema,
                total_bytes,
                partition_rows,
                partitioner: _,
            } => {
                self.emit_partitioned_build_profile_from_batches(&in_mem, &partition_rows);
                let spilled_partitions = (0..partitions.n_partitions())
                    .filter(|&p| partitions.has_partition(p))
                    .count();
                tracing::info!(
                    target: "arneb::mem",
                    operator = "HashJoinExec",
                    path = "execute_grace_single",
                    build_bytes = total_bytes,
                    spilled_partitions,
                    n_partitions,
                    "grace build complete",
                );
                // Cache-fit (no spill): the build is fully in memory.
                //
                // With `ARNEB_PROBE_THREADS > 1`, take the parallel probe
                // path — split the in-memory probe input into row-chunks
                // and probe them concurrently against the shared build
                // (the 14-core VM otherwise idles while one core drains a
                // 30M-row probe). Deadlock-free: chunks share an Arc build
                // and return owned batches, no streaming coordination.
                //
                // Otherwise (sequential, or real spill): `execute_grace_inner`
                // interleaves `left_stream.next()` with `yield` in Pass 1,
                // which deadlocks under OutputBuffer backpressure once the
                // probe is large (the streaming refactor hit this on
                // Q03/Q09; see 2026-05-23 notes). So for the no-spill case
                // we drain the left side FULLY first — exactly what the
                // Single fast path does — then feed Pass 1 an in-memory
                // stream, decoupling input from the output yield. Real-spill
                // builds keep the streaming left (bounded memory matters
                // there and the spill scale doesn't hit the deadlock).
                if spilled_partitions == 0 {
                    // Collect the probe input only up to the pool budget
                    // (`try_grow` is the pressure signal — the D2 design). A
                    // probe that FITS → fast in-memory/parallel path, unchanged
                    // (reservation held through the output stream). A probe
                    // that OVERFLOWS (q18's ~10 GB lineitem⋈orders intermediate)
                    // → bounded STREAMING probe via `execute_grace_inner` (the
                    // same proven path the real-spill case uses), fed the
                    // already-pulled prefix in front of the remainder. Stops the
                    // cache-fit path from materialising the whole probe and
                    // OOM-killing the worker (heap-profile attribution
                    // 2026-06-08; see project_2026-06-08_q18_oom_heapprofile_rootcause).
                    match collect_probe_within_budget(
                        left_stream,
                        Arc::clone(&self.memory_pool),
                        "HashJoinExec.grace_probe",
                        probe_collect_max_bytes(),
                    )
                    .await?
                    {
                        ProbeCollect::Fits {
                            batches: left_batches,
                            reservation,
                        } => {
                            let probe_rows: u64 =
                                left_batches.iter().map(|b| b.num_rows() as u64).sum();
                            // Composite-key cache-fit build (already gated on
                            // >16 MB by the cache-fit threshold, so the build
                            // gate is effectively always satisfied here).
                            let build_rows: u64 = in_mem
                                .iter()
                                .flatten()
                                .flat_map(|v| v.iter())
                                .map(|b| b.num_rows() as u64)
                                .sum();
                            let out = if use_parallel_probe(probe_rows, build_rows) {
                                self.execute_grace_inmem_parallel(
                                    in_mem,
                                    n_partitions,
                                    right_schema,
                                    left_batches,
                                    probe_threads(),
                                )
                                .await?
                            } else {
                                let left_schema = column_info_to_arrow_schema(&self.left.schema());
                                let left_for_probe = stream_from_batches(left_schema, left_batches);
                                self.execute_grace_inner(
                                    in_mem,
                                    partitions,
                                    n_partitions,
                                    right_schema,
                                    left_for_probe,
                                )
                                .await?
                            };
                            // Hold the probe reservation until the output
                            // stream drops — honest accounting for the
                            // in-flight cache-fit probe batches.
                            return Ok(hold_reservations(out, vec![reservation]));
                        }
                        ProbeCollect::Overflow { prefix, rest } => {
                            tracing::info!(
                                target: "arneb::mem",
                                operator = "HashJoinExec",
                                path = "execute_grace_single",
                                "probe exceeded pool budget; streaming probe (bounded)",
                            );
                            let left_schema = column_info_to_arrow_schema(&self.left.schema());
                            let left_for_probe = prepend_batches(left_schema, prefix, rest);
                            return self
                                .execute_grace_inner(
                                    in_mem,
                                    partitions,
                                    n_partitions,
                                    right_schema,
                                    left_for_probe,
                                )
                                .await;
                        }
                    }
                }
                self.execute_grace_inner(
                    in_mem,
                    partitions,
                    n_partitions,
                    right_schema,
                    left_stream,
                )
                .await
            }
            BuildChunksResult::Single {
                batches,
                schema: right_schema,
                total_bytes: _,
                total_rows: _,
                reservation: build_reservation,
            } => {
                // Build fit in memory as one chunk. q18's single-key joins
                // land HERE (they never qualify for the composite-key cache-
                // fit partition path), so this — not the Partitioned arm — is
                // q18's hot probe site. Budget-gate the probe collect: a probe
                // that FITS the pool → the fast collect-based
                // `execute_single_finish` (unchanged, now reservation-held); a
                // probe that OVERFLOWS (q18's ~10 GB lineitem⋈orders probe
                // intermediate) → bounded STREAMING probe against the single
                // build via `execute_grace_inner` (1 implicit partition, empty
                // spill file — the same proven streaming path the real-spill
                // case uses), fed the collected prefix + remainder. Stops the
                // unbounded collect that OOM-killed the worker (heap-profile
                // attribution 2026-06-08; project_2026-06-08_q18_oom_heapprofile_rootcause).
                //
                // q09 SF30 cgroup-OOM retainer (2026-06-11): HOLD the build
                // reservation (`build_reservation`) through the probe. The
                // build is concatenated into `right_combined` and probed
                // against while the output stream is alive; dropping its
                // reservation here (the old `let _ = reservation`) left that
                // in-memory working set UNTRACKED, so the pool under-counted,
                // let the probe + sibling joins grow into the "free" budget,
                // and RSS overshot the cgroup cap. The non-grace
                // `execute_single` already holds it (D3, 2026-06-04); this is
                // the same fix for the grace path q09 takes (ARNEB_GRACE_HJ=1).
                //
                // q09 SF30 ROOT FIX (2026-06-11): for a MULTI-batch INNER,
                // no-residual build, skip the `right_combined` concat entirely.
                // The inuse_space heap profile pinned the SF30 peak to that
                // concat (`arrow_select::concat` = 5.3 GB of the 6.9 GB live
                // set) — `concat_batches` holds the input batches AND the
                // output batch simultaneously, a ~2× spike on the ~5 GB build
                // that hit the 11 GB cgroup cap. The reservation-hold above
                // makes the build TRACKED but cannot stop the materialisation.
                // `execute_single_finish_streaming_multi` builds the hash table
                // over the batches in place and gathers the right output with
                // `compute::interleave` — no concat, no doubling. Gated on the
                // grace single-build shape (INNER + no residual); single-batch
                // builds (len <= 1) never concat so they keep the path below.
                if batches.len() > 1
                    && matches!(self.join_type, ast::JoinType::Inner)
                    && self.residual.is_none()
                {
                    let out = self.execute_single_finish_streaming_multi(left_stream, batches)?;
                    return Ok(hold_reservations(out, vec![build_reservation]));
                }
                let right_combined = if batches.is_empty() {
                    None
                } else if batches.len() == 1 {
                    Some(batches.into_iter().next().unwrap())
                } else {
                    let rs = right_schema
                        .clone()
                        .expect("schema set when batches non-empty");
                    Some(compute::concat_batches(&rs, batches.iter())?)
                };
                match collect_probe_within_budget(
                    left_stream,
                    Arc::clone(&self.memory_pool),
                    "HashJoinExec.grace_single_probe",
                    probe_collect_max_bytes(),
                )
                .await?
                {
                    ProbeCollect::Fits {
                        batches: left_batches,
                        reservation,
                    } => {
                        let out = self.execute_single_finish(left_batches, right_combined)?;
                        // Hold BOTH the probe collect and the build
                        // (`right_combined`) through the output stream.
                        Ok(hold_reservations(out, vec![reservation, build_reservation]))
                    }
                    ProbeCollect::Overflow { prefix, rest } => match right_combined {
                        // Empty build → INNER join yields nothing; the probe is
                        // irrelevant to the OUTPUT, so don't materialise it.
                        //
                        // q21 SF30 silent-truncation fix (2026-06-12): `rest` is
                        // this partition's remaining PROBE stream — in distributed
                        // mode a remote `ExchangeExec` over an upstream producer.
                        // Dropping it un-drained (the old behaviour) closes the
                        // consumer mid-stream → the producer's `consumer_gone`
                        // path truncates the partition (the `prefix` was already
                        // pulled), and the truncation CASCADES down the shared
                        // partitioned chain, dropping rows that DO matter for
                        // other partitions → q21 returned ~62/100 wrong suppliers.
                        // So DRAIN `rest` to EOF (discard — empty build yields no
                        // output) so the upstream producer completes. `prefix` is
                        // already collected/owned; only `rest` needs draining.
                        None => {
                            let mut rest = rest;
                            while let Some(b) = rest.next().await {
                                b.map_err(|e| {
                                    ExecutionError::InvalidOperation(format!(
                                        "grace_single empty-build probe drain: {e}"
                                    ))
                                })?;
                            }
                            drop(prefix);
                            let out_schema = column_info_to_arrow_schema(&self.schema());
                            Ok(stream_from_batches(out_schema, Vec::new()))
                        }
                        Some(rc) => {
                            tracing::info!(
                                target: "arneb::mem",
                                operator = "HashJoinExec",
                                path = "execute_grace_single.single",
                                "probe exceeded budget; direct streaming probe against single build",
                            );
                            // Direct streaming probe against the single
                            // in-memory build — bounded peak (build + one
                            // in-flight batch) WITHOUT the per-row partition
                            // routing `execute_grace_inner` pays (the build is
                            // one partition; routing is pure overhead here).
                            // The probe streams (no probe reservation), but the
                            // build `right_combined` stays resident → hold its
                            // reservation through the output stream.
                            let left_schema = column_info_to_arrow_schema(&self.left.schema());
                            let left_for_probe = prepend_batches(left_schema, prefix, rest);
                            let out =
                                self.execute_single_finish_streaming(left_for_probe, Some(rc))?;
                            Ok(hold_reservations(out, vec![build_reservation]))
                        }
                    },
                }
            }
            BuildChunksResult::Multipass { .. } => {
                // build_with_partitioned_spill never returns Multipass;
                // it returns either Single (no spill) or Partitioned.
                Err(ExecutionError::InvalidOperation(
                    "build_with_partitioned_spill returned Multipass; expected Partitioned or Single".into(),
                ))
            }
        }
    }

    /// Pre-3b.3 single-partition path: collect both sides without
    /// spill, then probe. Used as the fallback for join shapes the
    /// multipass spill path can't yet support (outer joins, residual).
    async fn execute_single_no_spill(
        &self,
        left_stream: SendableRecordBatchStream,
        right_stream: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let left_batches = collect_stream(left_stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("hash join left collect: {e}"))
        })?;
        let right_batches = collect_stream(right_stream).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!("hash join right collect: {e}"))
        })?;

        let right_combined = if right_batches.is_empty() {
            None
        } else if right_batches.len() == 1 {
            Some(right_batches.into_iter().next().unwrap())
        } else {
            Some(compute::concat_batches(
                &right_batches[0].schema(),
                right_batches.iter(),
            )?)
        };
        // A1.7 (2026-05-27): single-partition non-multipass path
        // (residual / outer joins). Same emit contract as the other
        // build paths.
        self.emit_cross_fragment_dfs(right_combined.as_ref());
        self.execute_single_finish(left_batches, right_combined)
    }

    /// Shared tail of the single-partition probe path. Takes the
    /// already-collected left batches and the (optional) concatenated
    /// right batch, then returns a stream that yields one matched +
    /// optional unmatched-left batch PER input left batch, plus one
    /// final unmatched-right tail batch for RIGHT/FULL joins.
    ///
    /// Phase A streaming refactor (2026-05-23): switched from
    /// "concat all left + probe once + collect into Vec + wrap in
    /// stream_from_batches" to per-left-batch streaming via
    /// `async_stream::try_stream!`. This eliminates the OOM-shaped
    /// `concat_batches(left)` pass on multi-million-row probes (Q09's
    /// 1591-batch / 1.6M-row left was the killer), at the cost of
    /// emitting more, smaller, output batches.
    ///
    /// Was deadlock-blocked by the per-worker admission semaphore in
    /// `task_manager` until Phase A removed it (same day).
    ///
    /// Intra-worker parallel probe (`ARNEB_PROBE_THREADS > 1`, INNER/LEFT
    /// only). The flat single-key probe is otherwise single-threaded — at
    /// SF10 that left the 14-core VM idle while one core drained a 30M-row
    /// probe (Q09's HJ#1/HJ#3). Split the already-collected probe input
    /// into row-chunks, probe each concurrently on the blocking pool
    /// against the shared `Arc` build, and stream the merged output. The
    /// chunks are spawned up front (run independently to completion) then
    /// drained in order, so a back-pressuring consumer never stalls the
    /// blocking work — deadlock-free. RIGHT/FULL stay sequential (the
    /// unmatched-right tail needs a `right_matched` bitmap aggregated
    /// across every chunk); INNER/LEFT never read it cross-batch, so each
    /// chunk uses a throwaway local one.
    #[allow(clippy::too_many_arguments)]
    fn execute_single_parallel(
        &self,
        left_batches: Vec<RecordBatch>,
        right_batch: RecordBatch,
        hash_map: JoinHashMap,
        output_schema: Arc<Schema>,
        threads: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let right_batch = Arc::new(right_batch);
        let hash_map = Arc::new(hash_map);
        let left_keys = Arc::new(self.left_keys.clone());
        let right_keys = Arc::new(self.right_keys.clone());
        let residual = Arc::new(self.residual.clone());
        let join_type = self.join_type;
        let n_right = right_batch.num_rows();
        let right_rows = n_right as u64;
        let probe_input_rows: u64 = left_batches.iter().map(|b| b.num_rows() as u64).sum();
        // Finer chunks than threads so `buffer_unordered(threads)` bounds
        // peak output memory (only ~threads chunk-outputs in flight,
        // drained continuously) instead of materialising the whole join
        // output at once.
        let chunks = split_batches_even(left_batches, threads * PROBE_CHUNK_FACTOR);
        let out_schema = output_schema.clone();

        let inner = async_stream::try_stream! {
            let t_probe = Instant::now();
            let mut tasks = futures::stream::iter(chunks.into_iter().map(|chunk| {
                let rb = Arc::clone(&right_batch);
                let hm = Arc::clone(&hash_map);
                let lk = Arc::clone(&left_keys);
                let rk = Arc::clone(&right_keys);
                let res = Arc::clone(&residual);
                let os = out_schema.clone();
                tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>, ExecutionError> {
                    // INNER/LEFT don't consult `right_matched`
                    // cross-batch — local throwaway per chunk.
                    let mut right_matched = vec![false; n_right];
                    let mut out = Vec::new();
                    for b in &chunk {
                        if b.num_rows() == 0 {
                            continue;
                        }
                        out.extend(probe_one_left_batch(
                            b,
                            &rb,
                            &hm,
                            &lk,
                            &rk,
                            res.as_ref().as_ref(),
                            join_type,
                            &os,
                            &mut right_matched,
                        )?);
                    }
                    Ok(out)
                })
            }))
            .buffer_unordered(threads);

            let mut output_rows: u64 = 0;
            let mut output_batches: u64 = 0;
            while let Some(res) = tasks.next().await {
                let part = res.map_err(|e| {
                    ExecutionError::InvalidOperation(format!(
                        "single parallel probe task join: {e}"
                    ))
                })??;
                for b in part {
                    output_rows += b.num_rows() as u64;
                    output_batches += 1;
                    yield b;
                }
            }
            tracing::info!(
                target: "arneb::profile",
                op = "HashJoinExec.execute_single_parallel",
                join_type = ?join_type,
                threads,
                right_rows,
                probe_ms = t_probe.elapsed().as_millis() as u64,
                probe_input_rows,
                output_rows,
                output_batches,
                "HashJoinExec probe done (parallel)"
            );
        };

        Ok(Box::pin(AsyncBatchStream {
            schema: output_schema,
            inner: Box::pin(inner),
        }))
    }

    fn execute_single_finish(
        &self,
        left_batches: Vec<RecordBatch>,
        right_combined: Option<RecordBatch>,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let output_arrow_schema = column_info_to_arrow_schema(&self.schema());

        // Both sides empty → trivial empty stream.
        if left_batches.is_empty() && right_combined.is_none() {
            return Ok(stream_from_batches(output_arrow_schema, vec![]));
        }

        // Right side absent OR zero-row → degenerate handling. For
        // outer joins we still need to emit padded-NULL output. The
        // helper takes Option<&RecordBatch>; collapse left here so its
        // contract matches the pre-refactor caller.
        let right_batch = match right_combined {
            Some(b) if b.num_rows() > 0 => b,
            _ => {
                self.emit_build_profile(0, 0, 0, "Single");
                let left_combined = if left_batches.is_empty() {
                    None
                } else if left_batches.len() == 1 {
                    Some(left_batches.into_iter().next().unwrap())
                } else {
                    Some(compute::concat_batches(
                        &left_batches[0].schema(),
                        left_batches.iter(),
                    )?)
                };
                return self.handle_empty_right(left_combined.as_ref());
            }
        };

        // Left side absent OR all batches zero-row → handle_empty_left
        // emits padded-NULL output for RIGHT/FULL only.
        let total_left_rows: usize = left_batches.iter().map(|b| b.num_rows()).sum();
        if total_left_rows == 0 {
            return self.handle_empty_left(&right_batch);
        }

        // Build hash table once over the entire right side; reuse for
        // every left batch in the streaming probe below.
        let t_hash_build = Instant::now();
        let hash_map_single = JoinHashMap::build_single(&right_batch, &self.right_keys)?;
        let hash_build_ms = t_hash_build.elapsed().as_millis() as u64;
        let right_rows = right_batch.num_rows() as u64;
        self.emit_single_build_profile(0, Some(&right_batch), &hash_map_single);

        // Compute the joined output schema once, using any non-empty
        // left batch as the left-side template. All left batches share
        // the same schema (they're slices of the same logical input),
        // so picking the first non-empty one is fine.
        let left_template = left_batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .expect("total_left_rows > 0 implies at least one non-empty left batch");
        let output_schema = self.build_output_schema(left_template, &right_batch);

        // For RIGHT/FULL the unmatched-right tail needs a vector of
        // ArrowDataType to construct NULL columns of the right length
        // without holding a left batch reference. Pre-compute it from
        // the left template's schema.
        let left_dtypes: Vec<ArrowDataType> = left_template
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();

        // Intra-worker parallel probe (INNER/LEFT only — RIGHT/FULL need
        // the cross-chunk `right_matched` tail; small probes stay
        // sequential per `use_parallel_probe`). `left_template`'s borrow
        // of `left_batches` ends above (NLL), so the move is sound.
        if use_parallel_probe(total_left_rows as u64, right_batch.num_rows() as u64)
            && matches!(self.join_type, ast::JoinType::Inner | ast::JoinType::Left)
        {
            return self.execute_single_parallel(
                left_batches,
                right_batch,
                hash_map_single,
                output_schema,
                probe_threads(),
            );
        }

        // Clone everything the async_stream! body will need to capture
        // by move. Methods can't be called on `self` from inside the
        // generated stream (lifetime), so we drop down to free
        // functions + plain values.
        let left_keys = self.left_keys.clone();
        let right_keys = self.right_keys.clone();
        let residual = self.residual.clone();
        let join_type = self.join_type;
        let output_schema_for_stream = output_schema.clone();

        // Phase A.2 (2026-05-23): coalesce per-batch output to
        // ~STREAMING_OUTPUT_TARGET_ROWS before yielding. The pure
        // per-batch-yield streaming refactor (commit f6b8154) traded
        // Q09's OOM fix for a 27-29% regression on Q05/Q10, traced to
        // downstream consumers paying per-batch overhead × N small
        // output batches (~10× more new_null_array, RecordBatch::try_new,
        // and compute::take dispatches than the old monolithic path).
        // Accumulating output until ~1024 rows mirrors Trino's
        // `PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES` shape: big
        // probes still stream batch-by-batch (no concat_batches(left)
        // OOM), small-output joins recover the old monolithic-path
        // efficiency.
        let inner = async_stream::try_stream! {
            let t_probe = Instant::now();
            let mut probe_input_rows: u64 = 0;
            let mut output_rows: u64 = 0;
            let mut output_batches: u64 = 0;
            let mut right_matched: Vec<bool> = vec![false; right_batch.num_rows()];
            let mut pending: Vec<RecordBatch> = Vec::new();
            let mut pending_rows: usize = 0;

            for left_batch in left_batches.iter() {
                if left_batch.num_rows() == 0 {
                    continue;
                }
                probe_input_rows += left_batch.num_rows() as u64;
                let out_batches = probe_one_left_batch(
                    left_batch,
                    &right_batch,
                    &hash_map_single,
                    &left_keys,
                    &right_keys,
                    residual.as_ref(),
                    join_type,
                    &output_schema_for_stream,
                    &mut right_matched,
                )?;
                for b in out_batches {
                    pending_rows += b.num_rows();
                    pending.push(b);
                }
                if pending_rows >= STREAMING_OUTPUT_TARGET_ROWS {
                    let merged = if pending.len() == 1 {
                        pending.pop().expect("len == 1")
                    } else {
                        compute::concat_batches(&output_schema_for_stream, pending.iter())?
                    };
                    pending.clear();
                    pending_rows = 0;
                    output_rows += merged.num_rows() as u64;
                    output_batches += 1;
                    yield merged;
                }
            }

            // Flush any pending output below the target threshold.
            if !pending.is_empty() {
                let merged = if pending.len() == 1 {
                    pending.pop().expect("len == 1")
                } else {
                    compute::concat_batches(&output_schema_for_stream, pending.iter())?
                };
                output_rows += merged.num_rows() as u64;
                output_batches += 1;
                yield merged;
            }

            // RIGHT/FULL: one final unmatched-right tail batch after
            // every left batch has had a chance to update right_matched.
            if matches!(join_type, ast::JoinType::Right | ast::JoinType::Full) {
                if let Some(tail) = build_unmatched_right_tail(
                    &right_batch,
                    &right_matched,
                    &output_schema_for_stream,
                    &left_dtypes,
                )? {
                    output_rows += tail.num_rows() as u64;
                    output_batches += 1;
                    yield tail;
                }
            }

            tracing::info!(
                target: "arneb::profile",
                op = "HashJoinExec.execute_single_finish",
                join_type = ?join_type,
                hash_build_ms,
                right_rows,
                probe_ms = t_probe.elapsed().as_millis() as u64,
                probe_input_rows,
                output_rows,
                output_batches,
                "HashJoinExec probe done"
            );
        };

        Ok(Box::pin(AsyncBatchStream {
            schema: output_schema,
            inner: Box::pin(inner),
        }))
    }

    /// Streaming variant of [`execute_single_finish`] for a NO-SPILL build:
    /// build the single hash table once, then probe the left input
    /// batch-by-batch WITHOUT collecting it, keeping the probe peak bounded
    /// (build + one in-flight batch). Used when the no-spill probe overflows
    /// the collect cap (`ARNEB_PROBE_COLLECT_MAX_BYTES`) — instead of
    /// `execute_grace_inner`, which re-hashes every probe row to route it to
    /// one of N partitions (~1.8× slower for TPC-H Q08, measured 2026-06-09;
    /// a single in-memory build needs no routing). INNER/LEFT only (grace HJ
    /// is INNER-gated); the caller never reaches it for RIGHT/FULL.
    fn execute_single_finish_streaming(
        &self,
        mut left_stream: SendableRecordBatchStream,
        right_combined: Option<RecordBatch>,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let output_arrow_schema = column_info_to_arrow_schema(&self.schema());
        let right_batch = match right_combined {
            Some(b) if b.num_rows() > 0 => b,
            // INNER + empty/absent build → empty output (no padded-NULL left
            // tail needed: grace HJ is INNER-gated).
            _ => return Ok(stream_from_batches(output_arrow_schema, vec![])),
        };
        let hash_map_single = JoinHashMap::build_single(&right_batch, &self.right_keys)?;
        // Output schema from the DECLARED left schema (no batch in hand yet)
        // + the right side; a 0-row left template carries the field types.
        let left_arrow_schema = column_info_to_arrow_schema(&self.left.schema());
        let left_template = RecordBatch::new_empty(left_arrow_schema);
        let output_schema = self.build_output_schema(&left_template, &right_batch);

        let left_keys = self.left_keys.clone();
        let right_keys = self.right_keys.clone();
        let residual = self.residual.clone();
        let join_type = self.join_type;
        let output_schema_for_stream = output_schema.clone();
        let right_rows = right_batch.num_rows() as u64;

        let inner = async_stream::try_stream! {
            let t_probe = Instant::now();
            let mut probe_input_rows: u64 = 0;
            let mut output_rows: u64 = 0;
            let mut output_batches: u64 = 0;
            // INNER/LEFT never read `right_matched` cross-batch — throwaway.
            let mut right_matched: Vec<bool> = vec![false; right_rows as usize];
            let mut pending: Vec<RecordBatch> = Vec::new();
            let mut pending_rows: usize = 0;

            while let Some(batch_res) = left_stream.next().await {
                let left_batch = batch_res.map_err(|e| {
                    ExecutionError::InvalidOperation(format!(
                        "grace single streaming probe: left stream error: {e}"
                    ))
                })?;
                if left_batch.num_rows() == 0 {
                    continue;
                }
                probe_input_rows += left_batch.num_rows() as u64;
                let out_batches = probe_one_left_batch(
                    &left_batch,
                    &right_batch,
                    &hash_map_single,
                    &left_keys,
                    &right_keys,
                    residual.as_ref(),
                    join_type,
                    &output_schema_for_stream,
                    &mut right_matched,
                )?;
                for b in out_batches {
                    pending_rows += b.num_rows();
                    pending.push(b);
                }
                if pending_rows >= STREAMING_OUTPUT_TARGET_ROWS {
                    let merged = if pending.len() == 1 {
                        pending.pop().expect("len == 1")
                    } else {
                        compute::concat_batches(&output_schema_for_stream, pending.iter())?
                    };
                    pending.clear();
                    pending_rows = 0;
                    output_rows += merged.num_rows() as u64;
                    output_batches += 1;
                    yield merged;
                }
            }
            if !pending.is_empty() {
                let merged = if pending.len() == 1 {
                    pending.pop().expect("len == 1")
                } else {
                    compute::concat_batches(&output_schema_for_stream, pending.iter())?
                };
                output_rows += merged.num_rows() as u64;
                output_batches += 1;
                yield merged;
            }
            tracing::info!(
                target: "arneb::profile",
                op = "HashJoinExec.execute_single_finish_streaming",
                join_type = ?join_type,
                right_rows,
                probe_ms = t_probe.elapsed().as_millis() as u64,
                probe_input_rows,
                output_rows,
                output_batches,
                "HashJoinExec streaming probe done"
            );
        };

        Ok(Box::pin(AsyncBatchStream {
            schema: output_schema,
            inner: Box::pin(inner),
        }))
    }

    /// INNER streaming probe against a CONCAT-FREE multi-batch build
    /// ([`MultiBatchBuild`]). The build hash table is built over the build
    /// batches in place and the output right columns are gathered with
    /// `compute::interleave` — so a wide multi-GB build never doubles through
    /// `concat_batches` (the q09 SF30 OOM: the build concat held input +
    /// output live near the cgroup cap; inuse_space heap profile 2026-06-11
    /// pinned `arrow_select::concat` at 5.3 GB of the 6.9 GB live peak).
    ///
    /// INNER + no-residual only (the grace single-build shape — grace HJ is
    /// INNER-gated and q09's joins carry no residual). Probe streams batch by
    /// batch, so the peak is the build (held) + one in-flight output.
    fn execute_single_finish_streaming_multi(
        &self,
        mut left_stream: SendableRecordBatchStream,
        build_batches: Vec<RecordBatch>,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let output_arrow_schema = column_info_to_arrow_schema(&self.schema());
        // Empty build → INNER yields nothing (grace HJ is INNER-gated; no
        // padded-NULL left tail needed). `build_batches.iter().all(..)` is true
        // both for an empty Vec and for all-zero-row batches.
        if build_batches.iter().all(|b| b.num_rows() == 0) {
            self.emit_build_profile(0, 0, 0, "Single");
            // q21/q02 SF30 silent-truncation fix (2026-06-12): even though the
            // output is empty, `left_stream` is this partition's remaining
            // PROBE stream — in distributed mode a remote `ExchangeExec` over an
            // upstream producer. Dropping it un-drained (the old behaviour)
            // closes the consumer mid-stream → the producer's `consumer_gone`
            // path truncates the partition. This is the same class as the
            // `execute_grace_single` empty-build bug; here it's reachable only
            // for a multi-batch build whose rows are all empty, but distributed
            // empty batches make that possible. So DRAIN the probe to EOF
            // (discard) before ending the empty output stream. The fn is sync,
            // so the drain is deferred into the returned stream.
            let pin_schema = output_arrow_schema.clone();
            let inner = async_stream::try_stream! {
                while let Some(batch_res) = left_stream.next().await {
                    batch_res.map_err(|e| {
                        ExecutionError::InvalidOperation(format!(
                            "grace single multi-batch empty-build probe drain: {e}"
                        ))
                    })?;
                }
                // Empty INNER build → no output rows. This never-taken yield
                // only pins the stream's item type to `RecordBatch`.
                if false {
                    yield RecordBatch::new_empty(pin_schema);
                }
            };
            return Ok(Box::pin(AsyncBatchStream {
                schema: output_arrow_schema,
                inner: Box::pin(inner),
            }));
        }
        let right_schema = build_batches
            .first()
            .map(|b| b.schema())
            .expect("non-empty build implies a batch");
        let build = MultiBatchBuild::build(build_batches, &self.right_keys)?;
        let build_rows = build.total_rows();
        let build_bytes: usize = build
            .batches
            .iter()
            .map(crate::operator::record_batch_bytes)
            .sum::<usize>()
            .saturating_add(build.heap_bytes());
        self.emit_build_profile(0, build_rows, build_bytes, "Single");

        let left_arrow_schema = column_info_to_arrow_schema(&self.left.schema());
        let left_template = RecordBatch::new_empty(left_arrow_schema);
        let right_template = RecordBatch::new_empty(right_schema);
        let output_schema = self.build_output_schema(&left_template, &right_template);
        let output_schema_for_stream = output_schema.clone();
        let left_keys = self.left_keys.clone();

        let inner = async_stream::try_stream! {
            let t_probe = Instant::now();
            let mut probe_input_rows: u64 = 0;
            let mut output_rows: u64 = 0;
            let mut output_batches: u64 = 0;
            while let Some(batch_res) = left_stream.next().await {
                let left_batch = batch_res.map_err(|e| {
                    ExecutionError::InvalidOperation(format!(
                        "grace single multi-batch streaming probe: left stream error: {e}"
                    ))
                })?;
                if left_batch.num_rows() == 0 {
                    continue;
                }
                probe_input_rows += left_batch.num_rows() as u64;
                if let Some(out) = probe_one_left_batch_multi_inner(
                    &left_batch,
                    &build,
                    &left_keys,
                    &output_schema_for_stream,
                )? {
                    output_rows += out.num_rows() as u64;
                    output_batches += 1;
                    yield out;
                }
            }
            tracing::info!(
                target: "arneb::profile",
                op = "HashJoinExec.execute_single_finish_streaming_multi",
                build_batches = build.batches.len() as u64,
                build_rows = build.total_rows() as u64,
                probe_ms = t_probe.elapsed().as_millis() as u64,
                probe_input_rows,
                output_rows,
                output_batches,
                "HashJoinExec concat-free multi-batch streaming probe done"
            );
        };

        Ok(Box::pin(AsyncBatchStream {
            schema: output_schema,
            inner: Box::pin(inner),
        }))
    }

    fn probe(
        &self,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
        hash_map: &JoinHashMap,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let left_rows = left_batch.num_rows();
        let right_rows = right_batch.num_rows();

        let output_schema = self.build_output_schema(left_batch, right_batch);

        // Phase 1 — collect every equi-key match as a candidate. Matching is
        // NOT recorded on `left_matched`/`right_matched` yet: a residual
        // predicate may reject some candidates, and an outer join must still
        // report those left/right rows as unmatched so the correct NULL-padded
        // output is produced.
        let mut cand_left: Vec<u32> = Vec::with_capacity(left_rows);
        let mut cand_right: Vec<u32> = Vec::with_capacity(left_rows);

        // Hoist data-type matches OUT of the row loop: pre-downcast
        // each key column once, then the per-row inner loop only does
        // `typed.value(i)` (cheap slice access) for hash + equality.
        let left_keys_typed = TypedKeys::new(left_batch, &self.left_keys)?;
        let right_keys_typed = TypedKeys::new(right_batch, &self.right_keys)?;
        let left_hashes = vectorized_probe_enabled().then(|| left_keys_typed.hash_batch(left_rows));

        for l_row in 0..left_rows {
            if left_keys_typed.row_has_null(l_row) {
                continue;
            }
            let hash = left_hashes
                .as_ref()
                .map_or_else(|| left_keys_typed.hash_row(l_row), |hashes| hashes[l_row]);
            // Walk the build slot chain; `row_eq` filters slot collisions
            // (distinct keys sharing a slot) and confirms true matches. The
            // chain is one entry for unique build keys (the TPC-H case).
            let mut r = hash_map.chain_head(hash);
            while r != JoinHashMap::EMPTY {
                if left_keys_typed.row_eq(l_row, &right_keys_typed, r as usize) {
                    cand_left.push(l_row as u32);
                    cand_right.push(r);
                }
                r = hash_map.chain_next(r);
            }
        }

        // Phase 2 — apply the residual predicate (if any) in one batched pass.
        let (left_indices, right_indices) = if let Some(residual) = &self.residual {
            self.filter_candidates(
                left_batch,
                right_batch,
                &output_schema,
                cand_left,
                cand_right,
                residual,
            )?
        } else {
            (cand_left, cand_right)
        };

        let mut left_matched = vec![false; left_rows];
        let mut right_matched = vec![false; right_rows];
        for &l in &left_indices {
            left_matched[l as usize] = true;
        }
        for &r in &right_indices {
            right_matched[r as usize] = true;
        }

        let mut all_batches = Vec::new();

        // Matched rows.
        if !left_indices.is_empty() {
            let use_dict_probe_build =
                dict_probe_build_enabled() && matches!(self.join_type, ast::JoinType::Inner);
            let left_idx = UInt32Array::from(left_indices);
            let right_idx = UInt32Array::from(right_indices);

            let mut columns = Vec::new();
            for col_i in 0..left_batch.num_columns() {
                columns.push(compute::take(left_batch.column(col_i), &left_idx, None)?);
            }
            for col_i in 0..right_batch.num_columns() {
                if use_dict_probe_build {
                    let dict = DictionaryArray::<UInt32Type>::try_new(
                        right_idx.clone(),
                        right_batch.column(col_i).clone(),
                    )?;
                    columns.push(Arc::new(dict) as ArrayRef);
                } else {
                    columns.push(compute::take(right_batch.column(col_i), &right_idx, None)?);
                }
            }
            let batch_schema = if use_dict_probe_build {
                self.build_dict_probe_output_schema(left_batch, right_batch)
            } else {
                output_schema.clone()
            };
            all_batches.push(RecordBatch::try_new(batch_schema, columns)?);
        }

        // LEFT/FULL: unmatched left rows with NULL right columns.
        // TODO(dict-probe-build): extend dictionary build-side emission to LEFT/FULL
        // after null-padded build columns have a dictionary representation.
        if matches!(self.join_type, ast::JoinType::Left | ast::JoinType::Full) {
            let unmatched: Vec<u32> = left_matched
                .iter()
                .enumerate()
                .filter(|(_, m)| !**m)
                .map(|(i, _)| i as u32)
                .collect();
            if !unmatched.is_empty() {
                let idx = UInt32Array::from(unmatched);
                let mut cols: Vec<ArrayRef> = Vec::new();
                for col_i in 0..left_batch.num_columns() {
                    cols.push(compute::take(left_batch.column(col_i), &idx, None)?);
                }
                let null_len = idx.len();
                for col_i in 0..right_batch.num_columns() {
                    cols.push(arrow::array::new_null_array(
                        right_batch.column(col_i).data_type(),
                        null_len,
                    ));
                }
                all_batches.push(RecordBatch::try_new(output_schema.clone(), cols)?);
            }
        }

        // RIGHT/FULL: unmatched right rows with NULL left columns.
        // TODO(dict-probe-build): RIGHT/FULL still materialize build-side columns
        // with take; keep outer-join behavior unchanged while the gate is experimental.
        if matches!(self.join_type, ast::JoinType::Right | ast::JoinType::Full) {
            let unmatched: Vec<u32> = right_matched
                .iter()
                .enumerate()
                .filter(|(_, m)| !**m)
                .map(|(i, _)| i as u32)
                .collect();
            if !unmatched.is_empty() {
                let idx = UInt32Array::from(unmatched);
                let null_len = idx.len();
                let mut cols: Vec<ArrayRef> = Vec::new();
                for col_i in 0..left_batch.num_columns() {
                    cols.push(arrow::array::new_null_array(
                        left_batch.column(col_i).data_type(),
                        null_len,
                    ));
                }
                for col_i in 0..right_batch.num_columns() {
                    cols.push(compute::take(right_batch.column(col_i), &idx, None)?);
                }
                all_batches.push(RecordBatch::try_new(output_schema.clone(), cols)?);
            }
        }

        Ok(all_batches)
    }

    /// Materialize equi-match candidates into a joined batch, evaluate the
    /// residual predicate on it, and return only the candidates that pass.
    /// Column indices in the residual reference the joined layout (left
    /// columns first, then right), so `expression::evaluate` can be run
    /// directly against the concatenated batch.
    fn filter_candidates(
        &self,
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
        output_schema: &Arc<Schema>,
        cand_left: Vec<u32>,
        cand_right: Vec<u32>,
        residual: &PlanExpr,
    ) -> Result<(Vec<u32>, Vec<u32>), ExecutionError> {
        if cand_left.is_empty() {
            return Ok((cand_left, cand_right));
        }

        let left_idx = UInt32Array::from(cand_left.clone());
        let right_idx = UInt32Array::from(cand_right.clone());
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());
        for col_i in 0..left_batch.num_columns() {
            cols.push(compute::take(left_batch.column(col_i), &left_idx, None)?);
        }
        for col_i in 0..right_batch.num_columns() {
            cols.push(compute::take(right_batch.column(col_i), &right_idx, None)?);
        }
        let joined = RecordBatch::try_new(output_schema.clone(), cols)?;
        let mask_arr = crate::expression::evaluate(residual, &joined, None)?;
        let mask = mask_arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "hash join residual predicate must evaluate to boolean".into(),
                )
            })?;

        let mut kept_left = Vec::with_capacity(cand_left.len());
        let mut kept_right = Vec::with_capacity(cand_right.len());
        for i in 0..mask.len() {
            if !mask.is_null(i) && mask.value(i) {
                kept_left.push(cand_left[i]);
                kept_right.push(cand_right[i]);
            }
        }
        Ok((kept_left, kept_right))
    }

    fn build_output_schema(&self, left: &RecordBatch, right: &RecordBatch) -> Arc<Schema> {
        let mut fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .map(|f| {
                if matches!(self.join_type, ast::JoinType::Right | ast::JoinType::Full) {
                    Field::new(f.name(), f.data_type().clone(), true)
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        fields.extend(right.schema().fields().iter().map(|f| {
            if matches!(self.join_type, ast::JoinType::Left | ast::JoinType::Full) {
                Field::new(f.name(), f.data_type().clone(), true)
            } else {
                f.as_ref().clone()
            }
        }));
        Arc::new(Schema::new(fields))
    }

    fn build_dict_probe_output_schema(
        &self,
        left: &RecordBatch,
        right: &RecordBatch,
    ) -> Arc<Schema> {
        let mut fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.extend(right.schema().fields().iter().map(|f| {
            Field::new(
                f.name(),
                ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::UInt32),
                    Box::new(f.data_type().clone()),
                ),
                f.is_nullable(),
            )
        }));
        Arc::new(Schema::new(fields))
    }

    fn handle_empty_right(
        &self,
        left: Option<&RecordBatch>,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let schema = column_info_to_arrow_schema(&self.schema());
        match self.join_type {
            ast::JoinType::Left | ast::JoinType::Full => {
                if let Some(left_batch) = left {
                    if left_batch.num_rows() > 0 {
                        let right_schema = self.right.schema();
                        let mut cols: Vec<ArrayRef> = Vec::new();
                        for i in 0..left_batch.num_columns() {
                            cols.push(left_batch.column(i).clone());
                        }
                        for info in &right_schema {
                            let dt: ArrowDataType = info.data_type.clone().into();
                            cols.push(arrow::array::new_null_array(&dt, left_batch.num_rows()));
                        }
                        let output_schema = self.build_output_schema(
                            left_batch,
                            &RecordBatch::new_empty(column_info_to_arrow_schema(&right_schema)),
                        );
                        let batch = RecordBatch::try_new(output_schema.clone(), cols)?;
                        return Ok(stream_from_batches(output_schema, vec![batch]));
                    }
                }
                Ok(stream_from_batches(schema, vec![]))
            }
            _ => Ok(stream_from_batches(schema, vec![])),
        }
    }

    fn handle_empty_left(
        &self,
        right_batch: &RecordBatch,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let schema = column_info_to_arrow_schema(&self.schema());
        match self.join_type {
            ast::JoinType::Right | ast::JoinType::Full => {
                if right_batch.num_rows() > 0 {
                    let left_schema = self.left.schema();
                    let mut cols: Vec<ArrayRef> = Vec::new();
                    for info in &left_schema {
                        let dt: ArrowDataType = info.data_type.clone().into();
                        cols.push(arrow::array::new_null_array(&dt, right_batch.num_rows()));
                    }
                    for i in 0..right_batch.num_columns() {
                        cols.push(right_batch.column(i).clone());
                    }
                    let left_empty =
                        RecordBatch::new_empty(column_info_to_arrow_schema(&left_schema));
                    let output_schema = self.build_output_schema(&left_empty, right_batch);
                    let batch = RecordBatch::try_new(output_schema.clone(), cols)?;
                    return Ok(stream_from_batches(output_schema, vec![batch]));
                }
                Ok(stream_from_batches(schema, vec![]))
            }
            _ => Ok(stream_from_batches(schema, vec![])),
        }
    }
}

// ===========================================================================
// Multipass streaming probe helpers (Phase 3b.4, 2026-05-21)
// ===========================================================================

/// Max rows per output batch yielded from the streaming multipass
/// probe. Mirrors Trino's `PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES`
/// goal — small enough that one batch fits comfortably in the
/// downstream consumer's working set, large enough that per-batch
/// scheduling overhead stays amortised.
const MULTIPASS_OUTPUT_BATCH_ROWS: usize = 1024;

/// Row-count target for `execute_single_finish`'s per-batch streaming
/// output. Mirrors `MULTIPASS_OUTPUT_BATCH_ROWS` — the streaming
/// refactor (Phase A, 2026-05-23) accumulates output across multiple
/// input left batches until reaching this threshold, then concats +
/// yields one larger batch. Bounds per-batch downstream-consumer
/// overhead while preserving the streaming back-pressure that
/// eliminates the `concat_batches(left)` OOM on multi-million-row
/// probes (Q09).
const STREAMING_OUTPUT_TARGET_ROWS: usize = 1024;

/// Number of hash partitions Grace Hash Join (Phase 3b.5) carves the
/// build/probe sides into. Power-of-2 to enable future AND-mask
/// optimisation; 16 matches Trino's default. Each partition is
/// ~build_total / 16, so a 1.2 GB build → ~75 MB per partition, which
/// fits comfortably alongside other concurrent operators.
const GRACE_PARTITION_COUNT: usize = 16;

/// Cache-fit Hash Join gate (2026-05-30). When the build fits in memory
/// but is large enough that one flattened `JoinHashMap` overflows CPU
/// cache, keep it hash-partitioned in memory and probe per partition
/// (each partition cache-resident) instead of flattening to the Single
/// fast path. Opt-in via `ARNEB_CACHE_FIT_HJ` for the A/B; flip the
/// default once verified on the 22q-warm dual-axis. Only consulted from
/// the Grace partitioned-build path (build already split into
/// `grace_partition_count()` buckets), so it is naturally scoped to
/// `grace_enabled()` joins.
fn cache_fit_enabled() -> bool {
    std::env::var_os("ARNEB_CACHE_FIT_HJ").is_some()
}

/// Vectorized probe-side hash precompute. Default off; enabled with
/// `ARNEB_VECTORIZED_PROBE=1`.
#[cfg(not(test))]
fn vectorized_probe_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_VECTORIZED_PROBE").is_ok_and(|v| v == "1");
        tracing::info!(
            target: "arneb::config",
            knob = "ARNEB_VECTORIZED_PROBE",
            enabled,
            "runtime config"
        );
        enabled
    })
}

#[cfg(test)]
fn vectorized_probe_enabled() -> bool {
    std::env::var("ARNEB_VECTORIZED_PROBE").is_ok_and(|v| v == "1")
}

/// Restructure the multi-batch INNER probe as candidate gather + deferred Arrow
/// key-compare. Default off; enabled with `ARNEB_VEC_PROBE=1`.
#[cfg(not(test))]
fn vec_probe_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_VEC_PROBE").is_ok_and(|v| v == "1");
        tracing::info!(
            target: "arneb::config",
            knob = "ARNEB_VEC_PROBE",
            enabled,
            "runtime config"
        );
        enabled
    })
}

#[cfg(test)]
fn vec_probe_enabled() -> bool {
    std::env::var("ARNEB_VEC_PROBE").is_ok_and(|v| v == "1")
}

/// Software-prefetch the build hash-slot for a look-ahead probe row to hide the
/// random-access cache-miss latency of `head[hash & mask]` (flamegraph: the
/// multi-batch probe's hash lookups are ~15% of q07 CPU, cache-miss-bound on the
/// large build). Default off; enabled with `ARNEB_PROBE_PREFETCH=1`. Cell-safe:
/// a prefetch is a CPU hint with zero effect on results.
#[cfg(not(test))]
fn probe_prefetch_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_PROBE_PREFETCH").is_ok_and(|v| v == "1");
        tracing::info!(
            target: "arneb::config",
            knob = "ARNEB_PROBE_PREFETCH",
            enabled,
            "runtime config"
        );
        enabled
    })
}

#[cfg(test)]
fn probe_prefetch_enabled() -> bool {
    std::env::var("ARNEB_PROBE_PREFETCH").is_ok_and(|v| v == "1")
}

/// Stream the shared Single-build probe side batch-by-batch. Default off;
/// enabled with `ARNEB_STREAM_HASH_PROBE=1`.
#[cfg(not(test))]
fn stream_hash_probe_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_STREAM_HASH_PROBE").is_ok_and(|v| v == "1");
        tracing::info!(
            target: "arneb::config",
            knob = "ARNEB_STREAM_HASH_PROBE",
            enabled,
            "runtime config"
        );
        enabled
    })
}

#[cfg(test)]
fn stream_hash_probe_enabled() -> bool {
    std::env::var("ARNEB_STREAM_HASH_PROBE").is_ok_and(|v| v == "1")
}

/// Broadcast/single-build cross-fragment dynamic-filter publishing gate.
/// Default off; enabled with `ARNEB_BROADCAST_DF`.
#[cfg(not(test))]
fn broadcast_df_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var_os("ARNEB_BROADCAST_DF").is_some();
        tracing::info!(
            target: "arneb::config",
            knob = "ARNEB_BROADCAST_DF",
            enabled,
            "runtime config"
        );
        enabled
    })
}

#[cfg(test)]
fn broadcast_df_enabled() -> bool {
    std::env::var_os("ARNEB_BROADCAST_DF").is_some()
}

/// Total build bytes above which [`cache_fit_enabled`] keeps the build
/// partitioned instead of flattening. With `grace_partition_count()`
/// partitions, per-partition size ≈ threshold / N — pick a threshold
/// whose per-partition slice stays L2/L3-resident. Default 16 MiB;
/// override via `ARNEB_CACHE_FIT_THRESHOLD_MB`.
fn cache_fit_threshold_bytes() -> usize {
    std::env::var("ARNEB_CACHE_FIT_THRESHOLD_MB")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(16)
        .saturating_mul(1024 * 1024)
}

/// Number of hash partitions Grace / cache-fit Hash Join carves the
/// build + probe sides into. Defaults to [`GRACE_PARTITION_COUNT`];
/// override via `ARNEB_GRACE_PARTITIONS` to tune per-partition cache
/// residency without a recompile (e.g. 32 for a smaller per-partition
/// table). Clamped to `>= 1`.
fn grace_partition_count() -> usize {
    std::env::var("ARNEB_GRACE_PARTITIONS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(GRACE_PARTITION_COUNT)
}

/// Intra-worker probe parallelism for the cache-fit (no-spill) Grace
/// path. `1` = sequential (unchanged). `>1` = split the in-memory probe
/// input into N row-chunks and probe them concurrently on the blocking
/// thread pool against the shared read-only build (Q09 SF10 profiling
/// showed the probe runs ~single-threaded while the 14-core VM idles).
/// Default: the host's available parallelism (cgroup-aware via
/// `std::thread::available_parallelism`, so a container CPU limit auto-sizes
/// it). Override via `ARNEB_PROBE_THREADS`. Only engaged when the build is
/// fully in memory (cache-fit), so there is no spill / streaming coordination
/// — the chunks share an `Arc` build and return owned batches, which is
/// deadlock-free by construction.
fn probe_threads() -> usize {
    std::env::var("ARNEB_PROBE_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
}

/// Dedicated cap (bytes) on the no-spill probe MATERIALISATION, independent
/// of the pool budget. `Some(m)` makes a probe larger than `m` stream via
/// the bounded `execute_grace_inner` path instead of collecting in full,
/// bounding the join's peak without shrinking the build's spill headroom.
/// `None` (default, unset) keeps the prior pool-only behaviour. Runtime
/// knob `ARNEB_PROBE_COLLECT_MAX_BYTES`; q08's no-spill join otherwise
/// collected its whole ~3.4 GB lineitem-derived probe under the 5 GB pool
/// (heap-profile attribution 2026-06-09).
fn probe_collect_max_bytes() -> Option<usize> {
    std::env::var("ARNEB_PROBE_COLLECT_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
}

/// Minimum probe "work" (`probe_rows × build_rows`) to engage the
/// parallel probe. A single row-count threshold can't tell an expensive
/// join from a cheap one: TPC-H Q09's HJ#3 (30M probe × 54K build) is
/// worth parallelising, but Q08's join (1.6M probe × ~50K build — same
/// build size, 20× less work) is not and regressed ~50% under row-only
/// gates. The product cleanly separates them, and also excludes the
/// inverse (tiny-probe / huge-build) cheap joins. Default 5e11; override
/// via `ARNEB_PROBE_MIN_WORK` (0 disables the gate).
fn probe_min_work() -> u64 {
    std::env::var("ARNEB_PROBE_MIN_WORK")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(500_000_000_000)
}

/// Returns `true` when the parallel probe should run: more than one
/// thread requested AND the join is expensive enough
/// (`probe_rows × build_rows`) to amortise the chunk/spawn/materialise
/// overhead. Saturating multiply guards the (unrealistic) overflow case.
fn use_parallel_probe(probe_rows: u64, build_rows: u64) -> bool {
    probe_threads() > 1 && probe_rows.saturating_mul(build_rows) >= probe_min_work()
}

/// Chunk-count multiplier over `threads` for the parallel probe. More,
/// smaller chunks bound peak output memory: `buffer_unordered(threads)`
/// keeps only ~`threads` chunk-outputs in flight (drained continuously
/// by the downstream consumer) rather than materialising the whole join
/// output at once.
const PROBE_CHUNK_FACTOR: usize = 4;

/// Walk `left_batch` row-by-row and collect every equi-match candidate
/// against `right_batch` via `hash_map`. INNER-only — multipass is
/// gated to INNER without residual upstream, so no residual filter +
/// no outer-side unmatched bookkeeping is needed here.
fn multipass_collect_candidates(
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    hash_map: &JoinHashMap,
    left_keys: &[usize],
    right_keys: &[usize],
) -> Result<(Vec<u32>, Vec<u32>), ExecutionError> {
    let left_rows = left_batch.num_rows();
    let mut cand_left: Vec<u32> = Vec::new();
    let mut cand_right: Vec<u32> = Vec::new();
    if left_rows == 0 || right_batch.num_rows() == 0 {
        return Ok((cand_left, cand_right));
    }
    let left_typed = TypedKeys::new(left_batch, left_keys)?;
    let right_typed = TypedKeys::new(right_batch, right_keys)?;
    let left_hashes = vectorized_probe_enabled().then(|| left_typed.hash_batch(left_rows));
    for l_row in 0..left_rows {
        if left_typed.row_has_null(l_row) {
            continue;
        }
        let hash = left_hashes
            .as_ref()
            .map_or_else(|| left_typed.hash_row(l_row), |hashes| hashes[l_row]);
        let mut r = hash_map.chain_head(hash);
        while r != JoinHashMap::EMPTY {
            if left_typed.row_eq(l_row, &right_typed, r as usize) {
                cand_left.push(l_row as u32);
                cand_right.push(r);
            }
            r = hash_map.chain_next(r);
        }
    }
    Ok((cand_left, cand_right))
}

/// Phase Z.1 + Z.1b (2026-05-22): indices-only Grace HJ Pass 1 probe.
/// Walks only the rows of `left_typed`'s backing batch listed in
/// `left_subset` (those routed to this build partition by the hash
/// partitioner) against `hash_map`. Returns matched indices into the
/// ORIGINAL left batch (not into the subset) so `build_joined_slice`
/// can `compute::take` against the unmodified batch — no intermediate
/// sub-batch materialization, no 16×-per-batch `compute::take` at
/// routing time.
///
/// `left_typed` is taken by reference so callers that probe the same
/// left batch against many build partitions (Grace HJ Pass 1 has up to
/// 16) construct `TypedKeys::new(left_batch, left_keys)` once and
/// reuse — avoids 16× redundant `TypedKeys` allocation per batch.
fn multipass_collect_candidates_subset_with_left(
    left_typed: &TypedKeys,
    left_hashes: Option<&[u64]>,
    left_subset: &[u32],
    right_batch: &RecordBatch,
    hash_map: &JoinHashMap,
    right_keys: &[usize],
) -> Result<(Vec<u32>, Vec<u32>), ExecutionError> {
    let mut cand_left: Vec<u32> = Vec::new();
    let mut cand_right: Vec<u32> = Vec::new();
    if left_subset.is_empty() || right_batch.num_rows() == 0 {
        return Ok((cand_left, cand_right));
    }
    let right_typed = TypedKeys::new(right_batch, right_keys)?;
    for &l_row in left_subset {
        let l_row_usize = l_row as usize;
        if left_typed.row_has_null(l_row_usize) {
            continue;
        }
        let hash = left_hashes
            .map(|hashes| hashes[l_row_usize])
            .unwrap_or_else(|| left_typed.hash_row(l_row_usize));
        let mut r = hash_map.chain_head(hash);
        while r != JoinHashMap::EMPTY {
            if left_typed.row_eq(l_row_usize, &right_typed, r as usize) {
                cand_left.push(l_row);
                cand_right.push(r);
            }
            r = hash_map.chain_next(r);
        }
    }
    Ok((cand_left, cand_right))
}

/// Materialise one output RecordBatch from the given index slices.
/// The streaming multipass loop slices the full candidate arrays into
/// MULTIPASS_OUTPUT_BATCH_ROWS-sized windows and calls this per slice.
fn build_joined_slice(
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    cand_left: &[u32],
    cand_right: &[u32],
    output_schema: &Arc<Schema>,
) -> Result<RecordBatch, ExecutionError> {
    let left_idx = UInt32Array::from(cand_left.to_vec());
    let right_idx = UInt32Array::from(cand_right.to_vec());
    let mut columns: Vec<ArrayRef> =
        Vec::with_capacity(left_batch.num_columns() + right_batch.num_columns());
    for col_i in 0..left_batch.num_columns() {
        columns.push(compute::take(left_batch.column(col_i), &left_idx, None)?);
    }
    for col_i in 0..right_batch.num_columns() {
        columns.push(compute::take(right_batch.column(col_i), &right_idx, None)?);
    }
    Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

/// Round-robin `batches` into `n` groups of roughly equal row count
/// (batches are ~uniform size, so by-batch round-robin balances rows).
/// Empty groups are dropped. Used to chunk the probe input for the
/// parallel cache-fit probe.
fn split_batches_even(batches: Vec<RecordBatch>, n: usize) -> Vec<Vec<RecordBatch>> {
    let n = n.max(1);
    let mut groups: Vec<Vec<RecordBatch>> = (0..n).map(|_| Vec::new()).collect();
    for (i, b) in batches.into_iter().enumerate() {
        groups[i % n].push(b);
    }
    groups.into_iter().filter(|g| !g.is_empty()).collect()
}

/// Probe one left batch against the in-memory partitioned build
/// (`in_mem_hash_maps`), appending output batches to `out`. The no-spill
/// equivalent of `execute_grace_inner`'s Pass 1 inner body, factored out
/// so it can run inside a `spawn_blocking` chunk task (takes only `&`
/// args over the shared `Arc` build — no `self`, no spill writer).
#[allow(clippy::too_many_arguments)]
fn probe_partition_batch_inmem(
    left_batch: &RecordBatch,
    in_mem_hash_maps: &[Option<(RecordBatch, JoinHashMap)>],
    probe_partitioner: &HashPartitioner,
    left_keys: &[usize],
    right_keys: &[usize],
    output_schema: &Arc<Schema>,
    n_partitions: usize,
    out: &mut Vec<RecordBatch>,
) -> Result<(), ExecutionError> {
    if left_batch.num_rows() == 0 {
        return Ok(());
    }
    let assignments = probe_partitioner.assignments(left_batch)?;
    let mut buckets: Vec<Vec<u32>> = (0..n_partitions).map(|_| Vec::new()).collect();
    for (row, &p) in assignments.iter().enumerate() {
        buckets[p as usize].push(row as u32);
    }
    let left_typed = TypedKeys::new(left_batch, left_keys)?;
    let left_hashes =
        vectorized_probe_enabled().then(|| left_typed.hash_batch(left_batch.num_rows()));
    for (p, indices) in buckets.into_iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        // No-spill cache-fit: every populated partition is in memory; an
        // empty partition (`None`) means INNER produced no match → drop.
        if let Some((right_batch, hash_map)) = in_mem_hash_maps[p].as_ref() {
            let (cand_left, cand_right) = multipass_collect_candidates_subset_with_left(
                &left_typed,
                left_hashes.as_deref(),
                &indices,
                right_batch,
                hash_map,
                right_keys,
            )?;
            if cand_left.is_empty() {
                continue;
            }
            for slice_start in (0..cand_left.len()).step_by(MULTIPASS_OUTPUT_BATCH_ROWS) {
                let slice_end = (slice_start + MULTIPASS_OUTPUT_BATCH_ROWS).min(cand_left.len());
                out.push(build_joined_slice(
                    left_batch,
                    right_batch,
                    &cand_left[slice_start..slice_end],
                    &cand_right[slice_start..slice_end],
                    output_schema,
                )?);
            }
        }
    }
    Ok(())
}

// ===========================================================================
// Per-left-batch streaming probe helpers (Phase A streaming refactor,
// 2026-05-23). Mirror the body of `HashJoinExec::probe`, but split into
// pieces that can be driven by `async_stream::try_stream!` inside
// `execute_single_finish`. The key difference from `probe` is that
// `right_matched` lives OUTSIDE per-batch state — the caller threads it
// across every left batch so the RIGHT/FULL unmatched-right tail can
// fire once at the end.
// ===========================================================================

/// Probe one left batch against the (already-built) hash map and the
/// concatenated right batch. Updates `right_matched` in place. Returns
/// the matched output batch + the LEFT/FULL unmatched-left batch (if
/// any) for THIS left input only.
#[allow(clippy::too_many_arguments)]
fn probe_one_left_batch(
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    hash_map: &JoinHashMap,
    left_keys: &[usize],
    right_keys: &[usize],
    residual: Option<&PlanExpr>,
    join_type: ast::JoinType,
    output_schema: &Arc<Schema>,
    right_matched: &mut [bool],
) -> Result<Vec<RecordBatch>, ExecutionError> {
    let left_rows = left_batch.num_rows();
    if left_rows == 0 {
        return Ok(Vec::new());
    }

    // Phase 1 — equi-match candidate collection. Same loop body as the
    // monolithic `probe()`; lifted here to operate over the per-batch
    // left/right pair instead of the concatenated mega-batches.
    let mut cand_left: Vec<u32> = Vec::with_capacity(left_rows);
    let mut cand_right: Vec<u32> = Vec::with_capacity(left_rows);

    let left_keys_typed = TypedKeys::new(left_batch, left_keys)?;
    let right_keys_typed = TypedKeys::new(right_batch, right_keys)?;
    let left_hashes = vectorized_probe_enabled().then(|| left_keys_typed.hash_batch(left_rows));

    for l_row in 0..left_rows {
        if left_keys_typed.row_has_null(l_row) {
            continue;
        }
        let hash = left_hashes
            .as_ref()
            .map_or_else(|| left_keys_typed.hash_row(l_row), |hashes| hashes[l_row]);
        let mut r = hash_map.chain_head(hash);
        while r != JoinHashMap::EMPTY {
            if left_keys_typed.row_eq(l_row, &right_keys_typed, r as usize) {
                cand_left.push(l_row as u32);
                cand_right.push(r);
            }
            r = hash_map.chain_next(r);
        }
    }

    // Phase 2 — residual filter (one batched pass over candidates).
    let (left_indices, right_indices) = if let Some(residual) = residual {
        filter_candidates_free(
            left_batch,
            right_batch,
            output_schema,
            cand_left,
            cand_right,
            residual,
        )?
    } else {
        (cand_left, cand_right)
    };

    // Local left_matched (this batch only). Right_matched is the
    // caller's accumulator across all left batches.
    let mut left_matched = vec![false; left_rows];
    for &l in &left_indices {
        left_matched[l as usize] = true;
    }
    for &r in &right_indices {
        right_matched[r as usize] = true;
    }

    let mut out: Vec<RecordBatch> = Vec::new();

    // Matched rows.
    if !left_indices.is_empty() {
        let left_idx = UInt32Array::from(left_indices);
        let right_idx = UInt32Array::from(right_indices);
        let mut columns: Vec<ArrayRef> = Vec::new();
        for col_i in 0..left_batch.num_columns() {
            columns.push(compute::take(left_batch.column(col_i), &left_idx, None)?);
        }
        for col_i in 0..right_batch.num_columns() {
            columns.push(compute::take(right_batch.column(col_i), &right_idx, None)?);
        }
        out.push(RecordBatch::try_new(output_schema.clone(), columns)?);
    }

    // LEFT/FULL: unmatched left rows with NULL right columns. This is
    // safe to emit per-batch because each left row appears in exactly
    // one input batch — no cross-batch state needed.
    if matches!(join_type, ast::JoinType::Left | ast::JoinType::Full) {
        let unmatched: Vec<u32> = left_matched
            .iter()
            .enumerate()
            .filter(|(_, m)| !**m)
            .map(|(i, _)| i as u32)
            .collect();
        if !unmatched.is_empty() {
            let idx = UInt32Array::from(unmatched);
            let null_len = idx.len();
            let mut cols: Vec<ArrayRef> = Vec::new();
            for col_i in 0..left_batch.num_columns() {
                cols.push(compute::take(left_batch.column(col_i), &idx, None)?);
            }
            for col_i in 0..right_batch.num_columns() {
                cols.push(arrow::array::new_null_array(
                    right_batch.column(col_i).data_type(),
                    null_len,
                ));
            }
            out.push(RecordBatch::try_new(output_schema.clone(), cols)?);
        }
    }

    Ok(out)
}

/// Free-function twin of `HashJoinExec::filter_candidates`. Same body;
/// invoked from `probe_one_left_batch` since async_stream closures
/// can't capture `&self` across the stream boundary.
fn filter_candidates_free(
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    output_schema: &Arc<Schema>,
    cand_left: Vec<u32>,
    cand_right: Vec<u32>,
    residual: &PlanExpr,
) -> Result<(Vec<u32>, Vec<u32>), ExecutionError> {
    if cand_left.is_empty() {
        return Ok((cand_left, cand_right));
    }

    let left_idx = UInt32Array::from(cand_left.clone());
    let right_idx = UInt32Array::from(cand_right.clone());
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());
    for col_i in 0..left_batch.num_columns() {
        cols.push(compute::take(left_batch.column(col_i), &left_idx, None)?);
    }
    for col_i in 0..right_batch.num_columns() {
        cols.push(compute::take(right_batch.column(col_i), &right_idx, None)?);
    }
    let joined = RecordBatch::try_new(output_schema.clone(), cols)?;
    let mask_arr = crate::expression::evaluate(residual, &joined, None)?;
    let mask = mask_arr
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            ExecutionError::InvalidOperation(
                "hash join residual predicate must evaluate to boolean".into(),
            )
        })?;

    let mut kept_left = Vec::with_capacity(cand_left.len());
    let mut kept_right = Vec::with_capacity(cand_right.len());
    for i in 0..mask.len() {
        if !mask.is_null(i) && mask.value(i) {
            kept_left.push(cand_left[i]);
            kept_right.push(cand_right[i]);
        }
    }
    Ok((kept_left, kept_right))
}

/// Build the RIGHT/FULL unmatched-right tail batch. Emitted once at
/// the end of the streaming probe, after every left batch has had a
/// chance to mark its matches into `right_matched`. Returns `None` if
/// nothing is unmatched (in which case the caller skips the yield).
fn build_unmatched_right_tail(
    right_batch: &RecordBatch,
    right_matched: &[bool],
    output_schema: &Arc<Schema>,
    left_schema_template: &[ArrowDataType],
) -> Result<Option<RecordBatch>, ExecutionError> {
    let unmatched: Vec<u32> = right_matched
        .iter()
        .enumerate()
        .filter(|(_, m)| !**m)
        .map(|(i, _)| i as u32)
        .collect();
    if unmatched.is_empty() {
        return Ok(None);
    }
    let idx = UInt32Array::from(unmatched);
    let null_len = idx.len();
    let mut cols: Vec<ArrayRef> = Vec::new();
    for dt in left_schema_template {
        cols.push(arrow::array::new_null_array(dt, null_len));
    }
    for col_i in 0..right_batch.num_columns() {
        cols.push(compute::take(right_batch.column(col_i), &idx, None)?);
    }
    Ok(Some(RecordBatch::try_new(output_schema.clone(), cols)?))
}

/// `RecordBatchStream` adapter over a `Pin<Box<dyn Stream + Send>>`.
/// Used to wrap `async_stream::try_stream!` output so it satisfies the
/// `SendableRecordBatchStream` shape that the rest of arneb's
/// execution pipeline expects.
struct AsyncBatchStream {
    schema: Arc<Schema>,
    inner:
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<RecordBatch, ExecutionError>> + Send>>,
}

impl futures::Stream for AsyncBatchStream {
    type Item = Result<RecordBatch, arneb_common::error::ArnebError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner
            .as_mut()
            .poll_next(cx)
            .map(|opt| opt.map(|res| res.map_err(arneb_common::error::ArnebError::Execution)))
    }
}

impl arneb_common::stream::RecordBatchStream for AsyncBatchStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

/// exec-memory-accounting D3: wraps a probe-output stream and holds the
/// build + probe `MemoryReservation`s alive for the stream's whole lifetime.
/// `execute_single` previously dropped the build reservation (and never
/// tracked the probe collect) BEFORE the probe ran, so the concatenated
/// ~5 GB build batch + collected probe were held untracked through the probe
/// — the q18 SF30 worker-OOM retainer. Holding the reservations here keeps
/// that working set accounted against the pool until the probe finishes
/// draining; they release on drop when the stream completes.
struct ReservationHoldingStream {
    inner: SendableRecordBatchStream,
    schema: Arc<Schema>,
    _reservations: Vec<MemoryReservation>,
}

impl futures::Stream for ReservationHoldingStream {
    type Item = Result<RecordBatch, arneb_common::error::ArnebError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl arneb_common::stream::RecordBatchStream for ReservationHoldingStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

/// Wrap `inner` so `reservations` stay alive until the stream is fully drained.
fn hold_reservations(
    inner: SendableRecordBatchStream,
    reservations: Vec<MemoryReservation>,
) -> SendableRecordBatchStream {
    let schema = inner.schema();
    Box::pin(ReservationHoldingStream {
        inner,
        schema,
        _reservations: reservations,
    })
}

// ===========================================================================
// Equi-join detection
// ===========================================================================

/// Analyzes a join condition to extract equi-join key pairs and any residual
/// (non-equi) predicate.
///
/// Returns `Some((keys, residual))` if at least one top-level conjunct is a
/// column-to-column equality across the two inputs. `keys` holds the equi
/// pairs; `residual` carries every other conjunct AND-ed together, rewritten
/// so that right-side column indices are offset by `left_col_count` — matching
/// the joined batch layout that `HashJoinExec` builds when it evaluates the
/// residual. Returns `None` when the condition is absent or contains no equi
/// conjunct (in which case the planner falls back to `NestedLoopJoinExec`).
///
/// The residual must be preserved so that `LEFT`/`RIGHT`/`FULL` outer joins
/// keep the correct non-matching rows: dropping it would degrade an outer
/// join into an inner-join-with-filter and produce wrong results (TPC-H Q13).
/// Paired equi-join key indices (left input column, right input column).
pub(crate) type EquiKeys = Vec<(usize, usize)>;

/// Equi-join keys plus any non-equi residual predicate carried alongside them.
pub(crate) type EquiJoinSplit = (EquiKeys, Option<PlanExpr>);

pub(crate) fn extract_equi_join_keys(
    condition: &arneb_planner::JoinCondition,
    left_col_count: usize,
) -> Option<EquiJoinSplit> {
    match condition {
        arneb_planner::JoinCondition::None => None,
        arneb_planner::JoinCondition::On(expr) => {
            let mut keys = Vec::new();
            let mut residual_parts: Vec<PlanExpr> = Vec::new();
            collect_equi_keys(expr, left_col_count, &mut keys, &mut residual_parts);
            if keys.is_empty() {
                return None;
            }
            let residual = residual_parts
                .into_iter()
                .reduce(|acc, e| PlanExpr::BinaryOp {
                    left: Box::new(acc),
                    op: ast::BinaryOp::And,
                    right: Box::new(e),
                    span: None,
                });
            Some((keys, residual))
        }
    }
}

/// Walks a conjunctive join condition, routing column-to-column equalities
/// that span the two inputs into `keys` and every other conjunct into
/// `residuals`. Called recursively through `AND` nodes so that a condition
/// like `a = b AND c > d AND e LIKE '%x%'` splits cleanly into one equi key
/// and two residual predicates.
fn collect_equi_keys(
    expr: &PlanExpr,
    left_col_count: usize,
    keys: &mut Vec<(usize, usize)>,
    residuals: &mut Vec<PlanExpr>,
) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: ast::BinaryOp::And,
            right,
            ..
        } => {
            collect_equi_keys(left, left_col_count, keys, residuals);
            collect_equi_keys(right, left_col_count, keys, residuals);
        }
        PlanExpr::BinaryOp {
            left,
            op: ast::BinaryOp::Eq,
            right,
            ..
        } => {
            if let (PlanExpr::Column { index: l_idx, .. }, PlanExpr::Column { index: r_idx, .. }) =
                (left.as_ref(), right.as_ref())
            {
                if *l_idx < left_col_count && *r_idx >= left_col_count {
                    keys.push((*l_idx, *r_idx - left_col_count));
                    return;
                } else if *r_idx < left_col_count && *l_idx >= left_col_count {
                    keys.push((*r_idx, *l_idx - left_col_count));
                    return;
                }
            }
            residuals.push(expr.clone());
        }
        _ => residuals.push(expr.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;
    use crate::datasource::{DataSource, InMemoryDataSource};
    use crate::operator::ScanExec;
    use crate::scan_context::ScanContext;
    use arneb_common::types::{DataType, ScalarValue};
    use arneb_common::{Domain, DynamicFilterId};
    use arrow::array::{
        Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array, StringArray,
    };

    static BROADCAST_DF_TEST_ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[derive(Debug)]
    struct CapturingDynamicFilterPublisher {
        calls: Arc<Mutex<Vec<(DynamicFilterId, Domain)>>>,
    }

    #[async_trait]
    impl crate::DynamicFilterPublisher for CapturingDynamicFilterPublisher {
        async fn publish(&self, df_id: DynamicFilterId, domain: Domain) {
            self.calls.lock().unwrap().push((df_id, domain));
        }
    }

    #[derive(Debug)]
    struct CapturingPartitionDynamicFilterPublisher {
        task_idx: u32,
        calls: Arc<Mutex<Vec<(DynamicFilterId, u32, Domain)>>>,
    }

    #[async_trait]
    impl crate::DynamicFilterPublisher for CapturingPartitionDynamicFilterPublisher {
        fn task_partition_idx(&self) -> u32 {
            self.task_idx
        }

        async fn publish(&self, df_id: DynamicFilterId, domain: Domain) {
            self.publish_partition(df_id, self.task_idx, domain).await;
        }

        async fn publish_partition(
            &self,
            df_id: DynamicFilterId,
            partition_idx: u32,
            domain: Domain,
        ) {
            self.calls
                .lock()
                .unwrap()
                .push((df_id, partition_idx, domain));
        }
    }

    fn left_source() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap();
        let ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".into(),
                    data_type: DataType::Utf8,
                    nullable: false,
                },
            ],
            vec![batch],
        );
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: "left".into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        })
    }

    fn right_source() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("value", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![2, 3, 5])),
                Arc::new(Int64Array::from(vec![200, 300, 500])),
            ],
        )
        .unwrap();
        let ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "value".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            vec![batch],
        );
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: "right".into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        })
    }

    fn key_value_source_i32(
        table_name: &str,
        key_name: &str,
        value_name: &str,
        keys: Vec<i32>,
        values: Vec<i32>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(key_name, ArrowDataType::Int32, false),
            Field::new(value_name, ArrowDataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(keys)) as ArrayRef,
                Arc::new(Int32Array::from(values)) as ArrayRef,
            ],
        )
        .unwrap();
        let ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: key_name.into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: value_name.into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
            ],
            vec![batch],
        );
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: table_name.into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        })
    }

    fn key_value_source_i64(
        table_name: &str,
        key_name: &str,
        value_name: &str,
        keys: Vec<i64>,
        values: Vec<i32>,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(key_name, ArrowDataType::Int64, false),
            Field::new(value_name, ArrowDataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)) as ArrayRef,
                Arc::new(Int32Array::from(values)) as ArrayRef,
            ],
        )
        .unwrap();
        let ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: key_name.into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: value_name.into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
            ],
            vec![batch],
        );
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: table_name.into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        })
    }

    #[derive(Debug, Clone)]
    struct PartitionedInMemoryDataSource {
        schema: Vec<ColumnInfo>,
        partitions: Vec<Vec<RecordBatch>>,
    }

    #[async_trait]
    impl DataSource for PartitionedInMemoryDataSource {
        fn schema(&self) -> Vec<ColumnInfo> {
            self.schema.clone()
        }

        fn partition_count(&self) -> usize {
            self.partitions.len()
        }

        async fn scan(
            &self,
            _ctx: &ScanContext,
            partition: usize,
        ) -> Result<SendableRecordBatchStream, ExecutionError> {
            let batches = self.partitions.get(partition).cloned().ok_or_else(|| {
                ExecutionError::InvalidOperation(format!(
                    "PartitionedInMemoryDataSource: partition {partition} out of range"
                ))
            })?;
            Ok(stream_from_batches(
                column_info_to_arrow_schema(&self.schema),
                batches,
            ))
        }
    }

    fn partitioned_left_source(partitions: Vec<Vec<Vec<i32>>>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let partitions = partitions
            .into_iter()
            .map(|batches| {
                batches
                    .into_iter()
                    .map(|values| {
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![Arc::new(Int32Array::from(values))],
                        )
                        .unwrap()
                    })
                    .collect()
            })
            .collect();
        let ds = PartitionedInMemoryDataSource {
            schema: vec![ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            partitions,
        };
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: "left_partitioned".into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        })
    }

    fn empty_right_source() -> Arc<dyn ExecutionPlan> {
        let ds = InMemoryDataSource::empty(vec![
            ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "value".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ]);
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: "right_empty".into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        })
    }

    #[test]
    fn test_dict_probe_build_matches_take() {
        let schema_left = Arc::new(Schema::new(vec![Field::new(
            "probe_key",
            ArrowDataType::Int64,
            false,
        )]));
        let left_batch = RecordBatch::try_new(
            schema_left,
            vec![Arc::new(Int64Array::from(vec![1, 1, 2, 2, 2, 3]))],
        )
        .unwrap();

        let schema_right = Arc::new(Schema::new(vec![
            Field::new("build_key", ArrowDataType::Int64, false),
            Field::new("s_name", ArrowDataType::Utf8, false),
            Field::new("s_metric", ArrowDataType::Int64, false),
        ]));
        let right_batch = RecordBatch::try_new(
            schema_right,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "supplier#1",
                    "supplier#2",
                    "supplier#3",
                ])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let join = HashJoinExec {
            left: empty_right_source(),
            right: empty_right_source(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let hash_map = JoinHashMap::build_single(&right_batch, &[0]).unwrap();

        set_dict_probe_build_for_test(Some(false));
        let take_batches = join.probe(&left_batch, &right_batch, &hash_map).unwrap();

        set_dict_probe_build_for_test(Some(true));
        let dict_batches = join.probe(&left_batch, &right_batch, &hash_map).unwrap();
        set_dict_probe_build_for_test(None);

        assert_eq!(take_batches.len(), 1);
        assert_eq!(dict_batches.len(), 1);
        let take_batch = &take_batches[0];
        let dict_batch =
            materialize_batch_to_schema(&dict_batches[0], take_batch.schema()).unwrap();

        assert_eq!(take_batch.num_rows(), dict_batch.num_rows());
        assert_eq!(take_batch.num_columns(), dict_batch.num_columns());
        assert!(matches!(
            dict_batches[0].column(2).data_type(),
            ArrowDataType::Dictionary(_, value_type) if value_type.as_ref() == &ArrowDataType::Utf8
        ));
        assert!(matches!(
            dict_batches[0].column(3).data_type(),
            ArrowDataType::Dictionary(_, value_type) if value_type.as_ref() == &ArrowDataType::Int64
        ));

        for col in 0..take_batch.num_columns() {
            let expected = take_batch.column(col);
            let actual = dict_batch.column(col);
            for row in 0..take_batch.num_rows() {
                assert_eq!(
                    crate::operator::extract_scalar(expected, row).unwrap(),
                    crate::operator::extract_scalar(actual, row).unwrap(),
                    "mismatch at column {col}, row {row}"
                );
            }
        }
    }

    fn materialize_batch_to_schema(
        batch: &RecordBatch,
        schema: Arc<Schema>,
    ) -> Result<RecordBatch, ExecutionError> {
        let columns = batch
            .columns()
            .iter()
            .zip(schema.fields())
            .map(|(col, field)| {
                if col.data_type() == field.data_type() {
                    Ok(col.clone())
                } else {
                    compute::cast(col, field.data_type()).map_err(ExecutionError::from)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn hash_join_for_stream_probe_equivalence(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> HashJoinExec {
        HashJoinExec {
            left,
            right,
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        }
    }

    fn sorted_rows(batches: &[RecordBatch]) -> Vec<String> {
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let values = (0..batch.num_columns())
                    .map(|col| {
                        arrow::util::display::array_value_to_string(batch.column(col), row)
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>()
                    .join("|");
                rows.push(values);
            }
        }
        rows.sort();
        rows
    }

    async fn run_stream_probe_equivalence_case(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) {
        std::env::remove_var("ARNEB_STREAM_HASH_PROBE");
        let collect_join =
            hash_join_for_stream_probe_equivalence(Arc::clone(&left), Arc::clone(&right));
        let collect_batches = collect_stream(collect_join.execute(0).await.expect("execute off"))
            .await
            .expect("collect off");

        std::env::set_var("ARNEB_STREAM_HASH_PROBE", "1");
        let stream_join = hash_join_for_stream_probe_equivalence(left, right);
        let stream_batches = collect_stream(stream_join.execute(0).await.expect("execute on"))
            .await
            .expect("collect on");
        std::env::remove_var("ARNEB_STREAM_HASH_PROBE");

        assert_eq!(sorted_rows(&stream_batches), sorted_rows(&collect_batches));
    }

    #[tokio::test]
    async fn stream_hash_probe_matches_collect_for_multi_batch_inner() {
        let _g = env_test_guard().await;
        let left = partitioned_left_source(vec![
            vec![vec![1, 2], vec![10, 11], vec![3, 4]],
            vec![vec![5, 6]],
        ]);
        run_stream_probe_equivalence_case(left, right_source()).await;
    }

    #[tokio::test]
    async fn stream_hash_probe_matches_collect_for_empty_left() {
        let _g = env_test_guard().await;
        let left = partitioned_left_source(vec![vec![], vec![vec![2]]]);
        run_stream_probe_equivalence_case(left, right_source()).await;
    }

    #[tokio::test]
    async fn stream_hash_probe_matches_collect_for_empty_right() {
        let _g = env_test_guard().await;
        let left = partitioned_left_source(vec![vec![vec![1, 2], vec![3, 4]], vec![vec![5]]]);
        run_stream_probe_equivalence_case(left, empty_right_source()).await;
    }

    #[tokio::test]
    async fn hash_join_inner() {
        let join = HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2); // id 2 and 3 match
    }

    #[test]
    fn int32_and_int64_equal_values_hash_same() {
        let i32_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "k",
                ArrowDataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![42])) as ArrayRef],
        )
        .unwrap();
        let i64_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "k",
                ArrowDataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![42])) as ArrayRef],
        )
        .unwrap();

        let i32_keys = TypedKeys::new(&i32_batch, &[0]).unwrap();
        let i64_keys = TypedKeys::new(&i64_batch, &[0]).unwrap();

        assert_eq!(i32_keys.hash_row(0), i64_keys.hash_row(0));
    }

    #[test]
    fn row_eq_matches_cross_width_integer_values() {
        let i32_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "k",
                ArrowDataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![42, 43])) as ArrayRef],
        )
        .unwrap();
        let i64_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "k",
                ArrowDataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![42, 44])) as ArrayRef],
        )
        .unwrap();

        let i32_keys = TypedKeys::new(&i32_batch, &[0]).unwrap();
        let i64_keys = TypedKeys::new(&i64_batch, &[0]).unwrap();

        assert!(i32_keys.row_eq(0, &i64_keys, 0));
        assert!(i64_keys.row_eq(0, &i32_keys, 0));
        assert!(!i32_keys.row_eq(1, &i64_keys, 1));
    }

    #[tokio::test]
    async fn hash_join_inner_matches_int64_probe_to_int32_build_key() {
        let join = HashJoinExec {
            left: key_value_source_i64("left_i64", "lk", "lv", vec![1, 2, 3], vec![10, 20, 30]),
            right: key_value_source_i32(
                "right_i32",
                "rk",
                "rv",
                vec![2, 3, 4],
                vec![200, 300, 400],
            ),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };

        let batches = collect_stream(join.execute(0).await.unwrap())
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        assert_eq!(sorted_rows(&batches), vec!["2|20|2|200", "3|30|3|300"]);
    }

    #[tokio::test]
    async fn hash_join_inner_still_matches_int64_to_int64_key() {
        let join = HashJoinExec {
            left: key_value_source_i64("left_i64", "lk", "lv", vec![1, 2, 3], vec![10, 20, 30]),
            right: key_value_source_i64(
                "right_i64",
                "rk",
                "rv",
                vec![2, 3, 4],
                vec![200, 300, 400],
            ),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };

        let batches = collect_stream(join.execute(0).await.unwrap())
            .await
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        assert_eq!(sorted_rows(&batches), vec!["2|20|2|200", "3|30|3|300"]);
    }

    #[test]
    fn vectorized_probe_hash_matches_scalar() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i32", ArrowDataType::Int32, true),
            Field::new("i64", ArrowDataType::Int64, true),
            Field::new("date", ArrowDataType::Date32, true),
            Field::new("utf8", ArrowDataType::Utf8, true),
            Field::new("f64", ArrowDataType::Float64, true),
            Field::new("dec", ArrowDataType::Decimal128(12, 2), true),
        ]));
        let decimal =
            Decimal128Array::from(vec![Some(12345), None, Some(-67890), Some(0), Some(42)])
                .with_precision_and_scale(12, 2)
                .unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(2),
                    None,
                    Some(-4),
                    Some(5),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(10),
                    None,
                    Some(30),
                    Some(-40),
                    Some(50),
                ])) as ArrayRef,
                Arc::new(Date32Array::from(vec![
                    Some(18_000),
                    Some(18_001),
                    None,
                    Some(18_003),
                    Some(18_004),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    None,
                    Some("gamma"),
                    Some(""),
                    Some("epsilon"),
                ])) as ArrayRef,
                Arc::new(Float64Array::from(vec![
                    Some(1.5),
                    Some(-0.0),
                    None,
                    Some(f64::NAN),
                    Some(9.25),
                ])) as ArrayRef,
                Arc::new(decimal) as ArrayRef,
            ],
        )
        .unwrap();

        let key_sets: &[&[usize]] = &[
            &[0],
            &[1],
            &[2],
            &[3],
            &[4],
            &[5],
            &[0, 1],
            &[2, 3],
            &[4, 5],
        ];
        for keys in key_sets {
            let typed = TypedKeys::new(&batch, keys).unwrap();
            let hashes = typed.hash_batch(batch.num_rows());
            for (row, hash) in hashes.iter().enumerate() {
                assert_eq!(
                    *hash,
                    typed.hash_row(row),
                    "hash mismatch for keys {keys:?}, row {row}"
                );
            }
        }
    }

    #[tokio::test]
    async fn vectorized_probe_output_matches_scalar() {
        let _g = env_test_guard().await;

        fn join_for_probe(left_keys: Vec<usize>, right_keys: Vec<usize>) -> HashJoinExec {
            HashJoinExec {
                left: left_source(),
                right: right_source(),
                join_type: ast::JoinType::Inner,
                left_keys,
                right_keys,
                residual: None,
                build_state: Default::default(),
                peak_build_bytes: Default::default(),
                memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
                dynamic_filter_producers: Vec::new(),
                dynamic_filter_publisher: None,
                dynamic_filtering_enabled: false,
                df_targets: Vec::new(),
            }
        }

        fn single_key_rows(batches: &[RecordBatch]) -> Vec<(i32, i32, i32, i32)> {
            let mut rows = Vec::new();
            for batch in batches {
                let l_key = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let l_val = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let r_key = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let r_val = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    rows.push((
                        l_key.value(row),
                        l_val.value(row),
                        r_key.value(row),
                        r_val.value(row),
                    ));
                }
            }
            rows
        }

        fn multi_key_rows(batches: &[RecordBatch]) -> Vec<(i32, i32, i32, i32, i32, i32)> {
            let mut rows = Vec::new();
            for batch in batches {
                let cols: Vec<&Int32Array> = (0..6)
                    .map(|i| {
                        batch
                            .column(i)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                    })
                    .collect();
                for row in 0..batch.num_rows() {
                    rows.push((
                        cols[0].value(row),
                        cols[1].value(row),
                        cols[2].value(row),
                        cols[3].value(row),
                        cols[4].value(row),
                        cols[5].value(row),
                    ));
                }
            }
            rows
        }

        let single_left_schema = Arc::new(Schema::new(vec![
            Field::new("k", ArrowDataType::Int32, true),
            Field::new("lv", ArrowDataType::Int32, true),
        ]));
        let single_right_schema = Arc::new(Schema::new(vec![
            Field::new("k", ArrowDataType::Int32, true),
            Field::new("rv", ArrowDataType::Int32, true),
        ]));
        let single_left = RecordBatch::try_new(
            single_left_schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(3)])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef,
            ],
        )
        .unwrap();
        let single_right = RecordBatch::try_new(
            single_right_schema,
            vec![
                Arc::new(Int32Array::from(vec![2, 3, 4])) as ArrayRef,
                Arc::new(Int32Array::from(vec![200, 300, 400])) as ArrayRef,
            ],
        )
        .unwrap();
        let join = join_for_probe(vec![0], vec![0]);
        let single_map = JoinHashMap::build_single(&single_right, &[0]).unwrap();

        std::env::set_var("ARNEB_VECTORIZED_PROBE", "0");
        let single_scalar = join
            .probe(&single_left, &single_right, &single_map)
            .unwrap();
        std::env::set_var("ARNEB_VECTORIZED_PROBE", "1");
        let single_vectorized = join
            .probe(&single_left, &single_right, &single_map)
            .unwrap();
        assert_eq!(
            single_key_rows(&single_vectorized),
            single_key_rows(&single_scalar)
        );

        let multi_left_schema = Arc::new(Schema::new(vec![
            Field::new("k1", ArrowDataType::Int32, true),
            Field::new("k2", ArrowDataType::Int32, true),
            Field::new("lv", ArrowDataType::Int32, true),
        ]));
        let multi_right_schema = Arc::new(Schema::new(vec![
            Field::new("k1", ArrowDataType::Int32, true),
            Field::new("k2", ArrowDataType::Int32, true),
            Field::new("rv", ArrowDataType::Int32, true),
        ]));
        let multi_left = RecordBatch::try_new(
            multi_left_schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(2), None])) as ArrayRef,
                Arc::new(Int32Array::from(vec![
                    Some(10),
                    Some(20),
                    Some(99),
                    Some(30),
                ])) as ArrayRef,
                Arc::new(Int32Array::from(vec![100, 200, 299, 300])) as ArrayRef,
            ],
        )
        .unwrap();
        let multi_right = RecordBatch::try_new(
            multi_right_schema,
            vec![
                Arc::new(Int32Array::from(vec![2, 2, 3])) as ArrayRef,
                Arc::new(Int32Array::from(vec![20, 99, 30])) as ArrayRef,
                Arc::new(Int32Array::from(vec![220, 2990, 330])) as ArrayRef,
            ],
        )
        .unwrap();
        let join = join_for_probe(vec![0, 1], vec![0, 1]);
        let multi_map = JoinHashMap::build_single(&multi_right, &[0, 1]).unwrap();

        std::env::set_var("ARNEB_VECTORIZED_PROBE", "0");
        let multi_scalar = join.probe(&multi_left, &multi_right, &multi_map).unwrap();
        std::env::set_var("ARNEB_VECTORIZED_PROBE", "1");
        let multi_vectorized = join.probe(&multi_left, &multi_right, &multi_map).unwrap();
        std::env::remove_var("ARNEB_VECTORIZED_PROBE");
        assert_eq!(
            multi_key_rows(&multi_vectorized),
            multi_key_rows(&multi_scalar)
        );
    }

    fn broadcast_df_join(publisher: crate::DynamicFilterPublisherRef) -> HashJoinExec {
        HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: vec![arneb_planner::DynamicFilterProducer {
                id: DynamicFilterId(7),
                build_index: 0,
                probe_index: 0,
                column_name: "id".into(),
            }],
            dynamic_filter_publisher: Some(publisher),
            dynamic_filtering_enabled: true,
            df_targets: Vec::new(),
        }
    }

    async fn wait_for_publish_calls(
        calls: &Arc<Mutex<Vec<(DynamicFilterId, Domain)>>>,
        expected: usize,
    ) {
        for _ in 0..50 {
            if calls.lock().unwrap().len() == expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(calls.lock().unwrap().len(), expected);
    }

    fn broadcast_df_right_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("value", ArrowDataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![2, 3, 5])),
                Arc::new(Int64Array::from(vec![200, 300, 500])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn broadcast_df_gate_on_publishes_single_build_domain() {
        let _env = BROADCAST_DF_TEST_ENV.lock().await;
        std::env::set_var("ARNEB_BROADCAST_DF", "true");
        std::env::remove_var("ARNEB_DISABLE_DF");
        std::env::remove_var("ARNEB_GRACE_HJ");

        let calls = Arc::new(Mutex::new(Vec::new()));
        let publisher = Arc::new(CapturingDynamicFilterPublisher {
            calls: Arc::clone(&calls),
        });
        let join = broadcast_df_join(publisher);

        let stream = join.execute(0).await.unwrap();
        let _ = collect_stream(stream).await.unwrap();
        wait_for_publish_calls(&calls, 1).await;

        let calls = calls.lock().unwrap();
        assert_eq!(calls[0].0, DynamicFilterId(7));
        match &calls[0].1 {
            Domain::DistinctValues(values) => {
                let mut values = values.clone();
                values.sort_by_key(|v| match v {
                    ScalarValue::Int32(v) => *v,
                    other => panic!("expected Int32 value, got {other:?}"),
                });
                assert_eq!(
                    values,
                    vec![
                        ScalarValue::Int32(2),
                        ScalarValue::Int32(3),
                        ScalarValue::Int32(5)
                    ]
                );
            }
            other => panic!("expected DistinctValues, got {other:?}"),
        }

        std::env::remove_var("ARNEB_BROADCAST_DF");
    }

    #[tokio::test]
    async fn broadcast_df_parallel_probe_publishes_once_per_task_with_task_partition_idx() {
        let _env = BROADCAST_DF_TEST_ENV.lock().await;
        std::env::set_var("ARNEB_BROADCAST_DF", "true");
        std::env::remove_var("ARNEB_DISABLE_DF");
        std::env::remove_var("ARNEB_GRACE_HJ");

        let calls = Arc::new(Mutex::new(Vec::new()));
        let right_batch = broadcast_df_right_batch();

        for task_idx in 0..2 {
            let publisher = Arc::new(CapturingPartitionDynamicFilterPublisher {
                task_idx,
                calls: Arc::clone(&calls),
            });
            let join = broadcast_df_join(publisher);

            for probe_partition in 0..8 {
                join.publish_broadcast_cross_fragment_dfs(probe_partition, &right_batch)
                    .await;
            }
        }

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, DynamicFilterId(7));
        assert_eq!(calls[0].1, 0);
        assert_eq!(calls[1].0, DynamicFilterId(7));
        assert_eq!(calls[1].1, 1);
        for (_, _, domain) in calls.iter() {
            match domain {
                Domain::DistinctValues(values) => {
                    assert_eq!(values.len(), 3);
                    assert!(values.contains(&ScalarValue::Int32(2)));
                    assert!(values.contains(&ScalarValue::Int32(3)));
                    assert!(values.contains(&ScalarValue::Int32(5)));
                }
                other => panic!("expected DistinctValues, got {other:?}"),
            }
        }

        std::env::remove_var("ARNEB_BROADCAST_DF");
    }

    fn right_source_key_second() -> Arc<dyn ExecutionPlan> {
        // Build-side join key ("id") sits at COLUMN INDEX 1, not 0, so the DF
        // producer's `build_index` (1) differs from the right_keys SLOT
        // position (0). Extracting by slot publishes the wrong column
        // ("value"); extracting by `build_index` publishes "id".
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", ArrowDataType::Int64, false),
            Field::new("id", ArrowDataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![200, 300, 500])),
                Arc::new(Int32Array::from(vec![2, 3, 5])),
            ],
        )
        .unwrap();
        let ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "value".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
            ],
            vec![batch],
        );
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: "right".into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        })
    }

    fn broadcast_df_join_key_second(publisher: crate::DynamicFilterPublisherRef) -> HashJoinExec {
        HashJoinExec {
            left: left_source(),
            right: right_source_key_second(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![1],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: vec![arneb_planner::DynamicFilterProducer {
                id: DynamicFilterId(9),
                build_index: 1,
                probe_index: 0,
                column_name: "id".into(),
            }],
            dynamic_filter_publisher: Some(publisher),
            dynamic_filtering_enabled: true,
            df_targets: Vec::new(),
        }
    }

    #[tokio::test]
    async fn broadcast_df_publishes_build_index_column_not_keyslot() {
        // Regression: the broadcast publish must extract the DF column by
        // `producer.build_index` (the column index into the full build
        // schema), NOT by the right_keys slot position. With the key at
        // column 1, the slot is 0 → the pre-fix code published "value"
        // (200,300,500); the fix publishes "id" (2,3,5). This was the
        // SF30 "domain published empty/wrong" gap for the broadcast DF path.
        let _env = BROADCAST_DF_TEST_ENV.lock().await;
        std::env::set_var("ARNEB_BROADCAST_DF", "true");
        std::env::remove_var("ARNEB_DISABLE_DF");
        std::env::remove_var("ARNEB_GRACE_HJ");

        let calls = Arc::new(Mutex::new(Vec::new()));
        let publisher = Arc::new(CapturingDynamicFilterPublisher {
            calls: Arc::clone(&calls),
        });
        let join = broadcast_df_join_key_second(publisher);

        let stream = join.execute(0).await.unwrap();
        let _ = collect_stream(stream).await.unwrap();
        wait_for_publish_calls(&calls, 1).await;

        let calls = calls.lock().unwrap();
        assert_eq!(calls[0].0, DynamicFilterId(9));
        match &calls[0].1 {
            Domain::DistinctValues(values) => {
                let mut values = values.clone();
                values.sort_by_key(|v| match v {
                    ScalarValue::Int32(v) => *v,
                    other => panic!("expected Int32 'id' key value, got {other:?}"),
                });
                assert_eq!(
                    values,
                    vec![
                        ScalarValue::Int32(2),
                        ScalarValue::Int32(3),
                        ScalarValue::Int32(5)
                    ]
                );
            }
            other => panic!("expected DistinctValues, got {other:?}"),
        }

        std::env::remove_var("ARNEB_BROADCAST_DF");
    }

    #[tokio::test]
    async fn grace_df_collector_over_cap_with_bloom_yields_bloom_without_false_negatives() {
        let _env = BROADCAST_DF_TEST_ENV.lock().await;

        let mut collector = DfDistinctCollector::Int64 {
            set: crate::fast_hash::FastHashSet::default(),
            cap: 3,
            bloom: Some(arneb_common::BloomFilter::with_fixed_params()),
        };
        let first: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20, 30]));
        let second: ArrayRef = Arc::new(Int64Array::from(vec![40_i64, 50, 60]));
        collector.accumulate(&first);
        collector.accumulate(&second);

        match collector.finish_domain() {
            Domain::Bloom(bloom) => {
                for value in [10_i64, 20, 30, 40, 50, 60] {
                    assert!(
                        bloom.contains(&ScalarValue::Int64(value)),
                        "bloom missed inserted value {value}"
                    );
                }
            }
            other => panic!("expected Bloom, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn grace_df_publish_uses_non_first_right_key_slot_and_never_skips_producer() {
        let _env = BROADCAST_DF_TEST_ENV.lock().await;
        std::env::remove_var("ARNEB_DISABLE_DF");

        let calls = Arc::new(Mutex::new(Vec::new()));
        let publisher = Arc::new(CapturingDynamicFilterPublisher {
            calls: Arc::clone(&calls),
        });
        let join = HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0, 1],
            right_keys: vec![0, 2],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: vec![
                arneb_planner::DynamicFilterProducer {
                    id: DynamicFilterId(11),
                    build_index: 2,
                    probe_index: 0,
                    column_name: "swapped_key".into(),
                },
                arneb_planner::DynamicFilterProducer {
                    id: DynamicFilterId(12),
                    build_index: 99,
                    probe_index: 1,
                    column_name: "unmapped_key".into(),
                },
            ],
            dynamic_filter_publisher: Some(publisher),
            dynamic_filtering_enabled: true,
            df_targets: Vec::new(),
        };
        let domains_per_slot = vec![
            Domain::DistinctValues(vec![ScalarValue::Int32(1)]),
            Domain::DistinctValues(vec![ScalarValue::Int32(42)]),
        ];

        join.publish_grace_cross_fragment_dfs(&domains_per_slot);
        wait_for_publish_calls(&calls, 2).await;

        let calls = calls.lock().unwrap();
        assert_eq!(calls[0].0, DynamicFilterId(11));
        assert_eq!(
            calls[0].1,
            Domain::DistinctValues(vec![ScalarValue::Int32(42)])
        );
        assert_eq!(calls[1].0, DynamicFilterId(12));
        assert_eq!(calls[1].1, Domain::All);
    }

    #[tokio::test]
    async fn broadcast_df_gate_off_skips_single_build_publish() {
        let _env = BROADCAST_DF_TEST_ENV.lock().await;
        std::env::remove_var("ARNEB_BROADCAST_DF");
        std::env::remove_var("ARNEB_DISABLE_DF");
        std::env::remove_var("ARNEB_GRACE_HJ");

        let calls = Arc::new(Mutex::new(Vec::new()));
        let publisher = Arc::new(CapturingDynamicFilterPublisher {
            calls: Arc::clone(&calls),
        });
        let join = broadcast_df_join(publisher);

        let stream = join.execute(0).await.unwrap();
        let _ = collect_stream(stream).await.unwrap();
        tokio::task::yield_now().await;

        assert!(calls.lock().unwrap().is_empty());
    }

    // dynamic-filter-provenance-targeting: a build-key filter must reach ONLY
    // the join-key column's owning scan, by INDEX descent — never every scan
    // that happens to share the column NAME. Builds a probe subtree with two
    // same-named `k` columns (a self-join twin) and asserts the descent routes
    // by provenance. The pre-fix name-based descent would land the filter on
    // BOTH scans (the TPC-H Q08 corruption class).
    fn two_col_scan(id_col: &str, ids: Vec<i32>, ks: Vec<i32>) -> Arc<ScanExec> {
        let cols = vec![
            ColumnInfo {
                name: id_col.into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "k".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new(id_col, ArrowDataType::Int32, false),
            Field::new("k", ArrowDataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(ks)),
            ],
        )
        .unwrap();
        Arc::new(ScanExec {
            source: Arc::new(InMemoryDataSource::new(cols, vec![batch])),
            _table_name: id_col.into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        })
    }

    #[test]
    fn dynamic_filter_routes_by_provenance_not_name() {
        let scan_a = two_col_scan("a_id", vec![1, 2, 3], vec![10, 20, 30]);
        let scan_b = two_col_scan("b_id", vec![1, 2, 3], vec![30, 20, 10]);
        // join output schema = [a_id@0, A.k@1, b_id@2, B.k@3] — two `k` columns.
        let join = HashJoinExec {
            left: scan_a.clone(),
            right: scan_b.clone(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let filter = || PlanExpr::InList {
            expr: Box::new(PlanExpr::Column {
                index: 0,
                name: "k".into(),
                span: None,
            }),
            list: vec![PlanExpr::Literal {
                value: arneb_common::types::ScalarValue::Int32(10),
                span: None,
            }],
            negated: false,
            span: None,
        };
        let count = |s: &Arc<ScanExec>| s.dynamic_filters.lock().unwrap().len();

        // Target A.k (output index 1) → only scan_a receives the filter.
        join.inject_dynamic_filter(filter(), 1);
        assert_eq!(count(&scan_a), 1, "A.k filter must reach scan_a");
        assert_eq!(
            count(&scan_b),
            0,
            "A.k filter must NOT reach the same-named twin scan_b (the Q08 misroute)"
        );

        // Target B.k (output index 3) → only scan_b receives the filter.
        join.inject_dynamic_filter(filter(), 3);
        assert_eq!(count(&scan_b), 1, "B.k filter must reach scan_b");
        assert_eq!(count(&scan_a), 1, "scan_a unchanged by the B.k filter");
    }

    #[tokio::test]
    async fn hash_join_left() {
        let join = HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Left,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4); // all 4 left rows (2 matched + 2 unmatched with NULLs)
    }

    #[tokio::test]
    async fn hash_join_right() {
        let join = HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Right,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3); // all 3 right rows (2 matched + 1 unmatched with NULLs)
    }

    #[tokio::test]
    async fn hash_join_full() {
        let join = HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Full,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5); // 2 matched + 2 unmatched left + 1 unmatched right
    }

    #[tokio::test]
    async fn hash_join_no_matches() {
        // Right side has no matching keys.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("val", ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![99, 100])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let right_ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "val".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            vec![batch],
        );
        let right: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source: Arc::new(right_ds),
            _table_name: "right".into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });

        let join = HashJoinExec {
            left: left_source(),
            right,
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn equi_join_detection_simple() {
        let condition = arneb_planner::JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "l.id".into(),
                span: None,
            }),
            op: ast::BinaryOp::Eq,
            right: Box::new(PlanExpr::Column {
                index: 2,
                name: "r.id".into(),
                span: None,
            }),
            span: None,
        });
        let (keys, residual) = extract_equi_join_keys(&condition, 2).unwrap();
        assert_eq!(keys, vec![(0, 0)]);
        assert!(residual.is_none());
    }

    #[test]
    fn equi_join_detection_multi_key() {
        let condition = arneb_planner::JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "l.a".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 2,
                    name: "r.a".into(),
                    span: None,
                }),
                span: None,
            }),
            op: ast::BinaryOp::And,
            right: Box::new(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 1,
                    name: "l.b".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 3,
                    name: "r.b".into(),
                    span: None,
                }),
                span: None,
            }),
            span: None,
        });
        let (keys, residual) = extract_equi_join_keys(&condition, 2).unwrap();
        assert_eq!(keys, vec![(0, 0), (1, 1)]);
        assert!(residual.is_none());
    }

    #[test]
    fn non_equi_returns_none() {
        let condition = arneb_planner::JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "l.id".into(),
                span: None,
            }),
            op: ast::BinaryOp::Gt,
            right: Box::new(PlanExpr::Column {
                index: 2,
                name: "r.id".into(),
                span: None,
            }),
            span: None,
        });
        assert!(extract_equi_join_keys(&condition, 2).is_none());
    }

    #[test]
    fn equi_with_residual_is_captured() {
        // Mirrors TPC-H Q13: `c_custkey = o_custkey AND o_comment NOT LIKE '%x%'`.
        // The equi key is extracted; the non-equi predicate is returned as a
        // residual to be evaluated at join time.
        let condition = arneb_planner::JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "l.id".into(),
                    span: None,
                }),
                op: ast::BinaryOp::Eq,
                right: Box::new(PlanExpr::Column {
                    index: 2,
                    name: "r.id".into(),
                    span: None,
                }),
                span: None,
            }),
            op: ast::BinaryOp::And,
            right: Box::new(PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 3,
                    name: "r.comment".into(),
                    span: None,
                }),
                op: ast::BinaryOp::NotEq,
                right: Box::new(PlanExpr::Literal {
                    value: arneb_common::types::ScalarValue::Utf8("special".into()),
                    span: None,
                }),
                span: None,
            }),
            span: None,
        });
        let (keys, residual) = extract_equi_join_keys(&condition, 2).unwrap();
        assert_eq!(keys, vec![(0, 0)]);
        let residual = residual.expect("residual should be captured");
        // The residual is the original non-equi binary op (column-index 3
        // already points into the joined layout: 2 left cols + col 1 of right).
        match residual {
            PlanExpr::BinaryOp { op, .. } => assert_eq!(op, ast::BinaryOp::NotEq),
            other => panic!("unexpected residual shape: {other:?}"),
        }
    }

    #[tokio::test]
    async fn hash_join_left_with_residual_preserves_unmatched() {
        // Regression test for TPC-H Q13: `LEFT JOIN ... ON k = k AND r != 'skip'`.
        // Left rows whose only matching right row is rejected by the residual
        // must still appear in the output with NULL-padded right columns.
        //
        // left:  (1,a) (2,b) (3,c)
        // right: (1,"keep") (2,"skip") (3,"keep")
        //
        // Expected: id=1 + "keep", id=2 + NULL (residual rejected), id=3 + "keep".
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("tag", ArrowDataType::Utf8, false),
        ]));
        let left_batch = RecordBatch::try_new(
            left_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let left_ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "tag".into(),
                    data_type: DataType::Utf8,
                    nullable: false,
                },
            ],
            vec![left_batch],
        );
        let left: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source: Arc::new(left_ds),
            _table_name: "left".into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("note", ArrowDataType::Utf8, false),
        ]));
        let right_batch = RecordBatch::try_new(
            right_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["keep", "skip", "keep"])),
            ],
        )
        .unwrap();
        let right_ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "note".into(),
                    data_type: DataType::Utf8,
                    nullable: false,
                },
            ],
            vec![right_batch],
        );
        let right: Arc<dyn ExecutionPlan> = Arc::new(ScanExec {
            source: Arc::new(right_ds),
            _table_name: "right".into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        });

        // Residual: joined_batch.column(3) != 'skip'. Indices reference the
        // joined layout (2 left columns + right column 1 => index 3).
        let residual = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 3,
                name: "note".into(),
                span: None,
            }),
            op: ast::BinaryOp::NotEq,
            right: Box::new(PlanExpr::Literal {
                value: arneb_common::types::ScalarValue::Utf8("skip".into()),
                span: None,
            }),
            span: None,
        };

        let join = HashJoinExec {
            left,
            right,
            join_type: ast::JoinType::Left,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: Some(residual),
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };

        let stream = join.execute(0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();

        // Tally by left id, checking whether the right side came back NULL.
        let mut by_id: HashMap<i32, bool> = HashMap::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let notes = batch.column(3);
            for row in 0..batch.num_rows() {
                by_id.insert(ids.value(row), notes.is_null(row));
            }
        }
        assert_eq!(by_id.len(), 3, "all 3 left rows must appear");
        assert!(!by_id[&1], "id=1 should keep its right side");
        assert!(by_id[&2], "id=2 residual rejected → right NULL");
        assert!(!by_id[&3], "id=3 should keep its right side");
    }

    // -----------------------------------------------------------------
    // Phase 3b: chunked spill + multi-pass probe
    // -----------------------------------------------------------------

    /// Multi-batch scan source so the build phase actually sees > 1
    /// batch and can spill mid-way. Single Int32 column matches the
    /// SemiJoinExec spill tests' shape so the budget calibration carries
    /// over (~500-700 bytes per 100-row batch, fits 2 per 2048-byte
    /// pool but spills on the third).
    fn right_source_multi_batch(values_per_batch: Vec<Vec<i32>>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let mut batches = Vec::with_capacity(values_per_batch.len());
        for chunk in values_per_batch {
            let b = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(chunk))])
                .unwrap();
            batches.push(b);
        }
        let ds = InMemoryDataSource::new(
            vec![ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            batches,
        );
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: "right_multi".into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        })
    }

    /// Inner join with a budget tight enough to force spill across
    /// multiple chunks. Verifies multi-pass output matches the single-
    /// pass result (matches on id=2 and id=3 in the test setup).
    ///
    /// Dispatches via `execute_single` (left has 1 partition) → exercises
    /// the Phase 3b.3 wiring through `build_with_spill`. Budget 700 B
    /// is tight enough that every 100-row Int32 batch (~500 B in mem)
    /// triggers a fresh spill on the NEXT batch — every spilled chunk
    /// is a single batch, so the load reservation also stays under
    /// budget (single-batch IPC round-trip ≈ 608 B < 700 B). The D3
    /// probe-collect reservation is released before the multipass re-probe
    /// (see `drop(left_reservation)` in the Multipass arm), so it doesn't
    /// shrink this budget.
    #[tokio::test]
    async fn hash_join_inner_spill_multipass() {
        let mut b1: Vec<i32> = (1000..1100).collect();
        b1[10] = 2; // first match lands on a spilled chunk
        let b2: Vec<i32> = (2000..2100).collect();
        let mut b3: Vec<i32> = (3000..3100).collect();
        b3[25] = 3; // second match lands on a different spilled chunk
        let b4: Vec<i32> = (4000..4100).collect();
        let right = right_source_multi_batch(vec![b1, b2, b3, b4]);
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(700));
        let join = HashJoinExec {
            left: left_source(),
            right,
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: pool,
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.expect("execute ok");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 2,
            "multipass INNER spill must produce same match count as single-pass (id=2, id=3)"
        );
    }

    /// exec-memory-accounting D3: `execute_single`'s probe-side collect is now
    /// pool-tracked, so a large probe under a tight pool FAILS FAST
    /// (ResourceExhausted) instead of being held untracked alongside the
    /// concatenated build through the probe — the q18 SF30 worker-OOM pattern.
    /// Small build (1 row → Single arm, no spill) + big probe (5000 rows) +
    /// tight 500 B pool → the probe collect overflows and errors cleanly.
    #[tokio::test]
    async fn execute_single_probe_collect_fails_fast_under_tight_pool() {
        // Serialize against grace-HJ tests + ensure the non-grace path:
        // a concurrent test leaking ARNEB_GRACE_HJ would route execute(0)
        // to the grace path, which streams (no fail-fast) → flaky panic.
        let _g = env_test_guard().await;
        std::env::remove_var("ARNEB_GRACE_HJ");
        let left = right_source_multi_batch(vec![(0..5000).collect()]); // big probe
        let right = right_source_multi_batch(vec![vec![1]]); // tiny build
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(500));
        let join = HashJoinExec {
            left,
            right,
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: pool,
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        match join.execute(0).await {
            Err(ExecutionError::ResourceExhausted(_)) => {}
            Err(other) => panic!("expected ResourceExhausted, got {other:?}"),
            Ok(_) => {
                panic!("expected fail-fast under tight pool, got Ok (probe collect untracked?)")
            }
        }
    }

    /// Phase 3b.3 regression: `execute_single` without spill (budget
    /// large enough to keep everything in memory) must produce the same
    /// rows as the pre-extraction code path.
    #[tokio::test]
    async fn execute_single_no_spill_regression() {
        let join = HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::GreedyMemoryPool::new(65_536)),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.expect("execute ok");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "id 2 and 3 match");
    }

    /// Phase 3b.3 fallback: LEFT-outer joins use the no-spill path
    /// even under a tight pool budget — multipass can't reconstruct
    /// cross-chunk `left_matched` state, but the no-spill collect
    /// preserves correctness (as long as the build fits in container
    /// memory). This guards against regressing TPC-H Q13 style
    /// queries.
    #[tokio::test]
    async fn execute_single_left_uses_no_spill_fallback() {
        let mut b1: Vec<i32> = (1000..1100).collect();
        b1[10] = 2;
        let b2: Vec<i32> = (2000..2100).collect();
        let mut b3: Vec<i32> = (3000..3100).collect();
        b3[25] = 3;
        let b4: Vec<i32> = (4000..4100).collect();
        let right = right_source_multi_batch(vec![b1, b2, b3, b4]);
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(1024));
        let join = HashJoinExec {
            left: left_source(),
            right,
            join_type: ast::JoinType::Left,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: pool,
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join
            .execute(0)
            .await
            .expect("LEFT join must succeed via no-spill fallback");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 4 left rows: id=2 and id=3 match, id=1 and id=4 unmatched
        // (NULL-padded right). LEFT semantics preserve every left row.
        assert_eq!(total, 4, "LEFT join keeps every left row");
    }

    /// A probe stream of `n` 1-row batches behind a shared pull counter — the
    /// counter tells whether a downstream operator DRAINED the stream (counter
    /// == n) or DROPPED it un-drained (counter < n). The batch schema is a
    /// standalone single-Int32 column: the empty-build drain paths under test
    /// only map-for-errors-and-discard, so it need not match the join schema.
    fn counting_probe(
        n: usize,
        pulled: Arc<std::sync::atomic::AtomicUsize>,
    ) -> SendableRecordBatchStream {
        use std::sync::atomic::Ordering;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "p",
            ArrowDataType::Int32,
            false,
        )]));
        let yield_schema = schema.clone();
        let inner = async_stream::try_stream! {
            for i in 0..n {
                pulled.fetch_add(1, Ordering::SeqCst);
                yield RecordBatch::try_new(
                    yield_schema.clone(),
                    vec![Arc::new(Int32Array::from(vec![i as i32]))],
                )
                .unwrap();
            }
        };
        Box::pin(AsyncBatchStream {
            schema,
            inner: Box::pin(inner),
        })
    }

    /// q21/q02 SF30 silent-truncation guard (2026-06-12), the multi-batch
    /// sibling: `execute_single_finish_streaming_multi` must DRAIN its probe
    /// (`left_stream`) to EOF when the INNER build is empty — dropping it
    /// un-drained closes the consumer mid-stream and the upstream producer
    /// truncates the partition. Pre-fix the probe was never pulled (counter 0).
    #[tokio::test]
    async fn grace_single_multi_empty_build_drains_probe() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let join = HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        // A multi-batch build (>1 batch) whose rows are all empty hits the
        // empty-INNER-build fast path of the _multi_ probe.
        let right_arrow = column_info_to_arrow_schema(&join.right.schema());
        let empty_build = vec![
            RecordBatch::new_empty(right_arrow.clone()),
            RecordBatch::new_empty(right_arrow),
        ];
        let pulled = Arc::new(AtomicUsize::new(0));
        let probe = counting_probe(3, pulled.clone());

        let out = join
            .execute_single_finish_streaming_multi(probe, empty_build)
            .expect("multi empty-build path");
        let batches = collect_stream(out).await.expect("collect ok");
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            0,
            "empty INNER build yields no output rows"
        );
        assert_eq!(
            pulled.load(Ordering::SeqCst),
            3,
            "probe MUST be fully drained, not dropped (q21/q02 silent-truncation guard)"
        );
    }

    /// q21/q02 SF30 silent-truncation root-cause fix (bba7107): the grace
    /// single-build empty-build + probe-OVERFLOW arm must DRAIN the un-collected
    /// probe remainder (`rest`) to EOF, not drop it. Forced via
    /// `ARNEB_PROBE_COLLECT_MAX_BYTES=1` (any non-empty probe overflows the cap)
    /// over an empty build (zero batches → `right_combined = None`).
    #[tokio::test]
    async fn grace_single_empty_build_overflow_drains_rest() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let _g = env_test_guard().await;
        std::env::set_var("ARNEB_PROBE_COLLECT_MAX_BYTES", "1");

        let join = HashJoinExec {
            left: left_source(),
            right: right_source(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        // Empty build (zero batches) → Single{batches: empty} → right_combined
        // None.
        let right_arrow = column_info_to_arrow_schema(&join.right.schema());
        let build = stream_from_batches(right_arrow, vec![]);
        let pulled = Arc::new(AtomicUsize::new(0));
        let probe = counting_probe(4, pulled.clone());

        let out = join
            .execute_grace_single(probe, build)
            .await
            .expect("grace single empty-build overflow path");
        let batches = collect_stream(out).await.expect("collect ok");
        std::env::remove_var("ARNEB_PROBE_COLLECT_MAX_BYTES");

        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            0,
            "empty INNER build yields no output rows"
        );
        assert_eq!(
            pulled.load(Ordering::SeqCst),
            4,
            "probe remainder MUST be drained to EOF, not dropped (q21/q02 \
             silent-truncation root cause, bba7107)"
        );
    }

    #[test]
    fn coalesce_build_batches_preserves_all_rows() {
        let s = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let mk = |v: Vec<i32>| {
            RecordBatch::try_new(s.clone(), vec![Arc::new(Int32Array::from(v))]).unwrap()
        };
        let batches = || {
            vec![
                mk(vec![1, 2]),
                mk(vec![3]),
                mk(vec![4, 5, 6]),
                mk(vec![7]),
                mk(vec![8, 9, 10]),
            ]
        };
        let sorted_ids = |bs: &[RecordBatch]| -> Vec<i32> {
            let mut v: Vec<i32> = bs
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            v.sort();
            v
        };
        let expect: Vec<i32> = (1..=10).collect();

        // target 0 → disabled, unchanged.
        let c0 = coalesce_build_batches(batches(), 0).unwrap();
        assert_eq!(c0.len(), 5);
        assert_eq!(sorted_ids(&c0), expect);

        // huge target → all rows fold into ONE chunk.
        let cbig = coalesce_build_batches(batches(), usize::MAX).unwrap();
        assert_eq!(cbig.len(), 1);
        assert_eq!(cbig[0].num_rows(), 10);
        assert_eq!(sorted_ids(&cbig), expect);

        // any target preserves every row and emits no empty chunks.
        for t in [1usize, 64, 256] {
            let c = coalesce_build_batches(batches(), t).unwrap();
            assert_eq!(sorted_ids(&c), expect, "rows preserved at target={t}");
            assert!(
                c.iter().all(|b| b.num_rows() > 0),
                "no empty chunk at target={t}"
            );
            assert!(c.len() <= 5);
        }
    }

    /// q07 deep-join latency lever (2026-06-13): coalescing the multi-batch
    /// build must NOT change the join result — it only regroups build rows.
    /// Runs the multi-batch INNER probe with coalescing maximally ON vs OFF and
    /// asserts identical output.
    #[tokio::test]
    async fn coalesced_multi_build_join_matches_uncoalesced() {
        let _g = env_test_guard().await;
        let join = HashJoinExec {
            left: left_source(),   // ids 1,2,3,4
            right: right_source(), // ids 2,3,5
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let right_arrow = column_info_to_arrow_schema(&join.right.schema());
        // The build, split into 3 tiny batches (>1 → multi path).
        let build = || {
            vec![
                RecordBatch::try_new(
                    right_arrow.clone(),
                    vec![
                        Arc::new(Int32Array::from(vec![2])),
                        Arc::new(Int64Array::from(vec![200])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    right_arrow.clone(),
                    vec![
                        Arc::new(Int32Array::from(vec![3])),
                        Arc::new(Int64Array::from(vec![300])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    right_arrow.clone(),
                    vec![
                        Arc::new(Int32Array::from(vec![5])),
                        Arc::new(Int64Array::from(vec![500])),
                    ],
                )
                .unwrap(),
            ]
        };
        // coalescing disabled.
        std::env::set_var("ARNEB_BUILD_COALESCE_BYTES", "0");
        let probe = join.left.execute(0).await.unwrap();
        let off: usize = collect_stream(
            join.execute_single_finish_streaming_multi(probe, build())
                .unwrap(),
        )
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();

        // fold all 3 build batches into one.
        std::env::set_var("ARNEB_BUILD_COALESCE_BYTES", "1000000000");
        let probe = join.left.execute(0).await.unwrap();
        let on: usize = collect_stream(
            join.execute_single_finish_streaming_multi(probe, build())
                .unwrap(),
        )
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
        std::env::remove_var("ARNEB_BUILD_COALESCE_BYTES");

        assert_eq!(
            off, 2,
            "ids 2,3 match between probe {{1,2,3,4}} and build {{2,3,5}}"
        );
        assert_eq!(
            on, off,
            "coalescing the build must not change the join result"
        );
    }

    /// Phase 3b.3 fallback: INNER + residual also falls through to
    /// the no-spill path under tight budget — residual-aware
    /// multipass is a follow-up.
    #[tokio::test]
    async fn execute_single_residual_uses_no_spill_fallback() {
        let mut b1: Vec<i32> = (1000..1100).collect();
        b1[10] = 2;
        let b2: Vec<i32> = (2000..2100).collect();
        let mut b3: Vec<i32> = (3000..3100).collect();
        b3[25] = 3;
        let b4: Vec<i32> = (4000..4100).collect();
        let right = right_source_multi_batch(vec![b1, b2, b3, b4]);
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(1024));
        // Trivially-true residual: right.id > 0 (joined index 2).
        let residual = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 2,
                name: "id".into(),
                span: None,
            }),
            op: ast::BinaryOp::Gt,
            right: Box::new(PlanExpr::Literal {
                value: arneb_common::types::ScalarValue::Int32(0),
                span: None,
            }),
            span: None,
        };
        let join = HashJoinExec {
            left: left_source(),
            right,
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: Some(residual),
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: pool,
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join
            .execute(0)
            .await
            .expect("INNER+residual must succeed via no-spill fallback");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, 2,
            "INNER residual keeps the 2 matching rows (id=2, id=3)"
        );
    }

    // -----------------------------------------------------------------
    // Phase 3b.5c: build_with_partitioned_spill
    // -----------------------------------------------------------------

    /// Helper: drive `build_with_partitioned_spill` against an
    /// in-memory source plan and return the result. Avoids re-spinning
    /// the standalone right_source helpers.
    /// Serializes tests that mutate the process-global cache-fit / grace
    /// `ARNEB_*` env vars against tests that read them. `cargo test` runs
    /// tests within a binary in parallel, so without this a write-test
    /// could flip the gate under a concurrent read-test. A `tokio::sync`
    /// mutex (not `std::sync`) so the guard can be held across the
    /// `.await` points in the async test bodies without tripping
    /// `clippy::await_holding_lock`; tokio mutexes don't poison.
    static ENV_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn env_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
        ENV_TEST_LOCK.lock().await
    }

    async fn drive_partitioned_build(
        source: Arc<dyn ExecutionPlan>,
        key_idx: usize,
        n_partitions: usize,
        budget: usize,
    ) -> Result<BuildChunksResult, ExecutionError> {
        let stream = source.execute(0).await?;
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(budget));
        let partitioner = Arc::new(
            HashPartitioner::new(
                vec![PlanExpr::Column {
                    index: key_idx,
                    name: "id".into(),
                    span: None,
                }],
                n_partitions,
            )
            .expect("partitioner ok"),
        );
        build_with_partitioned_spill(stream, pool, "test_grace_build", partitioner).await
    }

    /// 2-column Int32 multi-batch source ("k1","k2") for composite-key
    /// cache-fit tests. Each `(a, b)` tuple is one row.
    fn right_source_2key_multi_batch(batches: Vec<Vec<(i32, i32)>>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", ArrowDataType::Int32, false),
            Field::new("k2", ArrowDataType::Int32, false),
        ]));
        let mut recs = Vec::with_capacity(batches.len());
        for chunk in batches {
            let (k1, k2): (Vec<i32>, Vec<i32>) = chunk.into_iter().unzip();
            recs.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int32Array::from(k1)),
                        Arc::new(Int32Array::from(k2)),
                    ],
                )
                .unwrap(),
            );
        }
        let ds = InMemoryDataSource::new(
            vec![
                ColumnInfo {
                    name: "k1".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnInfo {
                    name: "k2".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
            ],
            recs,
        );
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: "right_2key".into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        })
    }

    /// Drive the partitioned build with an arbitrary set of join keys so
    /// the cache-fit composite-key gate (`right_key_indices.len() >= 2`)
    /// is exercised. Unlike [`drive_partitioned_build`] (which routes
    /// through the `&[]`-keys wrapper), this calls the DF-collecting
    /// builder directly with the real key indices.
    async fn drive_partitioned_build_keys(
        source: Arc<dyn ExecutionPlan>,
        keys: &[usize],
        n_partitions: usize,
        budget: usize,
    ) -> Result<BuildChunksResult, ExecutionError> {
        let cols = source.schema();
        let stream = source.execute(0).await?;
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(budget));
        let exprs: Vec<PlanExpr> = keys
            .iter()
            .map(|&i| PlanExpr::Column {
                index: i,
                name: cols[i].name.clone(),
                span: None,
            })
            .collect();
        let partitioner = Arc::new(HashPartitioner::new(exprs, n_partitions).expect("partitioner"));
        Ok(build_with_partitioned_spill_collecting_df(
            stream,
            pool,
            "test_cache_fit",
            partitioner,
            keys,
        )
        .await?
        .result)
    }

    /// 2-key left source for the composite-key grace correctness test:
    /// rows (k1,k2) = (2,20),(3,30),(7,70),(8,80).
    fn left_2key() -> Arc<dyn ExecutionPlan> {
        right_source_2key_multi_batch(vec![vec![(2, 20), (3, 30), (7, 70), (8, 80)]])
    }

    /// 2-key right source: (2,20),(3,30),(9,90). Matches left on the
    /// FULL (k1,k2) tuple at (2,20) and (3,30) → 2 INNER rows. Note
    /// (3,30) matches on both, but e.g. (2,90) would not — verifying the
    /// probe compares BOTH key columns, not just the first.
    fn right_2key() -> Arc<dyn ExecutionPlan> {
        right_source_2key_multi_batch(vec![vec![(2, 20), (3, 30), (9, 90)]])
    }

    /// All partitions fit in budget → result is `Single` (caller will
    /// take the no-spill fast path); no on-disk file is created.
    #[tokio::test]
    async fn partitioned_build_all_in_memory() {
        // Reads the cache-fit env gate (no-spill path) — serialize
        // against the cache-fit write-tests below.
        let _g = env_test_guard().await;
        let right = right_source_multi_batch(vec![(1..=50).collect(), (51..=100).collect()]);
        let result = drive_partitioned_build(right, 0, 4, 1_000_000)
            .await
            .expect("build ok");
        match result {
            BuildChunksResult::Single {
                batches,
                total_bytes,
                ..
            } => {
                let row_sum: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(row_sum, 100, "all 100 rows kept in memory");
                assert!(total_bytes > 0);
            }
            _other => panic!("expected Single variant"),
        }
    }

    /// Tight budget forces at least one partition to spill → result is
    /// `Partitioned`; partitioner is preserved for the probe side.
    #[tokio::test]
    async fn partitioned_build_spills_under_tight_budget() {
        // Each 100-row Int32 batch is ~500 B in arrow buffer terms;
        // budget 700 fits one batch's worth and forces spill on the next.
        let right = right_source_multi_batch(vec![
            (1..=100).collect(),
            (101..=200).collect(),
            (201..=300).collect(),
            (301..=400).collect(),
        ]);
        let result = drive_partitioned_build(right, 0, 4, 700)
            .await
            .expect("build ok");
        match result {
            BuildChunksResult::Partitioned {
                in_mem,
                partitions,
                total_bytes,
                partitioner,
                ..
            } => {
                assert_eq!(in_mem.len(), 4, "4 partition slots");
                assert_eq!(partitioner.n_partitions(), 4);
                assert!(total_bytes > 0);
                // At least one partition spilled.
                let any_spilled =
                    (0..partitions.n_partitions()).any(|p| partitions.has_partition(p));
                assert!(any_spilled, "expected at least one partition file on disk");
                // Combined row count across in-mem + on-disk should be 400.
                let in_mem_rows: usize = in_mem
                    .iter()
                    .flatten()
                    .flat_map(|v| v.iter())
                    .map(|b| b.num_rows())
                    .sum();
                let mut spilled_rows = 0usize;
                for p in 0..partitions.n_partitions() {
                    if let Some(file) = partitions.partition(p) {
                        let reader = file.open_reader().expect("reader ok");
                        for b in reader {
                            spilled_rows += b.expect("batch ok").num_rows();
                        }
                    }
                }
                assert_eq!(
                    in_mem_rows + spilled_rows,
                    400,
                    "every input row must land in exactly one partition (in-mem or on-disk)"
                );
            }
            _other => panic!("expected Partitioned variant"),
        }
    }

    /// Empty right input → result is `Single` with no batches (no
    /// partitioning needed).
    #[tokio::test]
    async fn partitioned_build_empty_input() {
        let right = right_source_multi_batch(vec![]);
        let result = drive_partitioned_build(right, 0, 4, 1024)
            .await
            .expect("build ok");
        match result {
            BuildChunksResult::Single { batches, .. } => {
                assert!(batches.is_empty(), "empty input produces no batches");
            }
            _other => panic!("expected Single variant"),
        }
    }

    /// Phase 3b.5d end-to-end: with `ARNEB_GRACE_HJ=1`, INNER join
    /// goes through the Grace HJ path even under a tight pool budget.
    /// Output must match the non-grace baseline (id=2 and id=3 from
    /// the standard left/right sources).
    #[tokio::test]
    async fn grace_hj_inner_under_spill() {
        // Drive ARNEB_GRACE_HJ on for this test only.
        let _g = env_test_guard().await;
        std::env::set_var("ARNEB_GRACE_HJ", "1");

        let mut b1: Vec<i32> = (1000..1100).collect();
        b1[10] = 2; // first match
        let b2: Vec<i32> = (2000..2100).collect();
        let mut b3: Vec<i32> = (3000..3100).collect();
        b3[25] = 3; // second match
        let b4: Vec<i32> = (4000..4100).collect();
        let right = right_source_multi_batch(vec![b1, b2, b3, b4]);
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(700));
        let join = HashJoinExec {
            left: left_source(),
            right,
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: pool,
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.expect("grace execute ok");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();

        std::env::remove_var("ARNEB_GRACE_HJ");

        assert_eq!(
            total, 2,
            "Grace HJ must produce same INNER match count (id=2, id=3) as non-grace path"
        );
    }

    /// q18 OOM fix (2026-06-08): a single-key INNER build that fits in
    /// memory (`BuildChunksResult::Single`) but whose PROBE overflows the
    /// pool budget must take the bounded STREAMING probe (overflow →
    /// `execute_grace_inner` against a 1-partition build) and still produce
    /// the correct matches — no collect of the whole probe. Probe is 3
    /// batches of 2000 rows; id=2 lives in batch 0, id=3 in batch 1 (rest
    /// non-matching), build is {2,3,5}, so INNER yields exactly 2 rows. A
    /// 10 KB pool admits the tiny build + one ~8 KB probe batch then refuses
    /// the second → the Overflow branch fires.
    #[tokio::test]
    async fn grace_single_probe_overflow_streams_and_is_correct() {
        let _g = env_test_guard().await;
        std::env::set_var("ARNEB_GRACE_HJ", "1");

        let mut b0: Vec<i32> = (10_000..12_000).collect();
        b0[0] = 2;
        let mut b1: Vec<i32> = (20_000..22_000).collect();
        b1[0] = 3;
        let b2: Vec<i32> = (30_000..32_000).collect();
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(10_000));
        let join = HashJoinExec {
            left: left_source_multi_batch(vec![b0, b1, b2]),
            right: right_source(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: pool,
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.expect("grace single execute ok");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();

        std::env::remove_var("ARNEB_GRACE_HJ");

        assert_eq!(
            total, 2,
            "streaming single-build probe (overflow path) must match id=2 and id=3"
        );
    }

    /// q09 SF30 cgroup-OOM retainer (2026-06-11): the grace single-build
    /// path must HOLD the build reservation through the probe. The build's
    /// in-memory `right_combined` is probed against while the output stream
    /// is alive, so dropping its reservation (the old `let _ = reservation`)
    /// left that working set UNTRACKED — the pool under-counted, let the
    /// probe + sibling joins grow into the "free" budget, and RSS overshot
    /// the cgroup cap → worker OOM. The non-grace `execute_single` already
    /// holds it (D3), but q09 runs the grace path with `ARNEB_GRACE_HJ=1`.
    ///
    /// Discriminator: an EMPTY probe (left) reserves 0 probe bytes, so any
    /// nonzero `reserved()` while the output stream is alive is the build
    /// reservation. Before the fix it was dropped → `reserved() == 0`.
    #[tokio::test]
    async fn grace_single_holds_build_reservation_through_probe() {
        let _g = env_test_guard().await;
        std::env::set_var("ARNEB_GRACE_HJ", "1");

        // Greedy pool tracks reservations (Unbounded would not). Budget is
        // generous so the small build does not spill (→ Single arm) and the
        // empty probe trivially fits.
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(10_000_000));
        let join = HashJoinExec {
            left: left_source_multi_batch(vec![]), // empty probe
            right: right_source(),                 // non-empty build {2,3,5}
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::clone(&pool),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };

        let stream = join.execute(0).await.expect("grace single execute ok");
        // Build is done + probed against; its reservation must still be held
        // while the output stream is alive.
        let reserved_while_alive = pool.reserved();
        assert!(
            reserved_while_alive > 0,
            "grace single build reservation must be held through the probe \
             (got reserved()={reserved_while_alive}; the build was dropped untracked)"
        );

        let batches = collect_stream(stream).await.expect("collect ok");
        std::env::remove_var("ARNEB_GRACE_HJ");

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0, "empty probe → empty INNER output");
        assert_eq!(
            pool.reserved(),
            0,
            "all held reservations released once the output stream drops"
        );
    }

    /// q09 SF30 concat-free build (2026-06-11): the multi-batch INNER probe
    /// (`compute::interleave` over the build batches, no `concat_batches`)
    /// must produce exactly the same joined rows as concatenating the build
    /// into one batch and probing it. Build key 2 spans BOTH batches, which
    /// exercises the `(batch_idx << 32 | row_idx)` packing and a cross-batch
    /// multi-match.
    #[test]
    fn multi_batch_build_inner_matches_single_batch_concat() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("r_key", DataType::Int32, true),
            Field::new("r_val", DataType::Int32, true),
        ]));
        let mk_r = |keys: Vec<i32>, vals: Vec<i32>| {
            RecordBatch::try_new(
                right_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(keys)) as ArrayRef,
                    Arc::new(Int32Array::from(vals)) as ArrayRef,
                ],
            )
            .unwrap()
        };
        // key 2 appears in BOTH batches (batch0 row1, batch1 row0).
        let right_batches = vec![
            mk_r(vec![1, 2, 3], vec![10, 20, 30]),
            mk_r(vec![2, 4], vec![21, 40]),
        ];

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("l_key", DataType::Int32, true),
            Field::new("l_val", DataType::Int32, true),
        ]));
        let left = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 3, 5])) as ArrayRef,
                Arc::new(Int32Array::from(vec![200, 300, 500])) as ArrayRef,
            ],
        )
        .unwrap();

        let output_schema = Arc::new(Schema::new(
            left_schema
                .fields()
                .iter()
                .chain(right_schema.fields().iter())
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
        ));

        // Multi-batch path (no concat).
        let build = MultiBatchBuild::build(right_batches.clone(), &[0]).unwrap();
        let multi_out = probe_one_left_batch_multi_inner(&left, &build, &[0], &output_schema)
            .unwrap()
            .expect("non-empty INNER output");

        // Reference: concat the build then probe the single batch.
        let right_combined = compute::concat_batches(&right_schema, right_batches.iter()).unwrap();
        let hm = JoinHashMap::build_single(&right_combined, &[0]).unwrap();
        let mut right_matched = vec![false; right_combined.num_rows()];
        let single_vec = probe_one_left_batch(
            &left,
            &right_combined,
            &hm,
            &[0],
            &[0],
            None,
            ast::JoinType::Inner,
            &output_schema,
            &mut right_matched,
        )
        .unwrap();
        let single_out = compute::concat_batches(&output_schema, single_vec.iter()).unwrap();

        let rows = |b: &RecordBatch| -> Vec<(i32, i32, i32, i32)> {
            let c: Vec<&Int32Array> = (0..4)
                .map(|i| b.column(i).as_primitive::<datatypes::Int32Type>())
                .collect();
            let mut v: Vec<_> = (0..b.num_rows())
                .map(|i| (c[0].value(i), c[1].value(i), c[2].value(i), c[3].value(i)))
                .collect();
            v.sort();
            v
        };
        assert_eq!(multi_out.num_rows(), 3, "left 2 matches twice, left 3 once");
        assert_eq!(
            rows(&multi_out),
            rows(&single_out),
            "concat-free multi-batch INNER must equal single-batch concat join"
        );
    }

    #[tokio::test]
    async fn vec_probe_multi_batch_inner_matches_scalar_edge_cases() {
        let _g = env_test_guard().await;
        std::env::set_var("ARNEB_BUILD_COALESCE_BYTES", "0");

        fn low_tag(v: i32) -> u8 {
            use std::hash::{Hash, Hasher};
            let mut hasher = FastHasher::default();
            v.hash(&mut hasher);
            hasher.finish() as u8
        }

        let mut same_tag = None;
        'outer: for a in 10_000..20_000 {
            for b in (a + 1)..20_000 {
                if low_tag(a) == low_tag(b) {
                    same_tag = Some((a, b));
                    break 'outer;
                }
            }
        }
        let (collide_a, collide_b) = same_tag.expect("same low-8 hash tag pair");

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("r_key", ArrowDataType::Int32, true),
            Field::new("r_val", ArrowDataType::Int32, true),
        ]));
        let mk_r = |keys: Vec<Option<i32>>, vals: Vec<i32>| {
            RecordBatch::try_new(
                right_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(keys)) as ArrayRef,
                    Arc::new(Int32Array::from(vals)) as ArrayRef,
                ],
            )
            .unwrap()
        };
        let right_batches = vec![
            mk_r(
                vec![Some(1), Some(collide_a), Some(7), None],
                vec![10, 20, 70, 999],
            ),
            mk_r(vec![Some(collide_b), Some(7), Some(50)], vec![30, 71, 500]),
        ];

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("l_key", ArrowDataType::Int32, true),
            Field::new("l_val", ArrowDataType::Int32, true),
        ]));
        let left = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(collide_a),
                    Some(collide_b),
                    Some(7),
                    None,
                    Some(99),
                ])) as ArrayRef,
                Arc::new(Int32Array::from(vec![100, 200, 300, 700, 0, 990])) as ArrayRef,
            ],
        )
        .unwrap();

        let output_schema = Arc::new(Schema::new(
            left_schema
                .fields()
                .iter()
                .chain(right_schema.fields().iter())
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
        ));
        let build = MultiBatchBuild::build(right_batches, &[0]).unwrap();
        assert!(build.has_collisions, "same low-8 tag keys share a slot");

        let rows = |b: RecordBatch| -> Vec<(i32, i32, i32, i32)> {
            let c: Vec<&Int32Array> = (0..4)
                .map(|i| b.column(i).as_primitive::<datatypes::Int32Type>())
                .collect();
            let mut v: Vec<_> = (0..b.num_rows())
                .map(|i| (c[0].value(i), c[1].value(i), c[2].value(i), c[3].value(i)))
                .collect();
            v.sort_unstable();
            v
        };

        std::env::set_var("ARNEB_VEC_PROBE", "0");
        let scalar = probe_one_left_batch_multi_inner(&left, &build, &[0], &output_schema)
            .unwrap()
            .expect("scalar output");
        std::env::set_var("ARNEB_VEC_PROBE", "1");
        let vectorized = probe_one_left_batch_multi_inner(&left, &build, &[0], &output_schema)
            .unwrap()
            .expect("vectorized output");
        std::env::remove_var("ARNEB_VEC_PROBE");
        std::env::remove_var("ARNEB_BUILD_COALESCE_BYTES");

        let vectorized_rows = rows(vectorized);
        let scalar_rows = rows(scalar);
        assert_eq!(vectorized_rows, scalar_rows);
        assert_eq!(scalar_rows.len(), 5);
    }

    /// Flat (head+next) JoinHashMap contract: the slot chain reaches every
    /// non-null build row, and walking it + a `row_eq` filter reproduces
    /// the exact set of build rows whose key equals the probe key —
    /// including multi-match keys. NULL-keyed build rows are never linked.
    #[test]
    fn flat_join_hash_map_chain_returns_all_key_matches() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
        // rows: 0:5, 1:7, 2:5(dup), 3:NULL, 4:9, 5:5(dup)
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![
                Some(5),
                Some(7),
                Some(5),
                None,
                Some(9),
                Some(5),
            ])) as ArrayRef],
        )
        .unwrap();
        let hm = JoinHashMap::build_single(&batch, &[0]).unwrap();
        let bt = TypedKeys::new(&batch, &[0]).unwrap();

        // Full probe semantics: slot-chain walk + row_eq filter.
        let matches = |probe_row: usize, probe_typed: &TypedKeys<'_>| -> Vec<u32> {
            let mut out = Vec::new();
            let h = probe_typed.hash_row(probe_row);
            let mut r = hm.chain_head(h);
            while r != JoinHashMap::EMPTY {
                if probe_typed.row_eq(probe_row, &bt, r as usize) {
                    out.push(r);
                }
                r = hm.chain_next(r);
            }
            out.sort_unstable();
            out
        };
        assert_eq!(matches(0, &bt), vec![0, 2, 5], "key 5 → build rows 0,2,5");
        assert_eq!(matches(1, &bt), vec![1], "key 7 → build row 1");
        assert_eq!(matches(4, &bt), vec![4], "key 9 → build row 4");

        // Absent key (100) → no equal build row in its slot chain.
        let other = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![100])) as ArrayRef],
        )
        .unwrap();
        let ot = TypedKeys::new(&other, &[0]).unwrap();
        assert!(matches(0, &ot).is_empty(), "absent key 100 → no match");

        // NULL-keyed build row (3) is never linked into any chain.
        let mut reachable = hm.reachable_rows();
        reachable.sort_unstable();
        assert_eq!(
            reachable,
            vec![0, 1, 2, 4, 5],
            "every non-null build row reachable; NULL row 3 excluded"
        );

        // heap_bytes accounts for head + next arrays only (no per-key Vec).
        assert_eq!(
            hm.heap_bytes(),
            (hm.head_len() + 6) * std::mem::size_of::<u32>(),
            "heap_bytes = (slots + n_rows) * 4"
        );
    }

    /// Cache-fit gate (2026-05-30): a COMPOSITE-key build (>= 2 join
    /// keys) that fits in memory but exceeds the threshold must be kept
    /// PARTITIONED instead of flattened to `Single`. No partition file
    /// lands on disk; all rows stay in `in_mem`.
    #[tokio::test]
    async fn cache_fit_composite_key_keeps_partitioned() {
        let _g = env_test_guard().await;
        std::env::set_var("ARNEB_CACHE_FIT_HJ", "1");
        std::env::set_var("ARNEB_CACHE_FIT_THRESHOLD_MB", "0");

        let right = right_source_2key_multi_batch(vec![
            (1..=50).map(|i| (i, i + 1000)).collect(),
            (51..=100).map(|i| (i, i + 1000)).collect(),
        ]);
        let result = drive_partitioned_build_keys(right, &[0, 1], 4, 1_000_000).await;

        std::env::remove_var("ARNEB_CACHE_FIT_HJ");
        std::env::remove_var("ARNEB_CACHE_FIT_THRESHOLD_MB");

        match result.expect("build ok") {
            BuildChunksResult::Partitioned {
                in_mem,
                partitions,
                total_bytes,
                partitioner,
                ..
            } => {
                assert_eq!(partitioner.n_partitions(), 4);
                assert!(total_bytes > 0);
                // Cache-fit must NOT touch disk — the empty spill file
                // makes the probe's Pass 2 a no-op.
                let any_spilled =
                    (0..partitions.n_partitions()).any(|p| partitions.has_partition(p));
                assert!(!any_spilled, "cache-fit must keep everything in memory");
                let in_mem_rows: usize = in_mem
                    .iter()
                    .flatten()
                    .flat_map(|v| v.iter())
                    .map(|b| b.num_rows())
                    .sum();
                assert_eq!(in_mem_rows, 100, "all 100 rows retained across partitions");
            }
            _other => panic!("composite-key large build must be Partitioned (cache-fit)"),
        }
    }

    /// Single-key gate: a SINGLE-key build must stay `Single` even when
    /// it is large (threshold 0), because cache-fit's per-partition
    /// routing overhead is a net loss without a multi-column comparison
    /// to make cache-resident (q03/q05/q21 regressed under cache-fit).
    #[tokio::test]
    async fn cache_fit_single_key_stays_single() {
        let _g = env_test_guard().await;
        std::env::set_var("ARNEB_CACHE_FIT_HJ", "1");
        std::env::set_var("ARNEB_CACHE_FIT_THRESHOLD_MB", "0");

        let right = right_source_2key_multi_batch(vec![(1..=100).map(|i| (i, i + 1000)).collect()]);
        // Drive with a SINGLE key → composite-key gate excludes cache-fit.
        let result = drive_partitioned_build_keys(right, &[0], 4, 1_000_000).await;

        std::env::remove_var("ARNEB_CACHE_FIT_HJ");
        std::env::remove_var("ARNEB_CACHE_FIT_THRESHOLD_MB");

        match result.expect("build ok") {
            BuildChunksResult::Single { batches, .. } => {
                let row_sum: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(row_sum, 100, "single-key build must flatten to Single");
            }
            _other => panic!("single-key build must stay Single (composite-key gate)"),
        }
    }

    /// Below the threshold, the cache-fit gate must NOT fire even for a
    /// composite-key build: a small in-memory build still flattens to
    /// `Single`. Guards the routing overhead off tiny builds.
    #[tokio::test]
    async fn cache_fit_below_threshold_stays_single() {
        let _g = env_test_guard().await;
        std::env::set_var("ARNEB_CACHE_FIT_HJ", "1");
        // 100 rows of 2 Int32 cols is well under 1 MiB.
        std::env::set_var("ARNEB_CACHE_FIT_THRESHOLD_MB", "1");

        let right = right_source_2key_multi_batch(vec![(1..=100).map(|i| (i, i + 1000)).collect()]);
        let result = drive_partitioned_build_keys(right, &[0, 1], 4, 1_000_000).await;

        std::env::remove_var("ARNEB_CACHE_FIT_HJ");
        std::env::remove_var("ARNEB_CACHE_FIT_THRESHOLD_MB");

        match result.expect("build ok") {
            BuildChunksResult::Single { batches, .. } => {
                let row_sum: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(
                    row_sum, 100,
                    "small composite-key build flattened to Single"
                );
            }
            _other => panic!("expected Single below threshold"),
        }
    }

    /// End-to-end: with grace + cache-fit on and threshold 0, a
    /// COMPOSITE-key INNER join routes through the per-partition
    /// cache-resident probe (`execute_grace_inner`, all in memory) and
    /// must produce the correct match count. The probe compares BOTH key
    /// columns: left (2,20),(3,30),(7,70),(8,80) ⋈ right
    /// (2,20),(3,30),(9,90) → exactly (2,20) and (3,30) match.
    #[tokio::test]
    async fn cache_fit_composite_grace_correctness() {
        let _g = env_test_guard().await;
        std::env::set_var("ARNEB_GRACE_HJ", "1");
        std::env::set_var("ARNEB_CACHE_FIT_HJ", "1");
        std::env::set_var("ARNEB_CACHE_FIT_THRESHOLD_MB", "0");

        // Large budget → no spill → cache-fit keeps the build partitioned.
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(1_000_000));
        let join = HashJoinExec {
            left: left_2key(),
            right: right_2key(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0, 1],
            right_keys: vec![0, 1],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: pool,
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.expect("cache-fit execute ok");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();

        std::env::remove_var("ARNEB_GRACE_HJ");
        std::env::remove_var("ARNEB_CACHE_FIT_HJ");
        std::env::remove_var("ARNEB_CACHE_FIT_THRESHOLD_MB");

        assert_eq!(
            total, 2,
            "composite-key cache-fit probe must match (2,20) and (3,30) on BOTH key columns"
        );
    }

    /// Parallel cache-fit probe (`ARNEB_PROBE_THREADS=4`): the
    /// multi-chunk concurrent probe must produce the SAME matches as the
    /// sequential path. Multi-batch left so `split_batches_even` yields
    /// more than one chunk. Matches on the full (k1,k2) tuple at (2,20)
    /// and (3,30); (7,70)/(8,80) have no right match.
    #[tokio::test]
    async fn cache_fit_parallel_probe_correctness() {
        let _g = env_test_guard().await;
        std::env::set_var("ARNEB_GRACE_HJ", "1");
        std::env::set_var("ARNEB_CACHE_FIT_HJ", "1");
        std::env::set_var("ARNEB_CACHE_FIT_THRESHOLD_MB", "0");
        std::env::set_var("ARNEB_PROBE_THREADS", "4");
        std::env::set_var("ARNEB_PROBE_MIN_WORK", "0");

        let left =
            right_source_2key_multi_batch(vec![vec![(2, 20), (7, 70)], vec![(3, 30), (8, 80)]]);
        let pool: Arc<dyn crate::memory_pool::MemoryPool> =
            Arc::new(crate::memory_pool::GreedyMemoryPool::new(1_000_000));
        let join = HashJoinExec {
            left,
            right: right_2key(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0, 1],
            right_keys: vec![0, 1],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: pool,
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join
            .execute(0)
            .await
            .expect("parallel cache-fit execute ok");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();

        std::env::remove_var("ARNEB_GRACE_HJ");
        std::env::remove_var("ARNEB_CACHE_FIT_HJ");
        std::env::remove_var("ARNEB_CACHE_FIT_THRESHOLD_MB");
        std::env::remove_var("ARNEB_PROBE_THREADS");
        std::env::remove_var("ARNEB_PROBE_MIN_WORK");

        assert_eq!(
            total, 2,
            "parallel cache-fit probe must match the same rows as sequential (2,20),(3,30)"
        );
    }

    /// Single-key parallel probe (`ARNEB_PROBE_THREADS=4`, no grace):
    /// the INNER flat-hash-map probe routes through
    /// `execute_single_parallel` and must produce the same matches as the
    /// sequential streaming path. Multi-batch left (matches id=2 in batch
    /// 0, id=3 in batch 2; batch 1 has none) so chunks split across
    /// batches.
    #[tokio::test]
    async fn single_key_parallel_probe_correctness() {
        let _g = env_test_guard().await;
        std::env::set_var("ARNEB_PROBE_THREADS", "4");
        std::env::set_var("ARNEB_PROBE_MIN_WORK", "0");

        let join = HashJoinExec {
            left: left_source_multi_batch(vec![vec![1, 2], vec![10, 11], vec![3, 4]]),
            right: right_source(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.expect("parallel single execute ok");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();

        std::env::remove_var("ARNEB_PROBE_THREADS");
        std::env::remove_var("ARNEB_PROBE_MIN_WORK");

        assert_eq!(
            total, 2,
            "single-key parallel probe must match id=2 and id=3 like the sequential path"
        );
    }

    // -----------------------------------------------------------------
    // Phase A streaming refactor (2026-05-23): regression tests for
    // execute_single_finish's per-left-batch streaming probe path.
    // Each test feeds a MULTI-BATCH left input so the new per-batch
    // loop actually iterates (single-batch left was already covered by
    // the existing hash_join_* tests above). Covers each join_type and
    // the residual branch separately.
    // -----------------------------------------------------------------

    /// Multi-batch left source: id columns split across `chunks`,
    /// matched against `right_source()`'s id=2,3,5.
    fn left_source_multi_batch(chunks: Vec<Vec<i32>>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let batches: Vec<RecordBatch> = chunks
            .into_iter()
            .map(|c| {
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(c))]).unwrap()
            })
            .collect();
        let ds = InMemoryDataSource::new(
            vec![ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            batches,
        );
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: "left_multi".into(),
            scan_context: ScanContext::default(),
            dynamic_filters: Default::default(),
            dynamic_filters_consumed: Vec::new(),
            dynamic_filter_collector: None,
            dynamic_filtering_enabled: false,
            dynamic_filtering_wait_timeout: std::time::Duration::from_secs(10),
            scan_task_index: 0,
            scan_task_count: 1,
        })
    }

    /// INNER, multi-batch left. Matches in batch 0 (id=2) and batch 2
    /// (id=3); batch 1 contributes no matches. Verifies the streaming
    /// path doesn't lose or duplicate rows across batch boundaries.
    #[tokio::test]
    async fn streaming_inner_multi_batch_left() {
        let join = HashJoinExec {
            left: left_source_multi_batch(vec![vec![1, 2], vec![10, 11], vec![3, 4]]),
            right: right_source(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.expect("execute ok");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "INNER must match id=2 and id=3 once each");
    }

    /// LEFT, multi-batch left. Verifies that unmatched-left rows are
    /// emitted per batch (rather than collapsed across batches into
    /// one giant unmatched batch).
    #[tokio::test]
    async fn streaming_left_outer_multi_batch_left() {
        let join = HashJoinExec {
            left: left_source_multi_batch(vec![vec![1, 2], vec![10, 11], vec![3, 4]]),
            right: right_source(),
            join_type: ast::JoinType::Left,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.expect("execute ok");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 6 left rows total; 2 match (id=2,3), 4 unmatched (1,10,11,4)
        // — LEFT keeps them all NULL-padded.
        assert_eq!(total, 6, "LEFT must keep every left row across batches");
    }

    /// RIGHT, multi-batch left. The unmatched-right tail (id=5 in
    /// right_source) MUST emit exactly once after all left batches
    /// have been probed — this is the cross-batch state the refactor
    /// has to thread correctly via the `right_matched` accumulator.
    #[tokio::test]
    async fn streaming_right_outer_multi_batch_left() {
        let join = HashJoinExec {
            left: left_source_multi_batch(vec![vec![1, 2], vec![10, 11], vec![3, 4]]),
            right: right_source(),
            join_type: ast::JoinType::Right,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.expect("execute ok");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 2 matches + 1 unmatched right (id=5) = 3.
        assert_eq!(
            total, 3,
            "RIGHT must emit matches + unmatched-right tail (id=5) once across all batches"
        );
    }

    /// FULL outer, multi-batch left. Combines LEFT per-batch
    /// unmatched-left emission AND RIGHT cross-batch unmatched-right
    /// accumulator emission. Highest-coverage test for the streaming
    /// refactor.
    #[tokio::test]
    async fn streaming_full_outer_multi_batch_left() {
        let join = HashJoinExec {
            left: left_source_multi_batch(vec![vec![1, 2], vec![10, 11], vec![3, 4]]),
            right: right_source(),
            join_type: ast::JoinType::Full,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: None,
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.expect("execute ok");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 2 matches + 4 unmatched left (1,10,11,4) + 1 unmatched
        // right (5) = 7.
        assert_eq!(
            total, 7,
            "FULL must emit matches + both unmatched sides without dropping any rows"
        );
    }

    /// INNER + residual filter, multi-batch left. Validates that the
    /// per-batch path applies the residual identically to the old
    /// concat-then-probe path. Trivially-true residual `right.value
    /// > 0` keeps every equi-match.
    #[tokio::test]
    async fn streaming_inner_residual_multi_batch_left() {
        // Joined layout: [left.id, right.id, right.value] — left_source_multi_batch
        // contributes 1 column, right_source contributes 2.
        let residual = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 2,
                name: "value".into(),
                span: None,
            }),
            op: ast::BinaryOp::Gt,
            right: Box::new(PlanExpr::Literal {
                value: arneb_common::types::ScalarValue::Int64(0),
                span: None,
            }),
            span: None,
        };
        let join = HashJoinExec {
            left: left_source_multi_batch(vec![vec![1, 2], vec![10, 11], vec![3, 4]]),
            right: right_source(),
            join_type: ast::JoinType::Inner,
            left_keys: vec![0],
            right_keys: vec![0],
            residual: Some(residual),
            build_state: Default::default(),
            peak_build_bytes: Default::default(),
            memory_pool: Arc::new(crate::memory_pool::UnboundedMemoryPool::new()),
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            df_targets: Vec::new(),
        };
        let stream = join.execute(0).await.expect("execute ok");
        let batches = collect_stream(stream).await.expect("collect ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // INNER residual `right.value > 0` keeps both id=2 (value=200)
        // and id=3 (value=300).
        assert_eq!(
            total, 2,
            "INNER residual must keep matches whose joined right.value > 0"
        );
    }
}
