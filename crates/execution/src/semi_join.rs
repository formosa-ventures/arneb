//! Semi-join and anti-join physical operator.
//!
//! Build phase: concatenate right batches and evaluate the right join
//! key once into a single ArrayRef. Hash via typed-column dispatch
//! (no `array_value_to_string` per row) and stash the row positions
//! in either a `HashSet<u64>` (no residual) or `HashMap<u64,
//! SmallVec<[u32; 1]>>` (residual evaluates against (left_row,
//! right_row) pairs).
//!
//! Probe phase: per left batch, evaluate the left key once, typed-hash
//! every row, and emit a `take`-built output batch. The residual path
//! materialises ONE joined batch per left batch via column take + an
//! `expression::evaluate` against the residual predicate.

use std::fmt;
use std::mem::size_of;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use arneb_common::error::ExecutionError;
use arneb_common::memory_profile::LiveBytesGuard;
use arneb_common::stream::{stream_from_batches, SendableRecordBatchStream};
use arneb_common::types::ColumnInfo;
use arneb_planner::PlanExpr;
use arrow::array::{
    cast::AsArray, Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array,
    Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
};
use arrow::compute;
use arrow::datatypes::{
    DataType as ArrowDataType, Date32Type, Decimal128Type, Field, Float32Type, Float64Type,
    Int32Type, Int64Type, Schema,
};
use async_trait::async_trait;

use crate::expression;
use crate::fast_hash::{FastHashMap, FastHashSet, FastHasher};
use crate::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use crate::operator::ExecutionPlan;
use crate::repartition::HashPartitioner;
use crate::spill::{PartitionedSpillFile, PartitionedSpillWriter, SpillFile, SpillWriter};
use futures::StreamExt;

fn stream_semi_probe_enabled() -> bool {
    #[cfg(test)]
    {
        let enabled = std::env::var("ARNEB_STREAM_SEMI_PROBE")
            .map(|v| {
                matches!(
                    v.as_str(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            enabled,
            "ARNEB_STREAM_SEMI_PROBE"
        );
        enabled
    }

    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| {
            let enabled = std::env::var("ARNEB_STREAM_SEMI_PROBE")
                .map(|v| {
                    matches!(
                        v.as_str(),
                        "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                    )
                })
                .unwrap_or(false);
            tracing::info!(
                target: "arneb::config",
                enabled,
                "ARNEB_STREAM_SEMI_PROBE"
            );
            enabled
        })
    }
}

fn compact_semi_ne_enabled() -> bool {
    #[cfg(test)]
    {
        let enabled = std::env::var("ARNEB_COMPACT_SEMI_NE")
            .map(|v| {
                matches!(
                    v.as_str(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false);
        tracing::info!(target: "arneb::config", enabled, "ARNEB_COMPACT_SEMI_NE");
        enabled
    }

    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| {
            let enabled = std::env::var("ARNEB_COMPACT_SEMI_NE")
                .map(|v| {
                    matches!(
                        v.as_str(),
                        "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                    )
                })
                .unwrap_or(false);
            tracing::info!(target: "arneb::config", enabled, "ARNEB_COMPACT_SEMI_NE");
            enabled
        })
    }
}

pub(crate) fn semi_mark_join_enabled() -> bool {
    #[cfg(test)]
    {
        let enabled = std::env::var("ARNEB_SEMI_MARK_JOIN")
            .map(|v| {
                matches!(
                    v.as_str(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            enabled,
            "ARNEB_SEMI_MARK_JOIN"
        );
        enabled
    }

    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| {
            let enabled = std::env::var("ARNEB_SEMI_MARK_JOIN")
                .map(|v| {
                    matches!(
                        v.as_str(),
                        "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                    )
                })
                .unwrap_or(false);
            tracing::info!(
                target: "arneb::config",
                enabled,
                "ARNEB_SEMI_MARK_JOIN"
            );
            enabled
        })
    }
}

fn partitioned_semi_spill_enabled() -> bool {
    #[cfg(test)]
    {
        let enabled = std::env::var("ARNEB_PARTITIONED_SEMI_SPILL")
            .map(|v| {
                matches!(
                    v.as_str(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            enabled,
            "ARNEB_PARTITIONED_SEMI_SPILL"
        );
        enabled
    }

    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| {
            let enabled = std::env::var("ARNEB_PARTITIONED_SEMI_SPILL")
                .map(|v| {
                    matches!(
                        v.as_str(),
                        "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                    )
                })
                .unwrap_or(false);
            tracing::info!(
                target: "arneb::config",
                enabled,
                "ARNEB_PARTITIONED_SEMI_SPILL"
            );
            enabled
        })
    }
}

fn flat_semi_index_enabled() -> bool {
    #[cfg(test)]
    {
        let enabled = std::env::var("ARNEB_FLAT_SEMI_INDEX")
            .map(|v| {
                matches!(
                    v.as_str(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            enabled,
            "ARNEB_FLAT_SEMI_INDEX"
        );
        enabled
    }

    #[cfg(not(test))]
    {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        *ENABLED.get_or_init(|| {
            let enabled = std::env::var("ARNEB_FLAT_SEMI_INDEX")
                .map(|v| {
                    matches!(
                        v.as_str(),
                        "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                    )
                })
                .unwrap_or(false);
            tracing::info!(
                target: "arneb::config",
                enabled,
                "ARNEB_FLAT_SEMI_INDEX"
            );
            enabled
        })
    }
}

fn spill_fadvise_build_only_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_SPILL_FADVISE_BUILD_ONLY")
            .map(|v| {
                matches!(
                    v.as_str(),
                    "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
                )
            })
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            enabled,
            "ARNEB_SPILL_FADVISE_BUILD_ONLY effective value"
        );
        enabled
    })
}

/// Semi-join (or anti-join) operator.
#[derive(Debug)]
pub(crate) struct SemiJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    left_key: PlanExpr,
    right_key: PlanExpr,
    /// Optional residual predicate evaluated on (left_row, right_row)
    /// candidate pairs. Indices reference the concatenated joined
    /// layout — left columns first, then right.
    residual: Option<PlanExpr>,
    anti: bool,
    /// Gated MARK-JOIN variant: build LEFT and stream RIGHT, marking
    /// matching left rows, then emit from LEFT. Default false preserves
    /// the existing build-RIGHT implementation.
    build_left: bool,
    /// Peak bytes reserved by the build-side materialised state.
    /// Captures the size of `right_combined`'s buffer memory after
    /// the build phase completes, surfaced through
    /// [`ExecutionPlan::peak_bytes_reserved`].
    peak_build_bytes: std::sync::atomic::AtomicUsize,
    /// Memory pool the build-side reservation is registered with.
    /// Defaults (via single-node `ExecutionContext`) to
    /// `UnboundedMemoryPool`; distributed worker tasks install a
    /// `GreedyMemoryPool` sized to the container's cgroup budget so
    /// build allocations fail fast instead of being OOM-killed by the
    /// kernel. Phase 2b will replace the `Err(ResourceExhausted)` path
    /// with on-disk grace hash join semantics.
    memory_pool: Arc<dyn MemoryPool>,
    /// A1.5 (2026-05-27): cross-fragment DF producers — same shape
    /// as on `HashJoinExec`. Populated by the planner from
    /// `LogicalPlan::SemiJoin.dynamic_filter_ids`. AntiJoin (anti =
    /// true) emits nothing because the build side carries the
    /// "should-be-absent" set, which is the wrong semantic for a
    /// probe-side prune.
    dynamic_filter_producers: Vec<arneb_planner::DynamicFilterProducer>,
    /// A1.5: worker-side hook installed by `ExecutionContext`.
    dynamic_filter_publisher: Option<crate::DynamicFilterPublisherRef>,
    /// A1.5: feature flag mirroring `ExecutionContext::dynamic_filtering_enabled`.
    dynamic_filtering_enabled: bool,
    /// Z (2026-06-05): optional override for the per-chunk build-spill cap
    /// (bytes). `None` → resolve from `ARNEB_SPILL_CHUNK_BYTES` env / the
    /// 512 MiB default at execute time. Bounding chunk size bounds the
    /// multi-pass reload working set (`execute_multipass` loads one chunk
    /// at a time). Set by tests to force small chunks deterministically.
    spill_chunk_bytes: Option<usize>,
    /// Same-fragment dynamic-filter targets for `left_key`: the LEFT (probe)
    /// child output-schema column indices join-equal to the left key (its
    /// equivalence class within the probe subtree), computed at plan time via
    /// `properties::equivalent_output_columns`. The build-side `IN (...)`
    /// filter is injected ONLY at these indices (by index descent), never by
    /// name — so a self-join twin sharing the key's name is not misrouted.
    /// Empty → no same-fragment injection.
    df_targets: Vec<usize>,
}

impl SemiJoinExec {
    pub(crate) fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_key: PlanExpr,
        right_key: PlanExpr,
        residual: Option<PlanExpr>,
        anti: bool,
        memory_pool: Arc<dyn MemoryPool>,
    ) -> Self {
        Self {
            left,
            right,
            left_key,
            right_key,
            residual,
            anti,
            build_left: false,
            peak_build_bytes: std::sync::atomic::AtomicUsize::new(0),
            memory_pool,
            dynamic_filter_producers: Vec::new(),
            dynamic_filter_publisher: None,
            dynamic_filtering_enabled: false,
            spill_chunk_bytes: None,
            df_targets: Vec::new(),
        }
    }

    /// Set the same-fragment dynamic-filter target indices for `left_key`
    /// (its probe-subtree equivalence class). Called by the physical planner.
    pub(crate) fn with_df_targets(mut self, targets: Vec<usize>) -> Self {
        self.df_targets = targets;
        self
    }

    pub(crate) fn with_build_left(mut self, build_left: bool) -> Self {
        self.build_left = build_left;
        self
    }

    /// Z (2026-06-05): override the per-chunk build-spill cap (bytes).
    /// Test-only — production resolves the cap from the
    /// `ARNEB_SPILL_CHUNK_BYTES` env var (or the default) at execute time.
    #[cfg(test)]
    pub(crate) fn with_spill_chunk_bytes(mut self, bytes: usize) -> Self {
        self.spill_chunk_bytes = Some(bytes);
        self
    }

    /// A1.5 (2026-05-27): attach cross-fragment DF plumbing. Called
    /// by the physical planner from
    /// `LogicalPlan::SemiJoin.dynamic_filter_ids`. With either an
    /// empty producer list, no publisher, or the flag off the
    /// operator behaves exactly as before.
    pub(crate) fn with_dynamic_filters(
        mut self,
        producers: Vec<arneb_planner::DynamicFilterProducer>,
        publisher: Option<crate::DynamicFilterPublisherRef>,
        enabled: bool,
    ) -> Self {
        self.dynamic_filter_producers = producers;
        self.dynamic_filter_publisher = publisher;
        self.dynamic_filtering_enabled = enabled;
        self
    }
}

impl fmt::Display for SemiJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.anti {
            write!(f, "AntiJoinExec")
        } else {
            write!(f, "SemiJoinExec")
        }
    }
}

/// Single-column typed key view. Lifts the data-type dispatch OUT of
/// the per-row hot loop so probing one row costs at most one branch +
/// one slice access. Mirrors `TypedCol` in `hash_join.rs` but only
/// covers the single-column key shape that SemiJoin uses.
enum TypedKey<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Utf8(&'a StringArray),
    Boolean(&'a BooleanArray),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Date32(&'a arrow::array::Date32Array),
    Decimal128(&'a Decimal128Array),
}

impl<'a> TypedKey<'a> {
    fn from_array(arr: &'a ArrayRef) -> Result<Self, ExecutionError> {
        Ok(match arr.data_type() {
            ArrowDataType::Int32 => TypedKey::Int32(arr.as_primitive::<Int32Type>()),
            ArrowDataType::Int64 => TypedKey::Int64(arr.as_primitive::<Int64Type>()),
            ArrowDataType::Utf8 => TypedKey::Utf8(arr.as_string::<i32>()),
            ArrowDataType::Boolean => TypedKey::Boolean(
                arr.as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Boolean type"),
            ),
            ArrowDataType::Float32 => TypedKey::Float32(arr.as_primitive::<Float32Type>()),
            ArrowDataType::Float64 => TypedKey::Float64(arr.as_primitive::<Float64Type>()),
            ArrowDataType::Date32 => TypedKey::Date32(arr.as_primitive::<Date32Type>()),
            ArrowDataType::Decimal128(_, _) => {
                TypedKey::Decimal128(arr.as_primitive::<Decimal128Type>())
            }
            dt => {
                return Err(ExecutionError::InvalidOperation(format!(
                    "unsupported semi-join key type: {dt:?}"
                )))
            }
        })
    }

    #[inline]
    fn is_null(&self, row: usize) -> bool {
        match self {
            TypedKey::Int32(a) => a.is_null(row),
            TypedKey::Int64(a) => a.is_null(row),
            TypedKey::Utf8(a) => a.is_null(row),
            TypedKey::Boolean(a) => a.is_null(row),
            TypedKey::Float32(a) => a.is_null(row),
            TypedKey::Float64(a) => a.is_null(row),
            TypedKey::Date32(a) => a.is_null(row),
            TypedKey::Decimal128(a) => a.is_null(row),
        }
    }

    #[inline]
    fn hash_row(&self, row: usize) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = FastHasher::default();
        match self {
            TypedKey::Int32(a) => i64::from(a.value(row)).hash(&mut hasher),
            TypedKey::Int64(a) => a.value(row).hash(&mut hasher),
            TypedKey::Utf8(a) => a.value(row).hash(&mut hasher),
            TypedKey::Boolean(a) => a.value(row).hash(&mut hasher),
            TypedKey::Float32(a) => a.value(row).to_bits().hash(&mut hasher),
            TypedKey::Float64(a) => a.value(row).to_bits().hash(&mut hasher),
            TypedKey::Date32(a) => a.value(row).hash(&mut hasher),
            TypedKey::Decimal128(a) => a.value(row).hash(&mut hasher),
        }
        hasher.finish()
    }
}

/// Z (2026-06-05): resolve the per-chunk build-spill cap in bytes.
/// Runtime-tunable via `ARNEB_SPILL_CHUNK_BYTES` (per the build-time vs
/// runtime config convention); defaults to 512 MiB. Bounding the chunk
/// size bounds the `execute_multipass` reload working set — the q21 SF30
/// fix where a single ~1.8 GB chunk was loaded back WHOLE, so two big
/// joins reloading concurrently overran the budget.
fn resolve_spill_chunk_cap_bytes() -> usize {
    const DEFAULT: usize = 512 * 1024 * 1024;
    std::env::var("ARNEB_SPILL_CHUNK_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT)
}

/// Flush the current in-memory build chunk to an Arrow IPC spill file and
/// release its reserved bytes. Shared by the budget-triggered and the
/// size-capped spill paths in the build loop (Tidy-First: extracted from
/// the inline `Err` arm so both callers stay identical).
fn spill_build_chunk(
    right_batches: &mut Vec<RecordBatch>,
    schema: &Arc<Schema>,
    anti: bool,
    build_reservation: &mut MemoryReservation,
    spilled_chunks: &mut Vec<SpillFile>,
) -> Result<(), ExecutionError> {
    let mut writer = SpillWriter::new(
        schema.clone(),
        if anti {
            "anti_join_build"
        } else {
            "semi_join_build"
        },
    )?;
    let spilled_bytes: usize = right_batches
        .iter()
        .map(crate::operator::record_batch_bytes)
        .sum();
    let spilled_rows: usize = right_batches.iter().map(|b| b.num_rows()).sum();
    for b in right_batches.iter() {
        writer.write(b)?;
    }
    let file = writer.finish()?;
    tracing::info!(
        target: "arneb::mem",
        operator = if anti { "AntiJoinExec" } else { "SemiJoinExec" },
        chunk_idx = spilled_chunks.len(),
        spilled_bytes,
        spilled_rows,
        n_batches = right_batches.len(),
        path = %file.path().display(),
        "build-side chunk spilled to disk",
    );
    spilled_chunks.push(file);
    right_batches.clear();
    build_reservation.shrink(spilled_bytes);
    Ok(())
}

fn arrow_schema_from_cols(cols: &[ColumnInfo]) -> Arc<Schema> {
    let fields: Vec<Field> = cols
        .iter()
        .map(|c| Field::new(&c.name, c.data_type.clone().into(), c.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

#[derive(Debug, Clone, Copy)]
struct CompactNeSpec {
    left_value_idx: usize,
    right_value_idx: usize,
}

impl CompactNeSpec {
    fn try_from(residual: &PlanExpr, left_width: usize) -> Option<Self> {
        use arneb_sql_parser::ast::BinaryOp as B;
        let PlanExpr::BinaryOp {
            left,
            op: B::NotEq,
            right,
            ..
        } = residual
        else {
            return None;
        };
        let (PlanExpr::Column { index: a, .. }, PlanExpr::Column { index: b, .. }) =
            (left.as_ref(), right.as_ref())
        else {
            return None;
        };
        match (*a < left_width, *b < left_width) {
            (true, false) => Some(Self {
                left_value_idx: *a,
                right_value_idx: *b - left_width,
            }),
            (false, true) => Some(Self {
                left_value_idx: *b,
                right_value_idx: *a - left_width,
            }),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct CompactNe<T: Default> {
    distinct: u8,
    v0: T,
}

impl<T: Default> Default for CompactNe<T> {
    fn default() -> Self {
        Self {
            distinct: 0,
            v0: T::default(),
        }
    }
}

impl<T: Default + PartialEq> CompactNe<T> {
    #[inline]
    fn insert(&mut self, value: T) {
        match self.distinct {
            0 => {
                self.v0 = value;
                self.distinct = 1;
            }
            1 if self.v0 != value => {
                self.distinct = 2;
            }
            _ => {}
        }
    }

    #[inline]
    fn matches(&self, left: &T) -> bool {
        self.distinct >= 2 || (self.distinct == 1 && self.v0 != *left)
    }
}

enum CompactNeMap {
    Boolean(FastHashMap<u64, CompactNe<bool>>),
    Int32(FastHashMap<u64, CompactNe<i32>>),
    Int64(FastHashMap<u64, CompactNe<i64>>),
    Float32(FastHashMap<u64, CompactNe<f32>>),
    Float64(FastHashMap<u64, CompactNe<f64>>),
    Date32(FastHashMap<u64, CompactNe<i32>>),
    Decimal128(FastHashMap<u64, CompactNe<i128>>),
    Utf8(FastHashMap<u64, CompactNe<String>>),
}

macro_rules! compact_insert_primitive {
    ($map:expr, $array:expr, $row:expr) => {{
        let entry = $map.entry($row.0).or_default();
        if !$array.is_null($row.1) {
            entry.insert($array.value($row.1));
        }
    }};
}

impl CompactNeMap {
    fn new(data_type: &ArrowDataType) -> Option<Self> {
        Some(match data_type {
            ArrowDataType::Boolean => Self::Boolean(FastHashMap::default()),
            ArrowDataType::Int32 => Self::Int32(FastHashMap::default()),
            ArrowDataType::Int64 => Self::Int64(FastHashMap::default()),
            ArrowDataType::Float32 => Self::Float32(FastHashMap::default()),
            ArrowDataType::Float64 => Self::Float64(FastHashMap::default()),
            ArrowDataType::Date32 => Self::Date32(FastHashMap::default()),
            ArrowDataType::Decimal128(_, _) => Self::Decimal128(FastHashMap::default()),
            ArrowDataType::Utf8 => Self::Utf8(FastHashMap::default()),
            _ => return None,
        })
    }

    fn insert(&mut self, hash: u64, values: &ArrayRef, row: usize) {
        match self {
            Self::Boolean(map) => compact_insert_primitive!(
                map,
                values.as_any().downcast_ref::<BooleanArray>().unwrap(),
                (hash, row)
            ),
            Self::Int32(map) => {
                compact_insert_primitive!(map, values.as_primitive::<Int32Type>(), (hash, row))
            }
            Self::Int64(map) => {
                compact_insert_primitive!(map, values.as_primitive::<Int64Type>(), (hash, row))
            }
            Self::Float32(map) => {
                compact_insert_primitive!(map, values.as_primitive::<Float32Type>(), (hash, row))
            }
            Self::Float64(map) => {
                compact_insert_primitive!(map, values.as_primitive::<Float64Type>(), (hash, row))
            }
            Self::Date32(map) => {
                compact_insert_primitive!(map, values.as_primitive::<Date32Type>(), (hash, row))
            }
            Self::Decimal128(map) => {
                compact_insert_primitive!(map, values.as_primitive::<Decimal128Type>(), (hash, row))
            }
            Self::Utf8(map) => {
                let values = values.as_string::<i32>();
                let entry = map.entry(hash).or_default();
                if !values.is_null(row) {
                    entry.insert(values.value(row).to_owned());
                }
            }
        }
    }

    fn matches(&self, hash: u64, values: &ArrayRef, row: usize) -> bool {
        macro_rules! primitive_matches {
            ($map:expr, $array:expr) => {{
                !$array.is_null(row)
                    && $map
                        .get(&hash)
                        .is_some_and(|entry| entry.matches(&$array.value(row)))
            }};
        }
        match self {
            Self::Boolean(map) => {
                primitive_matches!(map, values.as_any().downcast_ref::<BooleanArray>().unwrap())
            }
            Self::Int32(map) => primitive_matches!(map, values.as_primitive::<Int32Type>()),
            Self::Int64(map) => primitive_matches!(map, values.as_primitive::<Int64Type>()),
            Self::Float32(map) => primitive_matches!(map, values.as_primitive::<Float32Type>()),
            Self::Float64(map) => primitive_matches!(map, values.as_primitive::<Float64Type>()),
            Self::Date32(map) => primitive_matches!(map, values.as_primitive::<Date32Type>()),
            Self::Decimal128(map) => {
                primitive_matches!(map, values.as_primitive::<Decimal128Type>())
            }
            Self::Utf8(map) => {
                let values = values.as_string::<i32>();
                !values.is_null(row)
                    && map.get(&hash).is_some_and(|entry| {
                        entry.distinct >= 2
                            || (entry.distinct == 1 && entry.v0 != values.value(row))
                    })
            }
        }
    }

    fn estimated_bytes(&self) -> usize {
        macro_rules! fixed_bytes {
            ($map:expr, $value:ty) => {
                $map.capacity() * (size_of::<u64>() + size_of::<CompactNe<$value>>() + 16)
            };
        }
        match self {
            Self::Boolean(map) => fixed_bytes!(map, bool),
            Self::Int32(map) | Self::Date32(map) => fixed_bytes!(map, i32),
            Self::Int64(map) => fixed_bytes!(map, i64),
            Self::Float32(map) => fixed_bytes!(map, f32),
            Self::Float64(map) => fixed_bytes!(map, f64),
            Self::Decimal128(map) => fixed_bytes!(map, i128),
            Self::Utf8(map) => {
                fixed_bytes!(map, String) + map.values().map(|v| v.v0.capacity()).sum::<usize>()
            }
        }
    }
}

// `concat_batches_opt` was removed in F-Perf10 — the build now keeps
// `Vec<RecordBatch>` and addresses rows via `(batch_idx, row_idx)`.

#[async_trait]
impl ExecutionPlan for SemiJoinExec {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.left.schema()
    }

    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let output_schema = arrow_schema_from_cols(&self.left.schema());

        if self.build_left && semi_mark_join_enabled() {
            return self.execute_mark_join_build_left(output_schema).await;
        }

        if compact_semi_ne_enabled() && !self.build_left {
            if let Some(stream) = self.execute_compact_ne(output_schema.clone()).await? {
                return Ok(stream);
            }
        }

        // ---- Build phase ----
        //
        // Phase 2b (2026-05-21): chunked-with-spill build. Stream right
        // batches one at a time, reserve against the active memory pool,
        // and on `ResourceExhausted` flush the current in-memory chunk
        // to an Arrow IPC `SpillFile`. After EOF either:
        //   - `spilled_chunks.is_empty()`: single in-memory chunk →
        //     existing fast path (PartitionedSet/Index, parallel probe,
        //     dynamic filter emission).
        //   - `spilled_chunks` non-empty: multi-pass probe path. Each
        //     chunk's PartitionedSet/Index is built/dropped in turn;
        //     the left side is collected once and scanned per chunk
        //     with a matched-row mask. (Residual support deferred to a
        //     follow-up commit — see `execute_multipass`.)
        let mut right_stream = self.right.execute(0).await?;
        let consumer = MemoryConsumer::new(if self.anti {
            "AntiJoinExec.build"
        } else {
            "SemiJoinExec.build"
        })
        .with_can_spill(true);
        let mut build_reservation = consumer.register(self.memory_pool.clone());

        let mut right_batches: Vec<RecordBatch> = Vec::new();
        let mut spilled_chunks: Vec<SpillFile> = Vec::new();
        let mut right_schema: Option<Arc<Schema>> = None;
        let mut total_right_rows: usize = 0;
        let mut total_build_bytes: usize = 0;
        // Z (2026-06-05): bytes currently resident in `right_batches`
        // (resets to 0 on each spill) + its high-water mark. `chunk_cap`
        // bounds a single in-memory chunk so the multi-pass reload holds
        // at most ~one cap-sized chunk, not the whole build.
        let mut in_mem_bytes: usize = 0;
        let mut peak_in_mem_bytes: usize = 0;
        let chunk_cap = self
            .spill_chunk_bytes
            .unwrap_or_else(resolve_spill_chunk_cap_bytes);

        while let Some(batch_res) = right_stream.next().await {
            let batch = batch_res.map_err(|e| {
                ExecutionError::InvalidOperation(format!("failed to collect right input: {e}"))
            })?;
            if right_schema.is_none() {
                right_schema = Some(batch.schema());
            }
            let n_rows = batch.num_rows();
            if n_rows == 0 {
                continue;
            }
            total_right_rows += n_rows;
            let batch_bytes = crate::operator::record_batch_bytes(&batch);
            total_build_bytes += batch_bytes;

            match build_reservation.try_grow(batch_bytes) {
                Ok(()) => {
                    right_batches.push(batch);
                    in_mem_bytes += batch_bytes;
                }
                Err(_) => {
                    // Budget-triggered spill: flush the current in-memory
                    // chunk and retry the rejected batch.
                    if right_batches.is_empty() {
                        return Err(ExecutionError::ResourceExhausted(format!(
                            "single right batch ({} bytes) exceeds memory budget for {}.build",
                            batch_bytes,
                            if self.anti {
                                "AntiJoinExec"
                            } else {
                                "SemiJoinExec"
                            }
                        )));
                    }
                    let schema = right_schema.clone().expect("schema set above");
                    spill_build_chunk(
                        &mut right_batches,
                        &schema,
                        self.anti,
                        &mut build_reservation,
                        &mut spilled_chunks,
                    )?;
                    in_mem_bytes = 0;
                    // Retry the rejected batch now that the budget is freed.
                    build_reservation.try_grow(batch_bytes)?;
                    right_batches.push(batch);
                    in_mem_bytes += batch_bytes;
                }
            }

            peak_in_mem_bytes = peak_in_mem_bytes.max(in_mem_bytes);

            // Z: size-capped spill — proactively flush once the resident
            // chunk reaches `chunk_cap`, independent of the pool budget,
            // so each spilled chunk (and thus the multi-pass reload) stays
            // bounded even when the pool is large or shared across joins.
            if in_mem_bytes >= chunk_cap {
                let schema = right_schema.clone().expect("schema set above");
                spill_build_chunk(
                    &mut right_batches,
                    &schema,
                    self.anti,
                    &mut build_reservation,
                    &mut spilled_chunks,
                )?;
                in_mem_bytes = 0;
            }
        }

        let has_spill = !spilled_chunks.is_empty();
        // Peak RESIDENT build bytes (high-water of the in-memory chunk),
        // not total processed — when the build spills, the materialised
        // state never holds the whole right side at once.
        self.peak_build_bytes
            .store(peak_in_mem_bytes, std::sync::atomic::Ordering::Relaxed);

        tracing::info!(
            target: "arneb::mem",
            operator = if self.anti { "AntiJoinExec" } else { "SemiJoinExec" },
            right_rows = total_right_rows,
            n_batches = right_batches.len(),
            has_residual = self.residual.is_some(),
            build_bytes = total_build_bytes,
            pool_reserved = self.memory_pool.reserved(),
            spilled_chunks = spilled_chunks.len(),
            "semi/anti join build complete",
        );

        // Multi-pass branch: at least one chunk was spilled to disk.
        // Hand off to the spill-aware probe path. Phase 2b.2 (2026-05-21)
        // wired residual + spill — both fast-path and residual queries
        // now spill correctly via `execute_multipass`.
        if has_spill {
            return self
                .execute_multipass(
                    spilled_chunks,
                    right_batches,
                    build_reservation,
                    output_schema,
                )
                .await;
        }

        // Single-pass path (no spill). Reconstruct the structures the
        // existing code below expects.
        let (right_set, right_index): (Option<AccountedSet>, Option<AccountedBuildIndex>) =
            if right_batches.is_empty() {
                (None, None)
            } else if self.residual.is_some() {
                let idx = build_build_index(
                    &self.right_key,
                    &right_batches,
                    index_alloc_label(self.anti),
                )
                .await?;
                (None, Some(idx))
            } else {
                let set = build_partitioned_set(
                    &self.right_key,
                    &right_batches,
                    set_alloc_label(self.anti),
                )
                .await?;
                (Some(set), None)
            };
        // Keep `build_reservation` alive for the remainder of `execute` —
        // its Drop releases the bytes back to the pool. The variable
        // appears unused after this point; the binding ensures the
        // reservation isn't dropped before probe completes.
        let _retain_reservation = &build_reservation;

        // ---- Dynamic filter emission (SemiJoin only, not AntiJoin) ----
        // SemiJoin = IN-semantics: rows survive only if left_key is in
        // the right-side distinct set. Emitting `left_key IN (right
        // distinct values)` as a runtime InList lets the left scan
        // skip rows / row-groups that have no match. This mirrors
        // Trino's `PredicatePushDown.visitFilteringSemiJoin` +
        // `LocalExecutionPlanner.visitSemiJoin` (PR #5017) which place
        // a `DynamicFilterSourceOperator` above the filtering-side build.
        //
        // ANTI-semantics is the OPPOSITE: rows survive only if left_key
        // is NOT in the right set. An IN-list DF here would prune the
        // surviving rows — Trino confirms `SemiJoinNode` has no
        // is-negated field and `PredicatePushDown` has zero
        // `JoinType.ANTI` references, so anti never emits DF. We
        // mirror that with the `!self.anti` guard.
        //
        // Dual-name pattern (same as HashJoinExec): also inject under
        // the right-key column name when the left subtree carries it.
        // Sound because equi-join `left_key = right_key` makes the
        // values equivalent on matched rows.
        // Per-type row-count gate: skip the expensive concat + distinct
        // scan when `total_right_rows` clearly exceeds the cap for the
        // right-key column type. For Q21's 6M-row Int64 right side the
        // unguarded path paid ~60 ms of wasted concat per probe before
        // `distinct_scalar_values` bailed at cap=1M. Allow a 1.5× margin
        // for duplicate-key collapsing (real distinct count usually
        // smaller than row count).
        let right_key_type = if let PlanExpr::Column { index, .. } = &self.right_key {
            self.right
                .schema()
                .get(*index)
                .map(|c| arrow::datatypes::DataType::from(c.data_type.clone()))
        } else {
            None
        };
        let df_eligible_by_rows = right_key_type
            .as_ref()
            .and_then(crate::hash_join::dynamic_filter_cap)
            .is_some_and(|cap| total_right_rows <= cap.saturating_mul(3) / 2);
        if !self.anti && !right_batches.is_empty() && df_eligible_by_rows {
            if let (PlanExpr::Column { .. }, PlanExpr::Column { .. }) =
                (&self.left_key, &self.right_key)
            {
                let arrays: Result<Vec<_>, _> = right_batches
                    .iter()
                    .map(|b| expression::evaluate(&self.right_key, b, None))
                    .collect();
                if let Ok(arrays) = arrays {
                    let refs: Vec<&dyn arrow::array::Array> =
                        arrays.iter().map(|a| a.as_ref()).collect();
                    if let Ok(concat) = arrow::compute::concat(&refs) {
                        if let Some(cap) = crate::hash_join::dynamic_filter_cap(concat.data_type())
                        {
                            if let Some(values) =
                                crate::hash_join::distinct_scalar_values(&concat, cap)
                            {
                                if !values.is_empty() {
                                    let literals: Vec<PlanExpr> = values
                                        .into_iter()
                                        .map(|v| PlanExpr::Literal {
                                            value: v,
                                            span: None,
                                        })
                                        .collect();
                                    // Provenance-targeted injection: push the
                                    // build-key `IN (...)` at every probe-side
                                    // column join-equal to `left_key`
                                    // (`df_targets`), by index descent, never
                                    // by name. Replaces the prior direct-key +
                                    // right-key "dual" name injections.
                                    let left_schema = self.left.schema();
                                    for &target_idx in &self.df_targets {
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
                        }
                    }
                }
            }
        }

        // ---- Cross-fragment dynamic filter emit (A1.5, 2026-05-27) ----
        // SemiJoin only, not AntiJoin (same semantic reasoning as the
        // same-fragment InList path above). Emit one partition-level
        // Domain per declared producer; the publisher ships to the
        // coord's `DynamicFilterService`, which union-merges across
        // worker partitions and pushes the resolved Domain to probe-
        // side scans via `notify_dynamic_filter`.
        if self.dynamic_filtering_enabled && !self.anti && !self.dynamic_filter_producers.is_empty()
        {
            if let Some(publisher) = &self.dynamic_filter_publisher {
                let mut domains: Vec<(arneb_common::DynamicFilterId, arneb_common::Domain)> =
                    Vec::with_capacity(self.dynamic_filter_producers.len());
                for producer in &self.dynamic_filter_producers {
                    let domain = if right_batches.is_empty() {
                        // No rows at all → empty distinct set. Coord
                        // union-merges to the actual distinct set
                        // produced by other partitions (or stays empty
                        // if every partition was empty).
                        arneb_common::Domain::DistinctValues(Vec::new())
                    } else {
                        let col_arrays: Vec<arrow::array::ArrayRef> = right_batches
                            .iter()
                            .map(|b| b.column(producer.build_index).clone())
                            .collect();
                        let refs: Vec<&dyn arrow::array::Array> =
                            col_arrays.iter().map(|a| a.as_ref()).collect();
                        match arrow::compute::concat(&refs) {
                            Ok(concat) => {
                                crate::dynamic_filter_publisher::build_partition_domain_for_column(
                                    &concat,
                                )
                            }
                            // Concat failure (mismatched types — should
                            // not happen for valid plans): fall back to
                            // no-filter `All` to preserve correctness.
                            Err(_) => arneb_common::Domain::All,
                        }
                    };
                    domains.push((producer.id, domain));
                }
                let publisher = publisher.clone();
                tokio::spawn(async move {
                    for (id, domain) in domains {
                        publisher.publish(id, domain).await;
                    }
                });
            }
        }

        if stream_semi_probe_enabled() {
            return self
                .execute_singlepass_streaming(
                    right_batches,
                    right_set,
                    right_index,
                    build_reservation,
                    output_schema,
                )
                .await;
        }

        // ---- Probe phase ----
        let left_stream = self.left.execute(0).await?;
        // D3 (exec-memory-accounting, 2026-06-04): pool-track the probe-side
        // collect. SF30 q21 OOM-killed worker-2 HERE — the AntiJoin held a
        // 113M-row / ~7 GB left side the pool never saw, so spill never fired
        // (pool_reserved 3.9 GB < 5 GB budget) and the kernel killed the
        // container at the 11 GB cap. Tracking it makes the pool honest: it now
        // fails fast with a clean ResourceExhausted (TrackConsumersPool names
        // the hog) instead of an OOM-kill. At SF1/single-node the pool is
        // Unbounded → try_grow never fails → unchanged. `_left_reservation` is
        // held to the end of `execute` so the bytes stay accounted while the
        // batches are probed.
        let (left_batches, _left_reservation) = crate::operator::collect_stream_pool_tracked(
            left_stream,
            self.memory_pool.clone(),
            "SemiJoinExec.probe_left",
        )
        .await?;

        // Trino-style column pruning for the residual path: the
        // residual references only a small subset of the joined
        // (left ++ right) schema (e.g. TPC-H Q21's
        // `l2.l_suppkey <> l1.l_suppkey` touches 2 of 26+ columns).
        // We pre-project EACH right batch to only the residual-
        // referenced columns, then rewrite the residual's column
        // indices to the projected layout. Combined with no-concat
        // build, the original full-column batches can be dropped
        // entirely after this pruning step.
        let (pruned_residual, pruned_right_batches, joined_schema) =
            if let Some(residual) = self.residual.as_ref() {
                let left_width = self.left.schema().len();
                let mut needed_right_indices: Vec<usize> = Vec::new();
                let mut seen = std::collections::HashSet::new();
                collect_column_indices(residual, &mut |idx| {
                    if idx >= left_width {
                        let r = idx - left_width;
                        if seen.insert(r) {
                            needed_right_indices.push(r);
                        }
                    }
                });
                needed_right_indices.sort();

                // Project each batch to the needed right columns.
                let pruned: Result<Vec<RecordBatch>, ExecutionError> = right_batches
                    .iter()
                    .map(|b| project_record_batch(b, &needed_right_indices))
                    .collect();
                let pruned = pruned?;

                // Build remap: original_joined_index → new_joined_index.
                let mut remap: std::collections::HashMap<usize, usize> =
                    std::collections::HashMap::new();
                for (new_pos, &orig_right_idx) in needed_right_indices.iter().enumerate() {
                    remap.insert(left_width + orig_right_idx, left_width + new_pos);
                }
                let pruned_resid = remap_column_indices(residual.clone(), &remap);

                // Build the joined schema once (reused across all probe
                // batches). Left cols then projected-right cols.
                let mut fields: Vec<Field> =
                    Vec::with_capacity(left_width + needed_right_indices.len());
                for c in self.left.schema().iter() {
                    fields.push(Field::new(&c.name, c.data_type.clone().into(), c.nullable));
                }
                if let Some(first) = pruned.first() {
                    for f in first.schema().fields().iter() {
                        fields.push(f.as_ref().clone());
                    }
                } else {
                    // Empty right side: synth fields from the right schema
                    // so the joined schema is still well-defined.
                    let right_schema = self.right.schema();
                    for &i in &needed_right_indices {
                        let c = &right_schema[i];
                        fields.push(Field::new(&c.name, c.data_type.clone().into(), c.nullable));
                    }
                }
                let schema = Arc::new(Schema::new(fields));
                (Some(pruned_resid), Some(pruned), Some(schema))
            } else {
                (None, None, None)
            };

        // Drop the full unpruned right batches now that:
        //   1. The hash table (right_set / right_index) holds only
        //      u64 hashes + addresses — no Arc refs to columns.
        //   2. The residual path uses `pruned_right_batches`, each of
        //      which holds only the residual-referenced columns.
        // For TPC-H Q21 this releases ~770MB (14 of 16 lineitem
        // columns) per SemiJoin and avoids the doubled-allocation peak
        // that `concat_batches` used to cause during the build.
        drop(right_batches);
        tracing::info!(
            target: "arneb::mem",
            operator = if self.anti { "AntiJoinExec" } else { "SemiJoinExec" },
            "dropped unpruned right batches after build + residual projection",
        );

        // Parallelise the probe across left batches. Each batch is
        // independent given the (now-immutable) right side, so we hand
        // them out to a pool of `tokio::spawn_blocking` tasks. Cap the
        // concurrency at the host's logical-core count so we don't
        // oversubscribe — `num_cpus::get()` is cached cheap.
        //
        // Adaptive parallelism: spawn_blocking has fixed overhead (~10–
        // 50µs per task) that dominates for queries with few small left
        // batches (Q17, Q18). Use one worker per ~4 batches, capped at
        // the host's logical cores, and clamp to 1 when residual is
        // absent and batch count is tiny — the fast path is so cheap
        // there's nothing to gain.
        let right_set = right_set.map(Arc::new);
        let right_index = right_index.map(Arc::new);
        let right_for_residual = pruned_right_batches.map(Arc::new);
        let left_key = self.left_key.clone();
        let residual_opt = pruned_residual;
        let output_schema_clone = output_schema.clone();
        let anti = self.anti;

        let n_batches = left_batches.len();
        let max_workers = num_cpus::get().max(1);
        let parallelism = if residual_opt.is_some() {
            // Residual path is per-pair O(N×M); even a few batches
            // benefit from spreading work across cores. Heuristic:
            // one worker per ~2 batches, capped at max_workers.
            n_batches.div_ceil(2).clamp(1, max_workers)
        } else {
            // Fast (hash-set) path is so cheap per row that
            // parallelism only helps for big batch counts.
            if n_batches >= max_workers * 4 {
                max_workers
            } else if n_batches >= 8 {
                (n_batches / 4).clamp(1, max_workers)
            } else {
                1
            }
        };
        let mut handles: Vec<tokio::task::JoinHandle<Result<Vec<RecordBatch>, ExecutionError>>> =
            Vec::with_capacity(parallelism);
        // Distribute batches round-robin across workers so each worker
        // sees a roughly even mix of small and large batches.
        let mut chunks: Vec<Vec<RecordBatch>> = (0..parallelism).map(|_| Vec::new()).collect();
        for (i, b) in left_batches.into_iter().enumerate() {
            chunks[i % parallelism].push(b);
        }
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let right_set = right_set.clone();
            let right_index = right_index.clone();
            let right_for_residual = right_for_residual.clone();
            let joined_schema = joined_schema.clone();
            let left_key = left_key.clone();
            let residual_opt = residual_opt.clone();
            let output_schema_clone = output_schema_clone.clone();
            handles.push(tokio::task::spawn_blocking(move || {
                let mut out: Vec<RecordBatch> = Vec::with_capacity(chunk.len());
                for left_batch in chunk {
                    let n_rows = left_batch.num_rows();
                    if n_rows == 0 {
                        continue;
                    }
                    let left_keys_arr = expression::evaluate(&left_key, &left_batch, None)?;
                    let left_typed = TypedKey::from_array(&left_keys_arr)?;

                    let keep_idx =
                        if let (Some(idx_map), Some(right_batches_pruned), Some(residual)) = (
                            right_index.as_ref(),
                            right_for_residual.as_ref(),
                            residual_opt.as_ref(),
                        ) {
                            probe_batch_residual(
                                &left_typed,
                                n_rows,
                                idx_map.as_ref(),
                                &left_batch,
                                right_batches_pruned.as_ref(),
                                residual,
                                joined_schema.as_ref().unwrap(),
                                anti,
                            )?
                        } else if let Some(set) = right_set.as_ref() {
                            probe_batch_set(&left_typed, n_rows, set.as_ref(), anti)
                        } else if anti {
                            (0..n_rows as u32).collect()
                        } else {
                            Vec::new()
                        };

                    if keep_idx.is_empty() {
                        continue;
                    }
                    let idx_array = UInt32Array::from(keep_idx);
                    let mut cols: Vec<ArrayRef> = Vec::with_capacity(left_batch.num_columns());
                    for c in 0..left_batch.num_columns() {
                        cols.push(
                            compute::take(left_batch.column(c), &idx_array, None).map_err(|e| {
                                ExecutionError::InvalidOperation(format!("take failed: {e}"))
                            })?,
                        );
                    }
                    out.push(RecordBatch::try_new(output_schema_clone.clone(), cols)?);
                }
                Ok::<_, ExecutionError>(out)
            }));
        }

        let mut result_batches: Vec<RecordBatch> = Vec::new();
        for handle in handles {
            let chunk_out = handle
                .await
                .map_err(|e| ExecutionError::InvalidOperation(format!("semi-join task: {e}")))??;
            result_batches.extend(chunk_out);
        }

        Ok(stream_from_batches(output_schema, result_batches))
    }

    fn display_name(&self) -> &str {
        if self.anti {
            "AntiJoinExec"
        } else {
            "SemiJoinExec"
        }
    }

    fn peak_bytes_reserved(&self) -> usize {
        self.peak_build_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl SemiJoinExec {
    /// Compact exact path for a lone cross-side `<>` residual. Returning
    /// `None` means the caller must run the unchanged full-row path.
    async fn execute_compact_ne(
        &self,
        output_schema: Arc<Schema>,
    ) -> Result<Option<SendableRecordBatchStream>, ExecutionError> {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let Some(spec) = self
            .residual
            .as_ref()
            .and_then(|r| CompactNeSpec::try_from(r, left_schema.len()))
        else {
            return Ok(None);
        };
        let (Some(left_col), Some(right_col)) = (
            left_schema.get(spec.left_value_idx),
            right_schema.get(spec.right_value_idx),
        ) else {
            return Ok(None);
        };
        let left_type: ArrowDataType = left_col.data_type.clone().into();
        let right_type: ArrowDataType = right_col.data_type.clone().into();
        if left_type != right_type {
            return Ok(None);
        }
        let Some(mut compact) = CompactNeMap::new(&right_type) else {
            return Ok(None);
        };

        let consumer = MemoryConsumer::new(if self.anti {
            "AntiJoinExec.compact_ne"
        } else {
            "SemiJoinExec.compact_ne"
        });
        let mut reservation = consumer.register(self.memory_pool.clone());
        let mut right_stream = self.right.execute(0).await?;
        let mut right_rows = 0usize;
        while let Some(batch) = right_stream.next().await {
            let batch = batch.map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "failed to collect compact right input: {e}"
                ))
            })?;
            let keys = expression::evaluate(&self.right_key, &batch, None)?;
            let typed_keys = TypedKey::from_array(&keys)?;
            let values = batch.column(spec.right_value_idx);
            right_rows += batch.num_rows();
            for row in 0..batch.num_rows() {
                if !typed_keys.is_null(row) {
                    compact.insert(typed_keys.hash_row(row), values, row);
                }
            }
            let estimated = compact.estimated_bytes();
            if reservation.try_resize(estimated).is_err() {
                tracing::info!(
                    target: "arneb::mem",
                    operator = if self.anti { "AntiJoinExec" } else { "SemiJoinExec" },
                    estimated_bytes = estimated,
                    "compact <> build exceeded its memory budget; falling back",
                );
                return Ok(None);
            }
        }

        let compact_bytes = compact.estimated_bytes();
        self.peak_build_bytes
            .store(compact_bytes, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(
            target: "arneb::mem",
            operator = if self.anti { "AntiJoinExec" } else { "SemiJoinExec" },
            right_rows,
            compact_bytes,
            "using compact single-column <> build",
        );

        // The compact representation intentionally does not retain build-key
        // values. Resolve declared cross-fragment filters to the sound no-op
        // domain so probe scans do not wait for a producer that took this path.
        if self.dynamic_filtering_enabled && !self.anti {
            if let Some(publisher) = &self.dynamic_filter_publisher {
                let publisher = publisher.clone();
                let ids: Vec<_> = self
                    .dynamic_filter_producers
                    .iter()
                    .map(|producer| producer.id)
                    .collect();
                tokio::spawn(async move {
                    for id in ids {
                        publisher.publish(id, arneb_common::Domain::All).await;
                    }
                });
            }
        }

        let left_stream = self.left.execute(0).await?;
        let left_key = self.left_key.clone();
        let anti = self.anti;
        let stream_schema = output_schema.clone();
        // Keep the compact map and its reservation in the unfold state. Probe
        // batches are transformed and yielded one at a time, so neither side
        // is materialized or spilled.
        let inner = futures::stream::unfold(
            (left_stream, compact, reservation),
            move |(mut input, compact, reservation)| {
                let left_key = left_key.clone();
                let output_schema = stream_schema.clone();
                async move {
                    loop {
                        let item = input.next().await?;
                        let result = (|| -> Result<Option<RecordBatch>, ExecutionError> {
                            let batch = item.map_err(|e| {
                                ExecutionError::InvalidOperation(format!(
                                    "failed to read compact left input: {e}"
                                ))
                            })?;
                            if batch.num_rows() == 0 {
                                return Ok(None);
                            }
                            let keys = expression::evaluate(&left_key, &batch, None)?;
                            let typed_keys = TypedKey::from_array(&keys)?;
                            let values = batch.column(spec.left_value_idx);
                            let mut keep = Vec::new();
                            for row in 0..batch.num_rows() {
                                let matched = !typed_keys.is_null(row)
                                    && compact.matches(typed_keys.hash_row(row), values, row);
                                if matched != anti {
                                    keep.push(row as u32);
                                }
                            }
                            if keep.is_empty() {
                                return Ok(None);
                            }
                            let indices = UInt32Array::from(keep);
                            let columns = batch
                                .columns()
                                .iter()
                                .map(|column| compute::take(column, &indices, None))
                                .collect::<Result<Vec<_>, _>>()?;
                            Ok(Some(RecordBatch::try_new(output_schema.clone(), columns)?))
                        })();
                        match result {
                            Ok(Some(batch)) => {
                                return Some((Ok(batch), (input, compact, reservation)));
                            }
                            Ok(None) => continue,
                            Err(error) => {
                                return Some((Err(error), (input, compact, reservation)));
                            }
                        }
                    }
                }
            },
        );
        Ok(Some(Box::pin(SemiAsyncBatchStream {
            schema: output_schema,
            inner: Box::pin(inner),
        })))
    }

    async fn execute_mark_join_build_left(
        &self,
        output_schema: Arc<Schema>,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let left_stream = self.left.execute(0).await?;
        let (left_batches, left_reservation) = crate::operator::collect_stream_pool_tracked(
            left_stream,
            self.memory_pool.clone(),
            "SemiJoinExec.mark_build_left",
        )
        .await?;
        let total_left_bytes: usize = left_batches
            .iter()
            .map(crate::operator::record_batch_bytes)
            .sum();
        self.peak_build_bytes
            .store(total_left_bytes, std::sync::atomic::Ordering::Relaxed);

        if left_batches.is_empty() {
            drop(left_reservation);
            return Ok(stream_from_batches(output_schema, Vec::new()));
        }

        let left_index =
            build_build_index(&self.left_key, &left_batches, index_alloc_label(self.anti)).await?;
        let mut matched: Vec<Vec<bool>> = left_batches
            .iter()
            .map(|batch| vec![false; batch.num_rows()])
            .collect();
        let joined_schema = if self.residual.is_some() {
            Some(joined_schema_from_cols(
                &self.left.schema(),
                &self.right.schema(),
            ))
        } else {
            None
        };

        let mut right_stream = self.right.execute(0).await?;
        while let Some(batch_res) = right_stream.next().await {
            let right_batch = batch_res.map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "failed to read right input (mark semi join): {e}"
                ))
            })?;
            if right_batch.num_rows() == 0 {
                continue;
            }
            mark_left_matches_for_right_batch(
                &right_batch,
                &self.right_key,
                &left_index,
                &left_batches,
                self.residual.as_ref(),
                joined_schema.as_ref(),
                &mut matched,
            )?;
        }

        let mut out_batches = Vec::new();
        for (batch_idx, left_batch) in left_batches.iter().enumerate() {
            let keep_idx: Vec<u32> = matched[batch_idx]
                .iter()
                .enumerate()
                .filter_map(|(row, is_match)| {
                    let keep = if self.anti { !*is_match } else { *is_match };
                    keep.then_some(row as u32)
                })
                .collect();
            if keep_idx.is_empty() {
                continue;
            }
            let idx_array = UInt32Array::from(keep_idx);
            let mut cols: Vec<ArrayRef> = Vec::with_capacity(left_batch.num_columns());
            for c in 0..left_batch.num_columns() {
                cols.push(
                    compute::take(left_batch.column(c), &idx_array, None).map_err(|e| {
                        ExecutionError::InvalidOperation(format!("take failed: {e}"))
                    })?,
                );
            }
            out_batches.push(RecordBatch::try_new(output_schema.clone(), cols)?);
        }

        drop(left_reservation);
        Ok(stream_from_batches(output_schema, out_batches))
    }

    async fn execute_singlepass_streaming(
        &self,
        right_batches: Vec<RecordBatch>,
        right_set: Option<AccountedSet>,
        right_index: Option<AccountedBuildIndex>,
        build_reservation: MemoryReservation,
        output_schema: Arc<Schema>,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let (pruned_residual, pruned_right_batches, joined_schema) = prepare_residual_probe(
            self.residual.as_ref(),
            &self.left.schema(),
            &self.right.schema(),
            &right_batches,
        )?;

        drop(right_batches);
        tracing::info!(
            target: "arneb::mem",
            operator = if self.anti { "AntiJoinExec" } else { "SemiJoinExec" },
            "dropped unpruned right batches after build + residual projection",
        );

        let left_stream = self.left.execute(0).await?;
        let right_set = right_set.map(Arc::new);
        let right_index = right_index.map(Arc::new);
        let right_for_residual = pruned_right_batches.map(Arc::new);
        let left_key = self.left_key.clone();
        let residual_opt = pruned_residual;
        let output_schema_clone = output_schema.clone();
        let anti = self.anti;
        let max_workers = num_cpus::get().max(1);
        let parallelism = if residual_opt.is_some() {
            max_workers
        } else {
            (max_workers / 4).clamp(1, max_workers)
        };

        let inner = async_stream::try_stream! {
            let _build_reservation = build_reservation;
            let tasks = left_stream
                .map(move |batch_res| {
                    let right_set = right_set.clone();
                    let right_index = right_index.clone();
                    let right_for_residual = right_for_residual.clone();
                    let joined_schema = joined_schema.clone();
                    let left_key = left_key.clone();
                    let residual_opt = residual_opt.clone();
                    let output_schema_clone = output_schema_clone.clone();
                    async move {
                        let left_batch = batch_res.map_err(|e| {
                            ExecutionError::InvalidOperation(format!(
                                "failed to read left input (streaming semi probe): {e}"
                            ))
                        })?;
                        tokio::task::spawn_blocking(move || {
                            probe_one_left_batch(
                                left_batch,
                                &left_key,
                                right_set.as_deref(),
                                right_index.as_deref(),
                                right_for_residual.as_deref().map(Vec::as_slice),
                                residual_opt.as_ref(),
                                joined_schema.as_ref(),
                                output_schema_clone,
                                anti,
                            )
                        })
                        .await
                        .map_err(|e| ExecutionError::InvalidOperation(format!("semi-join task: {e}")))?
                    }
                })
                .buffered(parallelism);
            futures::pin_mut!(tasks);
            while let Some(batch_res) = tasks.next().await {
                if let Some(batch) = batch_res? {
                    yield batch;
                }
            }
        };

        Ok(Box::pin(SemiAsyncBatchStream {
            schema: output_schema,
            inner: Box::pin(inner),
        }))
    }

    /// Symmetric grace-partition spill path (ARNEB_PARTITIONED_SEMI_SPILL, 2026-06-18).
    ///
    /// Both the build (right) side and the probe (left) side are hash-partitioned
    /// into N buckets using the equi-join key with a deterministic ahash seed.
    /// For each bucket p, only probe-bucket-p is joined against build-bucket-p
    /// (correctness: equal join keys always land in the same bucket). Each side
    /// is read from disk EXACTLY ONCE — O(build + probe) total I/O — vs the
    /// existing multi-pass O(chunks × probe) path.
    ///
    /// NULL key handling: NULL keys cannot be bucketed by hash; they are routed
    /// to a separate "null" pile. For SEMI, NULL-key probe rows are silently
    /// dropped (a NULL key can never match). For ANTI, NULL-key probe rows are
    /// unconditionally emitted (they can never match any build row). This
    /// preserves the existing NULL semantics exactly.
    ///
    /// N is chosen as `ceil(total_build_bytes / per_bucket_budget)`, clamped to
    /// [2, 64]. `per_bucket_budget` defaults to 256 MiB so each build bucket is
    /// expected to fit comfortably in the memory pool when loaded.
    async fn execute_partitioned_spill(
        &self,
        spilled_chunks: Vec<SpillFile>,
        last_in_mem: Vec<RecordBatch>,
        build_reservation: MemoryReservation,
        output_schema: Arc<Schema>,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        // ── Compute total build size to choose N ──────────────────────────
        let total_build_bytes: usize = {
            let chunk_sizes: usize = spilled_chunks.iter().map(|f| f.bytes_written()).sum();
            let in_mem_bytes: usize = last_in_mem
                .iter()
                .map(crate::operator::record_batch_bytes)
                .sum();
            chunk_sizes + in_mem_bytes
        };
        // Release the build reservation; we'll reload bucket-by-bucket below.
        drop(build_reservation);

        // Target ~256 MiB per build bucket so it fits in memory comfortably.
        const PER_BUCKET_TARGET: usize = 256 * 1024 * 1024;
        let n_partitions: usize = {
            let raw = total_build_bytes.div_ceil(PER_BUCKET_TARGET.max(1));
            raw.clamp(2, 64)
        };

        let op_name = if self.anti {
            "AntiJoinExec"
        } else {
            "SemiJoinExec"
        };
        tracing::info!(
            target: "arneb::mem",
            operator = op_name,
            total_build_bytes,
            n_partitions,
            "partitioned-semi-spill: starting symmetric grace partition",
        );

        // ── Build partitioner (deterministic ahash seed 0,0,0,0) ─────────
        // Both sides use the equi-join key so equal keys map to the same bucket.
        let right_partitioner = Arc::new(
            HashPartitioner::new(vec![self.right_key.clone()], n_partitions).map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "{op_name}: failed to build right partitioner: {e}"
                ))
            })?,
        );
        let left_partitioner = Arc::new(
            HashPartitioner::new(vec![self.left_key.clone()], n_partitions).map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "{op_name}: failed to build left partitioner: {e}"
                ))
            })?,
        );

        // ── Partition BUILD (right) into N spill buckets ──────────────────
        // Re-stream all spilled chunks + in-mem batches through the partitioner.
        let right_schema = {
            // Infer from in-mem batches or first spill chunk.
            if let Some(b) = last_in_mem.first() {
                b.schema()
            } else if let Some(f) = spilled_chunks.first() {
                let mut rd = f.open_reader()?;
                match rd.next() {
                    Some(Ok(b)) => b.schema(),
                    Some(Err(e)) => {
                        return Err(ExecutionError::InvalidOperation(format!(
                            "{op_name}: failed reading first build batch for schema: {e}"
                        )))
                    }
                    None => return Ok(stream_from_batches(output_schema, vec![])),
                }
            } else {
                // Both empty → no build rows → antijoin emits all left, semi emits none.
                let left_stream = self.left.execute(0).await?;
                let left_batches: Vec<RecordBatch> = {
                    use futures::StreamExt;
                    let mut s = left_stream;
                    let mut v = Vec::new();
                    while let Some(r) = s.next().await {
                        v.push(r.map_err(|e| {
                            ExecutionError::InvalidOperation(format!(
                                "{op_name}: left collect (empty build): {e}"
                            ))
                        })?);
                    }
                    v
                };
                if !self.anti {
                    return Ok(stream_from_batches(output_schema, vec![]));
                }
                // Anti + empty build → emit all left rows unchanged.
                return Ok(stream_from_batches(output_schema, left_batches));
            }
        };

        // Build the right PartitionedSpillWriter.
        let mut build_pwriter =
            PartitionedSpillWriter::new(right_schema.clone(), n_partitions, "semi_grace_build");

        // Helper: partition one batch of build rows into the writer.
        let partition_build_batch = |pwriter: &mut PartitionedSpillWriter,
                                     batch: &RecordBatch,
                                     partitioner: &HashPartitioner|
         -> Result<(), ExecutionError> {
            let assignments = partitioner.assignments(batch)?;
            let mut buckets: Vec<Vec<u32>> = (0..n_partitions).map(|_| Vec::new()).collect();
            for (row, &p) in assignments.iter().enumerate() {
                // NULL keys land on partition 0 with a special flag — but
                // we detect NULLs post-partition. Equal-key rows always
                // share the same bucket regardless (ahash of NULL is stable).
                buckets[p as usize].push(row as u32);
            }
            let schema = batch.schema();
            for (p, indices) in buckets.into_iter().enumerate() {
                if indices.is_empty() {
                    continue;
                }
                let idx_arr = UInt32Array::from(indices);
                let cols: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|i| {
                        compute::take(batch.column(i), &idx_arr, None).map_err(ExecutionError::from)
                    })
                    .collect::<Result<_, _>>()?;
                let sub = RecordBatch::try_new(schema.clone(), cols)?;
                pwriter.write_partition(p, &sub)?;
            }
            Ok(())
        };

        // Flush the in-mem chunk first.
        for b in &last_in_mem {
            partition_build_batch(&mut build_pwriter, b, &right_partitioner)?;
        }
        drop(last_in_mem);

        // Then re-stream every spilled chunk.
        for spill in &spilled_chunks {
            let reader = spill.open_reader()?;
            for batch_res in reader {
                let batch = batch_res?;
                partition_build_batch(&mut build_pwriter, &batch, &right_partitioner)?;
            }
            if spill_fadvise_build_only_enabled() {
                spill.evict_page_cache();
            }
        }
        drop(spilled_chunks);

        let mut build_pfile: PartitionedSpillFile = build_pwriter.finish()?;

        // ── Partition PROBE (left) into N spill buckets ───────────────────
        // NULL-key probe rows go to a dedicated null_probe spill file.
        let left_schema = arrow_schema_from_cols(&self.left.schema());
        let mut probe_pwriter =
            PartitionedSpillWriter::new(left_schema.clone(), n_partitions, "semi_grace_probe");
        let mut null_probe_writer: Option<SpillWriter> = None;

        let mut left_stream = self.left.execute(0).await?;
        while let Some(batch_res) = left_stream.next().await {
            let batch = batch_res.map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "{op_name}: left input error (partitioned spill): {e}"
                ))
            })?;
            if batch.num_rows() == 0 {
                continue;
            }
            // Evaluate the left key to detect NULLs.
            let left_keys_arr = expression::evaluate(&self.left_key, &batch, None)?;
            let assignments = left_partitioner.assignments(&batch)?;
            let mut buckets: Vec<Vec<u32>> = (0..n_partitions).map(|_| Vec::new()).collect();
            let mut null_rows: Vec<u32> = Vec::new();
            for (row, &p) in assignments.iter().enumerate() {
                if left_keys_arr.is_null(row) {
                    null_rows.push(row as u32);
                } else {
                    buckets[p as usize].push(row as u32);
                }
            }
            let schema = batch.schema();
            // Write non-null rows to their bucket.
            for (p, indices) in buckets.into_iter().enumerate() {
                if indices.is_empty() {
                    continue;
                }
                let idx_arr = UInt32Array::from(indices);
                let cols: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|i| {
                        compute::take(batch.column(i), &idx_arr, None).map_err(ExecutionError::from)
                    })
                    .collect::<Result<_, _>>()?;
                let sub = RecordBatch::try_new(schema.clone(), cols)?;
                probe_pwriter.write_partition(p, &sub)?;
            }
            // Null-key rows → null_probe_writer.
            if !null_rows.is_empty() {
                let idx_arr = UInt32Array::from(null_rows);
                let cols: Vec<ArrayRef> = (0..batch.num_columns())
                    .map(|i| {
                        compute::take(batch.column(i), &idx_arr, None).map_err(ExecutionError::from)
                    })
                    .collect::<Result<_, _>>()?;
                let null_batch = RecordBatch::try_new(schema.clone(), cols)?;
                if null_probe_writer.is_none() {
                    null_probe_writer =
                        Some(SpillWriter::new(schema.clone(), "semi_grace_probe_null")?);
                }
                null_probe_writer.as_mut().unwrap().write(&null_batch)?;
            }
        }

        let probe_pfile = probe_pwriter.finish()?;
        let null_probe_spill: Option<SpillFile> = match null_probe_writer {
            Some(w) => Some(w.finish()?),
            None => None,
        };

        // Precompute residual context (mirrors execute_multipass ResidualMultipassCtx).
        let residual_ctx: Option<ResidualMultipassCtx> =
            if let Some(residual) = self.residual.as_ref() {
                let left_width = self.left.schema().len();
                let mut needed_right_indices: Vec<usize> = Vec::new();
                let mut seen = std::collections::HashSet::new();
                collect_column_indices(residual, &mut |idx| {
                    if idx >= left_width {
                        let r = idx - left_width;
                        if seen.insert(r) {
                            needed_right_indices.push(r);
                        }
                    }
                });
                needed_right_indices.sort();

                let mut remap: std::collections::HashMap<usize, usize> =
                    std::collections::HashMap::new();
                for (new_pos, &orig_right_idx) in needed_right_indices.iter().enumerate() {
                    remap.insert(left_width + orig_right_idx, left_width + new_pos);
                }
                let pruned_residual = remap_column_indices(residual.clone(), &remap);

                let mut fields: Vec<Field> =
                    Vec::with_capacity(left_width + needed_right_indices.len());
                for c in self.left.schema().iter() {
                    fields.push(Field::new(&c.name, c.data_type.clone().into(), c.nullable));
                }
                let right_schema_cols = self.right.schema();
                for &i in &needed_right_indices {
                    let c = &right_schema_cols[i];
                    fields.push(Field::new(&c.name, c.data_type.clone().into(), c.nullable));
                }
                let joined_schema = Arc::new(Schema::new(fields));
                Some(ResidualMultipassCtx {
                    needed_right_indices,
                    pruned_residual,
                    joined_schema,
                })
            } else {
                None
            };

        // ── Per-bucket join ───────────────────────────────────────────────
        // For each bucket p: load build-p → build set/index, then stream
        // probe-p ONCE and probe it. Accumulate matched rows into result.
        let mut result_batches: Vec<RecordBatch> = Vec::new();

        for p in 0..n_partitions {
            // Load build bucket p (may be empty → no build rows in this bucket).
            let mut build_spill_for_eviction: Option<SpillFile> = None;
            let build_batches: Vec<RecordBatch> = match build_pfile.take_partition(p) {
                None => Vec::new(),
                Some(spill) => {
                    let reader = spill.open_reader()?;
                    let batches = reader.collect::<Result<Vec<_>, _>>()?;
                    build_spill_for_eviction = Some(spill);
                    batches
                }
            };

            // Process this bucket: collect probe batches from the spill file,
            // join against build_batches using the existing probe helpers.
            if build_batches.is_empty() {
                // No build rows in this bucket.
                // SEMI: no match possible → emit nothing for this bucket's probe rows.
                // ANTI: every probe row in this bucket is unmatched → emit them all.
                if self.anti {
                    if let Some(probe_spill_ref) = probe_pfile.partition(p) {
                        let reader = probe_spill_ref.open_reader()?;
                        for probe_batch_res in reader {
                            let probe_batch = probe_batch_res?;
                            if probe_batch.num_rows() == 0 {
                                continue;
                            }
                            result_batches.push(
                                probe_batch
                                    .project(&(0..output_schema.fields().len()).collect::<Vec<_>>())
                                    .unwrap_or(probe_batch),
                            );
                        }
                    }
                }
                // SEMI: do nothing (no output for this bucket).
                if let Some(spill) = build_spill_for_eviction.as_ref() {
                    if spill_fadvise_build_only_enabled() {
                        spill.evict_page_cache();
                    }
                }
                continue;
            }

            // Build has rows: use probe helpers.
            // Build a scratch "left_spill" from probe bucket p so we can reuse
            // the existing probe_chunk_{no_residual,residual} helpers unchanged.
            // Those helpers stream the probe from a SpillFile.

            // Read probe-bucket-p batches directly (without going through probe helpers
            // that need a SpillFile argument) to avoid borrowing issues.
            // Build matched mask for probe-bucket-p rows.
            let probe_batches: Vec<RecordBatch> =
                if let Some(probe_spill_ref) = probe_pfile.partition(p) {
                    let reader = probe_spill_ref.open_reader()?;
                    reader.collect::<Result<Vec<_>, _>>()?
                } else {
                    Vec::new() // empty probe bucket
                };

            if probe_batches.is_empty() {
                if let Some(spill) = build_spill_for_eviction.as_ref() {
                    if spill_fadvise_build_only_enabled() {
                        spill.evict_page_cache();
                    }
                }
                continue;
            }

            // Build hash set/index for this build bucket.
            // matched[batch_idx][row] = true if probe row found a build match.
            let probe_row_counts: Vec<usize> = probe_batches.iter().map(|b| b.num_rows()).collect();
            let mut matched: Vec<Vec<bool>> =
                probe_row_counts.iter().map(|&n| vec![false; n]).collect();

            if let Some(ctx) = &residual_ctx {
                // Residual path: build index from build_batches.
                let index = build_build_index(
                    &self.right_key,
                    &build_batches,
                    index_alloc_label(self.anti),
                )
                .await?;
                let pruned_chunk: Vec<RecordBatch> = build_batches
                    .iter()
                    .map(|b| project_record_batch(b, &ctx.needed_right_indices))
                    .collect::<Result<_, _>>()?;

                for (b_idx, probe_batch) in probe_batches.iter().enumerate() {
                    if probe_batch.num_rows() == 0 {
                        continue;
                    }
                    let left_keys_arr = expression::evaluate(&self.left_key, probe_batch, None)?;
                    let left_typed = TypedKey::from_array(&left_keys_arr)?;
                    let chunk_matches = probe_batch_residual(
                        &left_typed,
                        probe_batch.num_rows(),
                        &index,
                        probe_batch,
                        &pruned_chunk,
                        &ctx.pruned_residual,
                        &ctx.joined_schema,
                        false,
                    )?;
                    let mask = &mut matched[b_idx];
                    for row in chunk_matches {
                        mask[row as usize] = true;
                    }
                }
            } else {
                // No-residual path: build hash set from build_batches.
                let set = build_partitioned_set(
                    &self.right_key,
                    &build_batches,
                    set_alloc_label(self.anti),
                )
                .await?;
                for (b_idx, probe_batch) in probe_batches.iter().enumerate() {
                    if probe_batch.num_rows() == 0 {
                        continue;
                    }
                    let left_keys_arr = expression::evaluate(&self.left_key, probe_batch, None)?;
                    let left_typed = TypedKey::from_array(&left_keys_arr)?;
                    let mask = &mut matched[b_idx];
                    for (row, m) in mask.iter_mut().enumerate() {
                        if *m {
                            continue;
                        }
                        if left_typed.is_null(row) {
                            continue; // NULL never matches; handled in null_probe path
                        }
                        let h = left_typed.hash_row(row);
                        if set.contains(h) {
                            *m = true;
                        }
                    }
                }
            }

            // Emit surviving probe-bucket-p rows.
            for (b_idx, probe_batch) in probe_batches.into_iter().enumerate() {
                let n_rows = probe_batch.num_rows();
                let mask = &matched[b_idx];
                let mut keep: Vec<u32> = Vec::with_capacity(n_rows);
                for (r, &m) in mask.iter().enumerate() {
                    let keep_row = if self.anti { !m } else { m };
                    if keep_row {
                        keep.push(r as u32);
                    }
                }
                if keep.is_empty() {
                    continue;
                }
                let idx_arr = UInt32Array::from(keep);
                let cols: Vec<ArrayRef> = (0..probe_batch.num_columns())
                    .map(|i| {
                        compute::take(probe_batch.column(i), &idx_arr, None).map_err(|e| {
                            ExecutionError::InvalidOperation(format!("take failed: {e}"))
                        })
                    })
                    .collect::<Result<_, _>>()?;
                result_batches.push(RecordBatch::try_new(output_schema.clone(), cols)?);
            }
            if let Some(spill) = build_spill_for_eviction.as_ref() {
                if spill_fadvise_build_only_enabled() {
                    spill.evict_page_cache();
                }
            }
        }

        // ── Emit null-key probe rows ──────────────────────────────────────
        // SEMI: NULL key never matches → drop.
        // ANTI: NULL key never matches → always emit.
        if self.anti {
            if let Some(null_spill) = null_probe_spill {
                let reader = null_spill.open_reader()?;
                for batch_res in reader {
                    let batch = batch_res?;
                    if batch.num_rows() > 0 {
                        result_batches.push(batch);
                    }
                }
            }
        }

        Ok(stream_from_batches(output_schema, result_batches))
    }

    /// Multi-pass probe path used when the build side overflowed the
    /// memory budget and at least one chunk was spilled to disk
    /// (Phase 2b, 2026-05-21).
    ///
    /// Algorithm (Trino-inspired "whole-build spill with multi-pass
    /// probe"): collect left input once, scan against each build chunk
    /// in turn, OR each chunk's per-row match results into a single
    /// `matched` mask. After all chunks are processed, emit the left
    /// rows whose mask satisfies the semantic — `matched` for SEMI,
    /// `!matched` for ANTI. Memory peak is bounded by one chunk's
    /// `Vec<RecordBatch>` plus the left side plus the mask.
    ///
    /// Residual + spill is **not** supported yet — `execute` returns an
    /// error before reaching this path when both are present.
    /// Single-pass parallelism is also dropped here; the per-chunk
    /// `build_partitioned_set` parallelises internally and the multi-
    /// pass loop is sequential.
    async fn execute_multipass(
        &self,
        spilled_chunks: Vec<SpillFile>,
        last_in_mem: Vec<RecordBatch>,
        mut build_reservation: MemoryReservation,
        output_schema: Arc<Schema>,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        // Symmetric grace-partition spill gate (ARNEB_PARTITIONED_SEMI_SPILL).
        // When ON, both build and probe are hash-partitioned into N buckets so
        // each bucket of probe is joined against exactly ONE bucket of build —
        // O(build + probe) disk reads instead of the default O(chunks × probe).
        if partitioned_semi_spill_enabled() {
            return self
                .execute_partitioned_spill(
                    spilled_chunks,
                    last_in_mem,
                    build_reservation,
                    output_schema,
                )
                .await;
        }

        // SPILL the left/probe input to disk ONCE instead of collecting
        // it into RAM. The multi-pass probe re-reads it per build chunk;
        // streaming it from disk (Trino-style "spill both sides") bounds
        // the probe to one batch + the `matched` bitmap, instead of the
        // full ~lineitem-sized left. This is the q21 SF30 fix: the
        // collected left was the dominant ~8GB UNTRACKED allocation
        // (measured 2026-06-03 — budget tuning couldn't avoid it).
        let mut left_stream = self.left.execute(0).await?;
        let probe_prefix = if self.anti {
            "antijoin_probe"
        } else {
            "semijoin_probe"
        };
        let mut left_writer: Option<crate::spill::SpillWriter> = None;
        let mut left_row_counts: Vec<usize> = Vec::new();
        while let Some(batch_res) = left_stream.next().await {
            let batch = batch_res.map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "failed to read left input (multipass): {e}"
                ))
            })?;
            if left_writer.is_none() {
                left_writer = Some(crate::spill::SpillWriter::new(
                    batch.schema(),
                    probe_prefix,
                )?);
            }
            left_writer.as_mut().unwrap().write(&batch)?;
            left_row_counts.push(batch.num_rows());
        }
        // No left input → no output (matches the collect-then-empty path).
        let Some(left_writer) = left_writer else {
            return Ok(stream_from_batches(output_schema, vec![]));
        };
        let left_spill = left_writer.finish()?;

        // matched[batch_idx][row_idx] = found a match in any chunk.
        // Indexed by the spill's batch order (stable across re-opens).
        let mut matched: Vec<Vec<bool>> = left_row_counts.iter().map(|&n| vec![false; n]).collect();

        // For residual queries, precompute the pruning info ONCE so
        // each chunk's probe doesn't rederive it. The remap shrinks
        // the joined-schema indices in the residual expression to
        // the projected `[left | pruned_right]` shape, and the
        // `joined_schema` is reused across chunks.
        let residual_ctx: Option<ResidualMultipassCtx> =
            if let Some(residual) = self.residual.as_ref() {
                let left_width = self.left.schema().len();
                let mut needed_right_indices: Vec<usize> = Vec::new();
                let mut seen = std::collections::HashSet::new();
                collect_column_indices(residual, &mut |idx| {
                    if idx >= left_width {
                        let r = idx - left_width;
                        if seen.insert(r) {
                            needed_right_indices.push(r);
                        }
                    }
                });
                needed_right_indices.sort();

                let mut remap: std::collections::HashMap<usize, usize> =
                    std::collections::HashMap::new();
                for (new_pos, &orig_right_idx) in needed_right_indices.iter().enumerate() {
                    remap.insert(left_width + orig_right_idx, left_width + new_pos);
                }
                let pruned_residual = remap_column_indices(residual.clone(), &remap);

                let mut fields: Vec<Field> =
                    Vec::with_capacity(left_width + needed_right_indices.len());
                for c in self.left.schema().iter() {
                    fields.push(Field::new(&c.name, c.data_type.clone().into(), c.nullable));
                }
                let right_schema = self.right.schema();
                for &i in &needed_right_indices {
                    let c = &right_schema[i];
                    fields.push(Field::new(&c.name, c.data_type.clone().into(), c.nullable));
                }
                let joined_schema = Arc::new(Schema::new(fields));
                Some(ResidualMultipassCtx {
                    needed_right_indices,
                    pruned_residual,
                    joined_schema,
                })
            } else {
                None
            };

        // Process the last in-memory chunk first so we can release its
        // budget bytes before loading the spilled ones from disk.
        if !last_in_mem.is_empty() {
            let bytes: usize = last_in_mem
                .iter()
                .map(crate::operator::record_batch_bytes)
                .sum();
            if let Some(ctx) = &residual_ctx {
                probe_chunk_residual(
                    &self.right_key,
                    &self.left_key,
                    &last_in_mem,
                    ctx,
                    &left_spill,
                    &mut matched,
                    self.anti,
                )
                .await?;
            } else {
                probe_chunk_no_residual(
                    &self.right_key,
                    &self.left_key,
                    &last_in_mem,
                    &left_spill,
                    &mut matched,
                    self.anti,
                )
                .await?;
            }
            drop(last_in_mem);
            build_reservation.shrink(bytes);
        }

        // Walk each spilled chunk: load from IPC, build partitioned set,
        // probe, drop. A fresh reservation tracks the load-back bytes so
        // multi-pass also fails fast if the pool was sized so small that
        // not even ONE chunk fits — that indicates a budget too tight to
        // possibly succeed.
        let load_consumer = MemoryConsumer::new(if self.anti {
            "AntiJoinExec.spill_load"
        } else {
            "SemiJoinExec.spill_load"
        })
        .with_can_spill(false);
        let mut load_reservation = load_consumer.register(self.memory_pool.clone());

        for (chunk_idx, spill) in spilled_chunks.iter().enumerate() {
            let reader = spill.open_reader()?;
            let mut chunk_batches: Vec<RecordBatch> = Vec::new();
            let mut loaded_bytes: usize = 0;
            for batch_res in reader {
                let batch = batch_res?;
                let bytes = crate::operator::record_batch_bytes(&batch);
                load_reservation.try_grow(bytes)?;
                loaded_bytes += bytes;
                chunk_batches.push(batch);
            }
            tracing::info!(
                target: "arneb::mem",
                operator = if self.anti { "AntiJoinExec" } else { "SemiJoinExec" },
                chunk_idx,
                n_batches = chunk_batches.len(),
                loaded_bytes,
                has_residual = residual_ctx.is_some(),
                "multi-pass: loaded spilled chunk for probe",
            );
            if let Some(ctx) = &residual_ctx {
                probe_chunk_residual(
                    &self.right_key,
                    &self.left_key,
                    &chunk_batches,
                    ctx,
                    &left_spill,
                    &mut matched,
                    self.anti,
                )
                .await?;
            } else {
                probe_chunk_no_residual(
                    &self.right_key,
                    &self.left_key,
                    &chunk_batches,
                    &left_spill,
                    &mut matched,
                    self.anti,
                )
                .await?;
            }
            drop(chunk_batches);
            load_reservation.shrink(loaded_bytes);
            if spill_fadvise_build_only_enabled() {
                spill.evict_page_cache();
            }
        }

        // Final emit: re-stream the left from its spill file and pull the
        // surviving rows (per `matched`) into output batches. One left
        // batch in RAM at a time.
        let mut result_batches: Vec<RecordBatch> = Vec::with_capacity(matched.len());
        let emit_reader = left_spill.open_reader()?;
        for (b_idx, left_batch_res) in emit_reader.enumerate() {
            let left_batch = left_batch_res?;
            let n_rows = left_batch.num_rows();
            let mask = &matched[b_idx];
            let mut keep: Vec<u32> = Vec::with_capacity(n_rows);
            for (r, &m) in mask.iter().enumerate() {
                let keep_row = if self.anti { !m } else { m };
                if keep_row {
                    keep.push(r as u32);
                }
            }
            if keep.is_empty() {
                continue;
            }
            let idx_arr = UInt32Array::from(keep);
            let mut cols: Vec<ArrayRef> = Vec::with_capacity(left_batch.num_columns());
            for c in 0..left_batch.num_columns() {
                cols.push(
                    compute::take(left_batch.column(c), &idx_arr, None).map_err(|e| {
                        ExecutionError::InvalidOperation(format!("take failed: {e}"))
                    })?,
                );
            }
            result_batches.push(RecordBatch::try_new(output_schema.clone(), cols)?);
        }

        Ok(stream_from_batches(output_schema, result_batches))
    }
}

/// Probe a single build chunk against all left batches, OR'ing per-row
/// matches into the running `matched` mask. Used by the multi-pass
/// spill path; fast path only (no residual). NULL left keys are never
/// considered matches (same NULL semantics as `probe_batch_set`).
async fn probe_chunk_no_residual(
    right_key: &PlanExpr,
    left_key: &PlanExpr,
    chunk: &[RecordBatch],
    left_spill: &SpillFile,
    matched: &mut [Vec<bool>],
    anti: bool,
) -> Result<(), ExecutionError> {
    if chunk.is_empty() {
        return Ok(());
    }
    let set = build_partitioned_set(right_key, chunk, set_alloc_label(anti)).await?;
    // Stream the probe (left) input from its spill file instead of
    // holding it all in RAM. `matched` is indexed by the spill's batch
    // order, which is stable across re-opens. Bounds left memory to one
    // batch (2026-06-03 q21 fix).
    let reader = left_spill.open_reader()?;
    for (b_idx, left_batch_res) in reader.enumerate() {
        let left_batch = left_batch_res?;
        let n_rows = left_batch.num_rows();
        if n_rows == 0 {
            continue;
        }
        let left_keys_arr = expression::evaluate(left_key, &left_batch, None)?;
        let left_typed = TypedKey::from_array(&left_keys_arr)?;
        let mask = &mut matched[b_idx];
        for (row, m) in mask.iter_mut().enumerate() {
            if *m {
                // Already matched in an earlier chunk — skip the hash
                // probe; this is the equivalent of Trino's early-exit
                // on first match per probe row.
                continue;
            }
            if left_typed.is_null(row) {
                continue;
            }
            let h = left_typed.hash_row(row);
            if set.contains(h) {
                *m = true;
            }
        }
    }
    Ok(())
}

/// Precomputed pruning info reused across every chunk in a residual
/// multi-pass run. The pruning depends only on the residual expression
/// and the left/right schemas — not on the actual batch data — so the
/// pruning work happens once and each chunk just projects its own
/// batches with the cached `needed_right_indices`.
struct ResidualMultipassCtx {
    needed_right_indices: Vec<usize>,
    pruned_residual: PlanExpr,
    joined_schema: Arc<Schema>,
}

/// Residual-aware variant of [`probe_chunk_no_residual`].
///
/// For each left batch:
///   1. Build a `PartitionedIndex` over the chunk's batches (full
///      schema, used for hash-only candidate lookup).
///   2. Project the chunk to only the residual-referenced columns —
///      `probe_batch_residual` takes the pruned chunk for evaluating
///      the residual on candidate pairs.
///   3. Call `probe_batch_residual` with `anti=false` so the returned
///      indices represent left rows that found at least one matching
///      candidate AND passed the residual. OR them into the running
///      `matched` mask.
///
/// `anti` semantics are applied at emit time in `execute_multipass`
/// (anti emits `!matched`), so per-chunk we always treat matches as
/// "found a passing candidate in this chunk".
async fn probe_chunk_residual(
    right_key: &PlanExpr,
    left_key: &PlanExpr,
    chunk: &[RecordBatch],
    ctx: &ResidualMultipassCtx,
    left_spill: &SpillFile,
    matched: &mut [Vec<bool>],
    anti: bool,
) -> Result<(), ExecutionError> {
    if chunk.is_empty() {
        return Ok(());
    }
    let index = build_build_index(right_key, chunk, index_alloc_label(anti)).await?;
    let pruned_chunk: Vec<RecordBatch> = chunk
        .iter()
        .map(|b| project_record_batch(b, &ctx.needed_right_indices))
        .collect::<Result<_, _>>()?;

    // Stream the probe (left) from its spill file (see no_residual variant).
    let reader = left_spill.open_reader()?;
    for (b_idx, left_batch_res) in reader.enumerate() {
        let left_batch = left_batch_res?;
        let n_rows = left_batch.num_rows();
        if n_rows == 0 {
            continue;
        }
        let left_keys_arr = expression::evaluate(left_key, &left_batch, None)?;
        let left_typed = TypedKey::from_array(&left_keys_arr)?;

        let chunk_matches = probe_batch_residual(
            &left_typed,
            n_rows,
            &index,
            &left_batch,
            &pruned_chunk,
            &ctx.pruned_residual,
            &ctx.joined_schema,
            false,
        )?;

        let mask = &mut matched[b_idx];
        for row in chunk_matches {
            // OR'd into running mask — `false` from probe_batch_residual
            // means the residual didn't pass for this row in this chunk,
            // so we leave `mask[row]` at whatever earlier chunks set it.
            mask[row as usize] = true;
        }
    }
    Ok(())
}

/// Fast-path probe: hash each left row, route by partition, look up
/// against that partition's flat `HashSet<u64>`. NULL keys never
/// match, so they're kept for ANTI and dropped for SEMI (matches the
/// existing NULL semantics).
fn probe_batch_set(
    left_typed: &TypedKey<'_>,
    n_rows: usize,
    right_set: &AccountedSet,
    anti: bool,
) -> Vec<u32> {
    let mut keep_idx: Vec<u32> = Vec::with_capacity(n_rows);
    for row in 0..n_rows {
        let matched = if left_typed.is_null(row) {
            false
        } else {
            right_set.contains(left_typed.hash_row(row))
        };
        let keep = if anti { !matched } else { matched };
        if keep {
            keep_idx.push(row as u32);
        }
    }
    keep_idx
}

#[allow(clippy::too_many_arguments)]
fn probe_one_left_batch(
    left_batch: RecordBatch,
    left_key: &PlanExpr,
    right_set: Option<&AccountedSet>,
    right_index: Option<&AccountedBuildIndex>,
    right_batches_pruned: Option<&[RecordBatch]>,
    residual: Option<&PlanExpr>,
    joined_schema: Option<&Arc<Schema>>,
    output_schema: Arc<Schema>,
    anti: bool,
) -> Result<Option<RecordBatch>, ExecutionError> {
    let n_rows = left_batch.num_rows();
    if n_rows == 0 {
        return Ok(None);
    }
    let left_keys_arr = expression::evaluate(left_key, &left_batch, None)?;
    let left_typed = TypedKey::from_array(&left_keys_arr)?;

    let keep_idx =
        if let (Some(idx_map), Some(right_batches_pruned), Some(residual), Some(joined_schema)) =
            (right_index, right_batches_pruned, residual, joined_schema)
        {
            probe_batch_residual(
                &left_typed,
                n_rows,
                idx_map,
                &left_batch,
                right_batches_pruned,
                residual,
                joined_schema,
                anti,
            )?
        } else if let Some(set) = right_set {
            probe_batch_set(&left_typed, n_rows, set, anti)
        } else if anti {
            (0..n_rows as u32).collect()
        } else {
            Vec::new()
        };

    if keep_idx.is_empty() {
        return Ok(None);
    }
    let idx_array = UInt32Array::from(keep_idx);
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(left_batch.num_columns());
    for c in 0..left_batch.num_columns() {
        cols.push(
            compute::take(left_batch.column(c), &idx_array, None)
                .map_err(|e| ExecutionError::InvalidOperation(format!("take failed: {e}")))?,
        );
    }
    Ok(Some(RecordBatch::try_new(output_schema, cols)?))
}

/// Residual-path probe: for each left row, look up the bucket of
/// candidate `(batch_idx, row_idx)` right addresses by hash, group
/// them by source batch, materialise one joined batch per group via
/// per-source `compute::take`, evaluate the residual once per group,
/// and mark which left rows have at least one passing match.
///
/// Mirrors Trino's `DefaultPageJoiner` semantics — the build side is
/// stored as a `Vec<RecordBatch>` (Arrow analog of Trino's per-Page
/// channels in `PagesIndex`), so cross-page matches are routed back
/// to their source batch via the encoded `(batch_idx, row_idx)`
/// address, avoiding the giant `concat_batches` peak.
/// A two-column comparison residual reduces to:
///   left_col[cand_left_row]  <op>  right_col[cand_right_row]
/// Skip the generic `expression::evaluate` + RecordBatch construction
/// path: only the two referenced cols are needed, not all left/right
/// cols. For TPC-H Q21's `l1.l_suppkey != l2.l_suppkey`, this avoids
/// taking ~30 unused left cols per probe batch (the 4-way INNER join
/// of supplier × l1 × orders × nation feeds into SemiJoin).
#[derive(Debug, Clone, Copy)]
struct SpecializedResidual {
    /// Position of the left operand's column in the joined schema.
    /// Resolved at SemiJoinExec build into left/right side at use time.
    left_idx: usize,
    /// Position of the right operand's column in the joined schema.
    right_idx: usize,
    op: arneb_sql_parser::ast::BinaryOp,
}

impl SpecializedResidual {
    /// Recognise residual shapes the specialized fast path can handle.
    /// v1: `BinaryOp(Column{i}, op, Column{j})` where op is a 2-sided
    /// comparison (Eq/NotEq/Lt/LtEq/Gt/GtEq).
    fn try_from(residual: &PlanExpr) -> Option<Self> {
        use arneb_sql_parser::ast::BinaryOp as B;
        if let PlanExpr::BinaryOp {
            left, op, right, ..
        } = residual
        {
            let (PlanExpr::Column { index: li, .. }, PlanExpr::Column { index: ri, .. }) =
                (left.as_ref(), right.as_ref())
            else {
                return None;
            };
            if !matches!(op, B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq) {
                return None;
            }
            Some(Self {
                left_idx: *li,
                right_idx: *ri,
                op: *op,
            })
        } else {
            None
        }
    }
}

/// Resolve a `SpecializedResidual`'s two column indices against the
/// joined `[left | pruned_right]` schema and take the candidate rows
/// from the relevant source batch. Returns `(left_operand_taken,
/// right_operand_taken)` ready for `typed_compare_mask`.
///
/// `left_width` is `left_batch.num_columns()`. Indices `< left_width`
/// reference `left_batch`; indices `>= left_width` reference
/// `right_batch` at position `idx - left_width` (this is the
/// `pruned_right` index produced by F-Perf3).
fn take_residual_pair(
    spec: &SpecializedResidual,
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    left_width: usize,
    left_idx: &UInt32Array,
    right_idx: &UInt32Array,
) -> Result<(ArrayRef, ArrayRef), ExecutionError> {
    let pick = |spec_idx: usize| -> Result<ArrayRef, ExecutionError> {
        if spec_idx < left_width {
            // left side: index into left_batch via cand_left rows.
            Ok(compute::take(left_batch.column(spec_idx), left_idx, None)?)
        } else {
            let right_col_idx = spec_idx - left_width;
            // right side: index into right_batch via cand_right rows.
            Ok(compute::take(
                right_batch.column(right_col_idx),
                right_idx,
                None,
            )?)
        }
    };
    let a = pick(spec.left_idx)?;
    let b = pick(spec.right_idx)?;
    Ok((a, b))
}

/// Apply `op` to two ArrayRefs of matching dtype, returning a
/// BooleanArray mask. Mirrors `expression::compare_op` but kept
/// inline here so `probe_batch_residual` doesn't pay the call cost
/// of crossing the expression evaluator boundary.
fn typed_compare_mask(
    left: &ArrayRef,
    right: &ArrayRef,
    op: arneb_sql_parser::ast::BinaryOp,
) -> Result<BooleanArray, ExecutionError> {
    use arneb_sql_parser::ast::BinaryOp as B;
    use arrow::compute::kernels::cmp;
    // `cmp::*` kernels take `&dyn Datum`; `&ArrayRef` deref-converts.
    let l: &dyn arrow::array::Datum = left;
    let r: &dyn arrow::array::Datum = right;
    let mask = match op {
        B::Eq => cmp::eq(l, r)?,
        B::NotEq => cmp::neq(l, r)?,
        B::Lt => cmp::lt(l, r)?,
        B::LtEq => cmp::lt_eq(l, r)?,
        B::Gt => cmp::gt(l, r)?,
        B::GtEq => cmp::gt_eq(l, r)?,
        _ => unreachable!("SpecializedResidual::try_from filtered other ops"),
    };
    Ok(mask)
}

#[allow(clippy::too_many_arguments)]
fn probe_batch_residual(
    left_typed: &TypedKey<'_>,
    n_rows: usize,
    right_index: &AccountedBuildIndex,
    left_batch: &RecordBatch,
    right_batches: &[RecordBatch],
    residual: &PlanExpr,
    joined_schema: &Arc<Schema>,
    anti: bool,
) -> Result<Vec<u32>, ExecutionError> {
    // Pre-detect whether the residual is a simple two-column
    // comparison; if so, the per-batch loop skips materialising the
    // joined RecordBatch and the generic expression tree walk.
    let specialized = SpecializedResidual::try_from(residual);
    let left_width = left_batch.num_columns();
    // Gather candidates grouped by source batch so we can run one
    // vectorised `take` + residual eval per batch (preserving Arrow
    // batch-level efficiency instead of falling back to Trino's
    // per-cell `appendTo` shape).
    //
    // Use a `Vec<(Vec, Vec)>` indexed directly by `batch_idx` instead
    // of a `HashMap<u32, ...>` — for Q21 with 5872 right batches and
    // ~40K candidate pairs per probe-batch, the HashMap entry lookup
    // per match was ~25% of probe time (F-Perf10a Q21 regression).
    // Direct array indexing is one bounds-check + pointer-add and
    // avoids the per-match hash + bucket probe. The empty-`Vec`
    // initialisation cost is paid in capacity allocation only (the
    // `Vec`s start with no heap allocation).
    //
    // Per-probe-row early-exit (Trino's `outputSingleMatch=true`) was
    // prototyped here but regressed Q21 by ~2× because arneb's
    // expression evaluator has high per-call overhead (UInt32Array +
    // RecordBatch construction + downcast). The break-on-first-match
    // savings (~50% candidate eval) did NOT recover the per-row Arrow
    // pipeline cost. Re-enable only when expression eval gains a JIT
    // path or per-row fast lane.
    let n_right_batches = right_batches.len();
    let mut per_batch: Vec<(Vec<u32>, Vec<u32>)> = (0..n_right_batches)
        .map(|_| (Vec::new(), Vec::new()))
        .collect();
    for row in 0..n_rows {
        if left_typed.is_null(row) {
            continue;
        }
        let hash = left_typed.hash_row(row);
        for (batch_idx, row_idx) in right_index.matches(hash) {
            if (batch_idx as usize) < n_right_batches {
                let entry = &mut per_batch[batch_idx as usize];
                entry.0.push(row as u32);
                entry.1.push(row_idx);
            }
        }
    }

    let mut matched = vec![false; n_rows];
    for (batch_idx, (cand_left, cand_right)) in per_batch.into_iter().enumerate() {
        if cand_left.is_empty() {
            continue;
        }
        // Trino-style `outputSingleMatch` early-exit (skip candidates
        // for already-matched probe rows) was prototyped here. With
        // the specialized residual fast path the per-pair cost is ~1µs
        // and the savings should be 20-40%, but bench showed Q21
        // wall-time was unchanged on average with INCREASED run-to-run
        // variance (40% vs 30%) — the filter's data-dependent batch
        // sizes prevent the regular Arrow kernels from amortising.
        // Reverted. Re-attempt only when probe parallelism / cache
        // locality is more predictable.
        let right_batch = &right_batches[batch_idx];
        let left_idx = UInt32Array::from(cand_left.clone());
        let right_idx = UInt32Array::from(cand_right);

        let mask: BooleanArray = if let Some(spec) = specialized {
            // Specialised path: take ONLY the two cols referenced by
            // the residual, run Arrow's compare kernel directly. Saves
            // the per-batch RecordBatch::try_new + expression tree walk
            // + ~N unused-column takes that the generic path incurred.
            let (a, b) = take_residual_pair(
                &spec,
                left_batch,
                right_batch,
                left_width,
                &left_idx,
                &right_idx,
            )?;
            typed_compare_mask(&a, &b, spec.op)?
        } else {
            // Generic path: materialise the joined batch, evaluate the
            // residual expression. Used for residuals that aren't a
            // simple two-column comparison (e.g. `Cast(...) < expr`,
            // function calls, OR-trees).
            let mut cols: Vec<ArrayRef> = Vec::with_capacity(joined_schema.fields().len());
            for c in 0..left_batch.num_columns() {
                cols.push(compute::take(left_batch.column(c), &left_idx, None)?);
            }
            for c in 0..right_batch.num_columns() {
                cols.push(compute::take(right_batch.column(c), &right_idx, None)?);
            }
            let joined = RecordBatch::try_new(joined_schema.clone(), cols)?;
            let mask_arr = expression::evaluate(residual, &joined, None)?;
            mask_arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ExecutionError::InvalidOperation(
                        "semi-join residual predicate must evaluate to boolean".into(),
                    )
                })?
                .clone()
        };

        for (i, lrow) in cand_left.into_iter().enumerate() {
            if !mask.is_null(i) && mask.value(i) {
                matched[lrow as usize] = true;
            }
        }
    }

    let mut keep_idx: Vec<u32> = Vec::with_capacity(n_rows);
    for (row, &is_match) in matched.iter().enumerate() {
        let keep = if anti { !is_match } else { is_match };
        if keep {
            keep_idx.push(row as u32);
        }
    }
    Ok(keep_idx)
}

fn joined_schema_from_cols(left_schema: &[ColumnInfo], right_schema: &[ColumnInfo]) -> Arc<Schema> {
    let mut fields: Vec<Field> = Vec::with_capacity(left_schema.len() + right_schema.len());
    for c in left_schema {
        fields.push(Field::new(&c.name, c.data_type.clone().into(), c.nullable));
    }
    for c in right_schema {
        fields.push(Field::new(&c.name, c.data_type.clone().into(), c.nullable));
    }
    Arc::new(Schema::new(fields))
}

fn mark_left_matches_for_right_batch(
    right_batch: &RecordBatch,
    right_key: &PlanExpr,
    left_index: &AccountedBuildIndex,
    left_batches: &[RecordBatch],
    residual: Option<&PlanExpr>,
    joined_schema: Option<&Arc<Schema>>,
    matched: &mut [Vec<bool>],
) -> Result<(), ExecutionError> {
    let right_keys_arr = expression::evaluate(right_key, right_batch, None)?;
    let right_typed = TypedKey::from_array(&right_keys_arr)?;

    let mut per_left_batch: Vec<(Vec<u32>, Vec<u32>)> = (0..left_batches.len())
        .map(|_| (Vec::new(), Vec::new()))
        .collect();
    for right_row in 0..right_batch.num_rows() {
        if right_typed.is_null(right_row) {
            continue;
        }
        let hash = right_typed.hash_row(right_row);
        for (left_batch_idx, left_row_idx) in left_index.matches(hash) {
            if (left_batch_idx as usize) < left_batches.len() {
                let entry = &mut per_left_batch[left_batch_idx as usize];
                entry.0.push(left_row_idx);
                entry.1.push(right_row as u32);
            }
        }
    }

    if residual.is_none() {
        for (batch_idx, (left_rows, _)) in per_left_batch.into_iter().enumerate() {
            for left_row in left_rows {
                matched[batch_idx][left_row as usize] = true;
            }
        }
        return Ok(());
    }

    let residual = residual.expect("checked above");
    let joined_schema = joined_schema.expect("residual path has joined schema");
    let specialized = SpecializedResidual::try_from(residual);
    for (batch_idx, (cand_left, cand_right)) in per_left_batch.into_iter().enumerate() {
        if cand_left.is_empty() {
            continue;
        }
        let left_batch = &left_batches[batch_idx];
        let left_idx = UInt32Array::from(cand_left.clone());
        let right_idx = UInt32Array::from(cand_right);

        let mask: BooleanArray = if let Some(spec) = specialized {
            let (a, b) = take_residual_pair(
                &spec,
                left_batch,
                right_batch,
                left_batch.num_columns(),
                &left_idx,
                &right_idx,
            )?;
            typed_compare_mask(&a, &b, spec.op)?
        } else {
            let mut cols: Vec<ArrayRef> = Vec::with_capacity(joined_schema.fields().len());
            for c in 0..left_batch.num_columns() {
                cols.push(compute::take(left_batch.column(c), &left_idx, None)?);
            }
            for c in 0..right_batch.num_columns() {
                cols.push(compute::take(right_batch.column(c), &right_idx, None)?);
            }
            let joined = RecordBatch::try_new(joined_schema.clone(), cols)?;
            let mask_arr = expression::evaluate(residual, &joined, None)?;
            mask_arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ExecutionError::InvalidOperation(
                        "semi-join residual predicate must evaluate to boolean".into(),
                    )
                })?
                .clone()
        };

        for (i, left_row) in cand_left.into_iter().enumerate() {
            if !mask.is_null(i) && mask.value(i) {
                matched[batch_idx][left_row as usize] = true;
            }
        }
    }

    Ok(())
}

type ResidualProbePrep = (
    Option<PlanExpr>,
    Option<Vec<RecordBatch>>,
    Option<Arc<Schema>>,
);

fn prepare_residual_probe(
    residual: Option<&PlanExpr>,
    left_schema: &[ColumnInfo],
    right_schema: &[ColumnInfo],
    right_batches: &[RecordBatch],
) -> Result<ResidualProbePrep, ExecutionError> {
    if let Some(residual) = residual {
        let left_width = left_schema.len();
        let mut needed_right_indices: Vec<usize> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        collect_column_indices(residual, &mut |idx| {
            if idx >= left_width {
                let r = idx - left_width;
                if seen.insert(r) {
                    needed_right_indices.push(r);
                }
            }
        });
        needed_right_indices.sort();

        let pruned: Result<Vec<RecordBatch>, ExecutionError> = right_batches
            .iter()
            .map(|b| project_record_batch(b, &needed_right_indices))
            .collect();
        let pruned = pruned?;

        let mut remap: std::collections::HashMap<usize, usize> = std::collections::HashMap::new();
        for (new_pos, &orig_right_idx) in needed_right_indices.iter().enumerate() {
            remap.insert(left_width + orig_right_idx, left_width + new_pos);
        }
        let pruned_resid = remap_column_indices(residual.clone(), &remap);

        let mut fields: Vec<Field> = Vec::with_capacity(left_width + needed_right_indices.len());
        for c in left_schema {
            fields.push(Field::new(&c.name, c.data_type.clone().into(), c.nullable));
        }
        if let Some(first) = pruned.first() {
            for f in first.schema().fields().iter() {
                fields.push(f.as_ref().clone());
            }
        } else {
            for &i in &needed_right_indices {
                let c = &right_schema[i];
                fields.push(Field::new(&c.name, c.data_type.clone().into(), c.nullable));
            }
        }
        let schema = Arc::new(Schema::new(fields));
        Ok((Some(pruned_resid), Some(pruned), Some(schema)))
    } else {
        Ok((None, None, None))
    }
}

struct SemiAsyncBatchStream {
    schema: Arc<Schema>,
    inner: Pin<Box<dyn futures::Stream<Item = Result<RecordBatch, ExecutionError>> + Send>>,
}

impl futures::Stream for SemiAsyncBatchStream {
    type Item = Result<RecordBatch, arneb_common::error::ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner
            .as_mut()
            .poll_next(cx)
            .map(|opt| opt.map(|res| res.map_err(arneb_common::error::ArnebError::Execution)))
    }
}

impl arneb_common::stream::RecordBatchStream for SemiAsyncBatchStream {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

/// Walk a [`PlanExpr`] tree and invoke `cb` once per `Column` leaf,
/// passing its index. Used for column-pruning analysis (find which
/// columns the residual touches) without allocating a result vector.
fn collect_column_indices<F: FnMut(usize)>(expr: &PlanExpr, cb: &mut F) {
    match expr {
        PlanExpr::Column { index, .. } => cb(*index),
        PlanExpr::Literal { .. } | PlanExpr::Parameter { .. } => {}
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_column_indices(left, cb);
            collect_column_indices(right, cb);
        }
        PlanExpr::UnaryOp { expr, .. } => collect_column_indices(expr, cb),
        PlanExpr::Function { args, .. } => {
            for a in args {
                collect_column_indices(a, cb);
            }
        }
        PlanExpr::IsNull { expr, .. } | PlanExpr::IsNotNull { expr, .. } => {
            collect_column_indices(expr, cb);
        }
        PlanExpr::InList { expr, list, .. } => {
            collect_column_indices(expr, cb);
            for e in list {
                collect_column_indices(e, cb);
            }
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            collect_column_indices(expr, cb);
            collect_column_indices(low, cb);
            collect_column_indices(high, cb);
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_column_indices(op, cb);
            }
            for (w, t) in when_clauses {
                collect_column_indices(w, cb);
                collect_column_indices(t, cb);
            }
            if let Some(e) = else_result {
                collect_column_indices(e, cb);
            }
        }
        PlanExpr::Cast { expr, .. } => collect_column_indices(expr, cb),
        PlanExpr::ScalarSubquery { .. } | PlanExpr::Wildcard => {
            // Scalar subqueries have their own index scope; Wildcard
            // has no columns to collect.
        }
    }
}

/// Return a new [`PlanExpr`] with every `Column { index }` rewritten
/// to its mapped value if present in `remap`, leaving anything not in
/// the map untouched.
fn remap_column_indices(
    expr: PlanExpr,
    remap: &std::collections::HashMap<usize, usize>,
) -> PlanExpr {
    match expr {
        PlanExpr::Column { index, name, span } => {
            let new_index = remap.get(&index).copied().unwrap_or(index);
            PlanExpr::Column {
                index: new_index,
                name,
                span,
            }
        }
        PlanExpr::Literal { .. } | PlanExpr::Parameter { .. } => expr,
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => PlanExpr::BinaryOp {
            left: Box::new(remap_column_indices(*left, remap)),
            op,
            right: Box::new(remap_column_indices(*right, remap)),
            span,
        },
        PlanExpr::UnaryOp { op, expr, span } => PlanExpr::UnaryOp {
            op,
            expr: Box::new(remap_column_indices(*expr, remap)),
            span,
        },
        PlanExpr::Function {
            name,
            args,
            distinct,
            span,
        } => PlanExpr::Function {
            name,
            args: args
                .into_iter()
                .map(|a| remap_column_indices(a, remap))
                .collect(),
            distinct,
            span,
        },
        PlanExpr::IsNull { expr, span } => PlanExpr::IsNull {
            expr: Box::new(remap_column_indices(*expr, remap)),
            span,
        },
        PlanExpr::IsNotNull { expr, span } => PlanExpr::IsNotNull {
            expr: Box::new(remap_column_indices(*expr, remap)),
            span,
        },
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => PlanExpr::InList {
            expr: Box::new(remap_column_indices(*expr, remap)),
            list: list
                .into_iter()
                .map(|e| remap_column_indices(e, remap))
                .collect(),
            negated,
            span,
        },
        PlanExpr::Between {
            expr,
            low,
            high,
            negated,
            span,
        } => PlanExpr::Between {
            expr: Box::new(remap_column_indices(*expr, remap)),
            low: Box::new(remap_column_indices(*low, remap)),
            high: Box::new(remap_column_indices(*high, remap)),
            negated,
            span,
        },
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => PlanExpr::CaseExpr {
            operand: operand.map(|o| Box::new(remap_column_indices(*o, remap))),
            when_clauses: when_clauses
                .into_iter()
                .map(|(w, t)| {
                    (
                        remap_column_indices(w, remap),
                        remap_column_indices(t, remap),
                    )
                })
                .collect(),
            else_result: else_result.map(|e| Box::new(remap_column_indices(*e, remap))),
            span,
        },
        PlanExpr::Cast {
            expr,
            data_type,
            span,
        } => PlanExpr::Cast {
            expr: Box::new(remap_column_indices(*expr, remap)),
            data_type,
            span,
        },
        // Identity for variants whose column indices don't apply
        // here (scalar subquery uses a separate scope; Wildcard
        // expands to all columns).
        PlanExpr::ScalarSubquery { .. } | PlanExpr::Wildcard => expr,
    }
}

/// Project a record batch down to the given column indices (in
/// order). Used to shrink the right side of a SemiJoin residual to
/// just the columns the predicate reads.
fn project_record_batch(
    batch: &RecordBatch,
    indices: &[usize],
) -> Result<RecordBatch, ExecutionError> {
    let in_schema = batch.schema();
    let mut fields: Vec<Field> = Vec::with_capacity(indices.len());
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(indices.len());
    for &i in indices {
        fields.push(in_schema.field(i).clone());
        cols.push(batch.column(i).clone());
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, cols)
        .map_err(|e| ExecutionError::InvalidOperation(format!("project right batch: {e}")))
}

// ===========================================================================
// Partitioned build (Trino-style, no merge)
// ===========================================================================

/// Build a build-side `HashSet<u64>` split into `N` partitions by
/// `hash & mask`. The probe routes each row to one partition; no
/// merge is needed.
///
/// Mirrors Trino's `PartitionedLookupSource` storage shape — see
/// `PartitionedLookupSource.java:137-146` in trino-main: probe routes
/// via `partitionGenerator.getPartition(rawHash)` then looks up in
/// that partition's flat hash table.
struct PartitionedSet {
    partitions: Vec<FastHashSet<u64>>,
    mask: u64,
}

impl PartitionedSet {
    #[inline]
    fn contains(&self, hash: u64) -> bool {
        if self.partitions.len() == 1 {
            self.partitions[0].contains(&hash)
        } else {
            let p = (hash & self.mask) as usize;
            self.partitions[p].contains(&hash)
        }
    }

    fn heap_bytes(&self) -> u64 {
        self.partitions
            .iter()
            .map(|partition| partition.capacity() as u64 * (size_of::<u64>() as u64 + 16))
            .sum()
    }
}

struct AccountedSet {
    inner: PartitionedSet,
    _heap_guard: LiveBytesGuard,
}

impl AccountedSet {
    fn new(inner: PartitionedSet, label: &'static str) -> Self {
        let bytes = inner.heap_bytes();
        Self {
            inner,
            _heap_guard: LiveBytesGuard::new(label, bytes),
        }
    }

    #[inline]
    fn contains(&self, hash: u64) -> bool {
        self.inner.contains(hash)
    }
}

/// Build-side `HashMap<u64, Vec<(u32, u32)>>` for the residual probe
/// path. Each hash bucket holds `(batch_idx, row_idx)` addresses
/// into the per-batch `Vec<RecordBatch>` build storage — the Apache
/// Arrow analog of Trino's `valueAddresses` `(pageIndex, position)`
/// encoding in `PagesIndex.java:241-253`. Same partitioning scheme
/// as [`PartitionedSet`].
struct PartitionedIndex {
    partitions: Vec<FastHashMap<u64, Vec<(u32, u32)>>>,
    mask: u64,
}

impl PartitionedIndex {
    #[inline]
    fn get(&self, hash: u64) -> Option<&[(u32, u32)]> {
        if self.partitions.len() == 1 {
            self.partitions[0].get(&hash).map(Vec::as_slice)
        } else {
            let p = (hash & self.mask) as usize;
            self.partitions[p].get(&hash).map(Vec::as_slice)
        }
    }

    fn heap_bytes(&self) -> u64 {
        let map_bytes: u64 = self
            .partitions
            .iter()
            .map(|partition| {
                partition.capacity() as u64
                    * (size_of::<u64>() as u64 + size_of::<Vec<(u32, u32)>>() as u64 + 16)
            })
            .sum();
        let value_bytes: u64 = self
            .partitions
            .iter()
            .flat_map(|partition| partition.values())
            .map(|addresses| addresses.capacity() as u64 * size_of::<(u32, u32)>() as u64)
            .sum();
        map_bytes + value_bytes
    }
}

struct FlatPartition {
    head: Vec<i32>,
    next: Vec<i32>,
    entry_hash: Vec<u64>,
    entry_addr: Vec<(u32, u32)>,
}

impl FlatPartition {
    fn from_entries(entries: &[(u64, (u32, u32))]) -> Self {
        Self::from_entries_with_head_len(entries, entries.len().next_power_of_two().max(1))
    }

    fn from_entries_with_head_len(entries: &[(u64, (u32, u32))], head_len: usize) -> Self {
        let head_len = head_len.next_power_of_two().max(1);
        let mut head = vec![-1; head_len];
        let mut next = vec![-1; entries.len()];
        let mut entry_hash = Vec::with_capacity(entries.len());
        let mut entry_addr = Vec::with_capacity(entries.len());
        let mask = head.len() - 1;

        for (i, (hash, addr)) in entries.iter().copied().enumerate() {
            let bucket = (hash as usize) & mask;
            next[i] = head[bucket];
            head[bucket] = i as i32;
            entry_hash.push(hash);
            entry_addr.push(addr);
        }

        Self {
            head,
            next,
            entry_hash,
            entry_addr,
        }
    }

    #[inline]
    fn matches(&self, hash: u64) -> FlatMatches<'_> {
        let bucket = (hash as usize) & (self.head.len() - 1);
        FlatMatches {
            partition: self,
            hash,
            next_idx: self.head[bucket],
        }
    }

    fn heap_bytes(&self) -> u64 {
        (self.head.capacity() as u64 * size_of::<i32>() as u64)
            + (self.next.capacity() as u64 * size_of::<i32>() as u64)
            + (self.entry_hash.capacity() as u64 * size_of::<u64>() as u64)
            + (self.entry_addr.capacity() as u64 * size_of::<(u32, u32)>() as u64)
    }
}

struct FlatMatches<'a> {
    partition: &'a FlatPartition,
    hash: u64,
    next_idx: i32,
}

impl Iterator for FlatMatches<'_> {
    type Item = (u32, u32);

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_idx >= 0 {
            let idx = self.next_idx as usize;
            self.next_idx = self.partition.next[idx];
            if self.partition.entry_hash[idx] == self.hash {
                return Some(self.partition.entry_addr[idx]);
            }
        }
        None
    }
}

struct FlatIndex {
    partitions: Vec<FlatPartition>,
    mask: u64,
}

impl FlatIndex {
    #[inline]
    fn matches(&self, hash: u64) -> FlatMatches<'_> {
        if self.partitions.len() == 1 {
            self.partitions[0].matches(hash)
        } else {
            let p = (hash & self.mask) as usize;
            self.partitions[p].matches(hash)
        }
    }

    fn heap_bytes(&self) -> u64 {
        self.partitions.iter().map(FlatPartition::heap_bytes).sum()
    }
}

enum BuildIndex {
    Hashed(PartitionedIndex),
    Flat(FlatIndex),
}

impl BuildIndex {
    fn heap_bytes(&self) -> u64 {
        match self {
            Self::Hashed(index) => index.heap_bytes(),
            Self::Flat(index) => index.heap_bytes(),
        }
    }

    #[inline]
    fn matches(&self, hash: u64) -> IndexMatches<'_> {
        match self {
            Self::Hashed(index) => IndexMatches::Hashed(index.get(hash).unwrap_or(&[]).iter()),
            Self::Flat(index) => IndexMatches::Flat(index.matches(hash)),
        }
    }
}

enum IndexMatches<'a> {
    Hashed(std::slice::Iter<'a, (u32, u32)>),
    Flat(FlatMatches<'a>),
}

impl Iterator for IndexMatches<'_> {
    type Item = (u32, u32);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Hashed(iter) => iter.next().copied(),
            Self::Flat(iter) => iter.next(),
        }
    }
}

struct AccountedBuildIndex {
    inner: BuildIndex,
    _heap_guard: LiveBytesGuard,
}

impl AccountedBuildIndex {
    fn new(inner: BuildIndex, label: &'static str) -> Self {
        let bytes = inner.heap_bytes();
        Self {
            inner,
            _heap_guard: LiveBytesGuard::new(label, bytes),
        }
    }

    #[inline]
    fn matches(&self, hash: u64) -> IndexMatches<'_> {
        self.inner.matches(hash)
    }
}

fn index_alloc_label(anti: bool) -> &'static str {
    if anti {
        "AntiJoinExec.index"
    } else {
        "SemiJoinExec.index"
    }
}

fn set_alloc_label(anti: bool) -> &'static str {
    if anti {
        "AntiJoinExec.set"
    } else {
        "SemiJoinExec.set"
    }
}

/// Below this many right rows, the parallel-spawn overhead exceeds
/// the savings — stay single-partition. Q04's filtered lineitem is
/// ~3.6M rows so easily crosses this; Q11's nation-region builds are
/// tens or hundreds of rows and stay serial.
const PARALLEL_BUILD_MIN_ROWS: usize = 100_000;

fn choose_partition_count(n_rows: usize) -> usize {
    if n_rows < PARALLEL_BUILD_MIN_ROWS {
        1
    } else {
        // Power-of-two count so the partition index is a single
        // `hash & mask` (one bit-and, no modulo).
        num_cpus::get().max(1).next_power_of_two().min(16)
    }
}

/// Compute per-batch hashes for the right-side build. Returns a
/// flat `Vec<Vec<Option<u64>>>` — one inner Vec per batch, one
/// `Option<u64>` per row (None for NULL keys). Single-threaded but
/// fast: ~5ns/row × 6M = ~30ms.
fn compute_right_hashes(
    right_key: &PlanExpr,
    right_batches: &[RecordBatch],
) -> Result<Vec<Vec<Option<u64>>>, ExecutionError> {
    let mut out: Vec<Vec<Option<u64>>> = Vec::with_capacity(right_batches.len());
    for batch in right_batches {
        let keys = expression::evaluate(right_key, batch, None)?;
        let typed = TypedKey::from_array(&keys)?;
        let n = batch.num_rows();
        let mut hashes: Vec<Option<u64>> = Vec::with_capacity(n);
        for row in 0..n {
            if typed.is_null(row) {
                hashes.push(None);
            } else {
                hashes.push(Some(typed.hash_row(row)));
            }
        }
        out.push(hashes);
    }
    Ok(out)
}

async fn build_partitioned_set(
    right_key: &PlanExpr,
    right_batches: &[RecordBatch],
    label: &'static str,
) -> Result<AccountedSet, ExecutionError> {
    let total_rows: usize = right_batches.iter().map(|b| b.num_rows()).sum();
    let n_partitions = choose_partition_count(total_rows);

    // Phase 1 (serial): compute per-batch hashes once.
    let hashes_per_batch = compute_right_hashes(right_key, right_batches)?;

    if n_partitions <= 1 {
        let mut set: FastHashSet<u64> = FastHashSet::default();
        set.reserve(total_rows);
        for hashes in &hashes_per_batch {
            for h in hashes.iter().flatten() {
                set.insert(*h);
            }
        }
        return Ok(AccountedSet::new(
            PartitionedSet {
                partitions: vec![set],
                mask: 0,
            },
            label,
        ));
    }

    // Phase 2 (parallel): each partition scans the shared per-batch
    // hashes and inserts only those that route to it. Per-task work:
    // total_rows × 1ns mask-check + (total_rows / N_partitions) × ~20ns
    // insert. Wall clock ≈ Phase 1 + slowest task.
    let mask = (n_partitions - 1) as u64;
    let hashes_per_batch = Arc::new(hashes_per_batch);
    let mut handles = Vec::<tokio::task::JoinHandle<FastHashSet<u64>>>::with_capacity(n_partitions);
    for p in 0..n_partitions as u64 {
        let hpb = hashes_per_batch.clone();
        handles.push(tokio::task::spawn_blocking(move || {
            let mut set: FastHashSet<u64> = FastHashSet::default();
            let total: usize = hpb.iter().map(|v| v.len()).sum();
            set.reserve(total / (mask as usize + 1) + 16);
            for hashes in hpb.iter() {
                for h in hashes.iter().flatten() {
                    if (h & mask) == p {
                        set.insert(*h);
                    }
                }
            }
            set
        }));
    }

    let mut partitions = Vec::with_capacity(n_partitions);
    for h in handles {
        partitions.push(
            h.await
                .map_err(|e| ExecutionError::InvalidOperation(format!("semi build task: {e}")))?,
        );
    }
    Ok(AccountedSet::new(
        PartitionedSet { partitions, mask },
        label,
    ))
}

async fn build_build_index(
    right_key: &PlanExpr,
    right_batches: &[RecordBatch],
    label: &'static str,
) -> Result<AccountedBuildIndex, ExecutionError> {
    let index = if flat_semi_index_enabled() {
        BuildIndex::Flat(build_flat_index(right_key, right_batches)?)
    } else {
        BuildIndex::Hashed(build_partitioned_index(right_key, right_batches).await?)
    };
    Ok(AccountedBuildIndex::new(index, label))
}

async fn build_partitioned_index(
    right_key: &PlanExpr,
    right_batches: &[RecordBatch],
) -> Result<PartitionedIndex, ExecutionError> {
    let total_rows: usize = right_batches.iter().map(|b| b.num_rows()).sum();
    let n_partitions = choose_partition_count(total_rows);

    // Phase 1: compute per-batch hashes serially.
    let hashes_per_batch = compute_right_hashes(right_key, right_batches)?;

    if n_partitions <= 1 {
        let mut map: FastHashMap<u64, Vec<(u32, u32)>> = FastHashMap::default();
        for (batch_idx, hashes) in hashes_per_batch.iter().enumerate() {
            for (row_idx, h_opt) in hashes.iter().enumerate() {
                if let Some(h) = h_opt {
                    map.entry(*h)
                        .or_default()
                        .push((batch_idx as u32, row_idx as u32));
                }
            }
        }
        return Ok(PartitionedIndex {
            partitions: vec![map],
            mask: 0,
        });
    }

    // Phase 2: each partition scans (hash, batch_idx, row_idx) tuples
    // and owns the ones whose hash routes to it.
    let mask = (n_partitions - 1) as u64;
    let hashes_per_batch = Arc::new(hashes_per_batch);
    let mut handles =
        Vec::<tokio::task::JoinHandle<FastHashMap<u64, Vec<(u32, u32)>>>>::with_capacity(
            n_partitions,
        );
    for p in 0..n_partitions as u64 {
        let hpb = hashes_per_batch.clone();
        handles.push(tokio::task::spawn_blocking(move || {
            let mut map: FastHashMap<u64, Vec<(u32, u32)>> = FastHashMap::default();
            for (batch_idx, hashes) in hpb.iter().enumerate() {
                for (row_idx, h_opt) in hashes.iter().enumerate() {
                    if let Some(h) = h_opt {
                        if (h & mask) == p {
                            map.entry(*h)
                                .or_default()
                                .push((batch_idx as u32, row_idx as u32));
                        }
                    }
                }
            }
            map
        }));
    }

    let mut partitions = Vec::with_capacity(n_partitions);
    for h in handles {
        partitions.push(
            h.await
                .map_err(|e| ExecutionError::InvalidOperation(format!("semi build task: {e}")))?,
        );
    }
    Ok(PartitionedIndex { partitions, mask })
}

fn build_flat_index(
    right_key: &PlanExpr,
    right_batches: &[RecordBatch],
) -> Result<FlatIndex, ExecutionError> {
    let total_rows: usize = right_batches.iter().map(|b| b.num_rows()).sum();
    let n_partitions = choose_partition_count(total_rows);
    let hashes_per_batch = compute_right_hashes(right_key, right_batches)?;

    if n_partitions <= 1 {
        let mut entries = Vec::with_capacity(total_rows);
        for (batch_idx, hashes) in hashes_per_batch.iter().enumerate() {
            for (row_idx, h_opt) in hashes.iter().enumerate() {
                if let Some(h) = h_opt {
                    entries.push((*h, (batch_idx as u32, row_idx as u32)));
                }
            }
        }
        return Ok(FlatIndex {
            partitions: vec![FlatPartition::from_entries(&entries)],
            mask: 0,
        });
    }

    let mask = (n_partitions - 1) as u64;
    let mut per_partition: Vec<Vec<(u64, (u32, u32))>> =
        (0..n_partitions).map(|_| Vec::new()).collect();
    for (batch_idx, hashes) in hashes_per_batch.iter().enumerate() {
        for (row_idx, h_opt) in hashes.iter().enumerate() {
            if let Some(h) = h_opt {
                let partition = (*h & mask) as usize;
                per_partition[partition].push((*h, (batch_idx as u32, row_idx as u32)));
            }
        }
    }

    let partitions = per_partition
        .iter()
        .map(|entries| FlatPartition::from_entries(entries))
        .collect();
    Ok(FlatIndex { partitions, mask })
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::{DataSource, InMemoryDataSource};
    use crate::memory_pool::GreedyMemoryPool;
    use crate::operator::ScanExec;
    use crate::scan_context::ScanContext;
    use arneb_common::stream::collect_stream;
    use arneb_common::types::DataType;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};

    static ENV_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn scan_of_batches(name: &str, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        scan_of_batches_with_nullable(name, batches, false)
    }

    fn scan_of_batches_with_nullable(
        name: &str,
        batches: Vec<RecordBatch>,
        nullable: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let source: Arc<dyn DataSource> = Arc::new(InMemoryDataSource::new(
            vec![ColumnInfo {
                name: "k".to_string(),
                data_type: DataType::Int32,
                nullable,
            }],
            batches,
        ));
        Arc::new(ScanExec {
            source,
            _table_name: name.to_string(),
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

    fn one_col_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k",
            ArrowDataType::Int32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn one_col_batch_opt(values: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "k",
            ArrowDataType::Int32,
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn int32_batch(names: &[&str], columns: Vec<Vec<i32>>) -> RecordBatch {
        let fields: Vec<Field> = names
            .iter()
            .map(|name| Field::new(*name, ArrowDataType::Int32, false))
            .collect();
        let arrays: Vec<ArrayRef> = columns
            .into_iter()
            .map(|values| Arc::new(Int32Array::from(values)) as ArrayRef)
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    fn nullable_int64_batch(names: &[&str], columns: Vec<Vec<Option<i64>>>) -> RecordBatch {
        let fields: Vec<Field> = names
            .iter()
            .map(|name| Field::new(*name, ArrowDataType::Int64, true))
            .collect();
        let arrays: Vec<ArrayRef> = columns
            .into_iter()
            .map(|values| Arc::new(Int64Array::from(values)) as ArrayRef)
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    fn scan_of_int32_schema(
        name: &str,
        columns: &[(&str, DataType)],
        batches: Vec<RecordBatch>,
    ) -> Arc<dyn ExecutionPlan> {
        let source: Arc<dyn DataSource> = Arc::new(InMemoryDataSource::new(
            columns
                .iter()
                .map(|(name, data_type)| ColumnInfo {
                    name: (*name).to_string(),
                    data_type: data_type.clone(),
                    nullable: false,
                })
                .collect(),
            batches,
        ));
        Arc::new(ScanExec {
            source,
            _table_name: name.to_string(),
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

    fn key_expr() -> PlanExpr {
        PlanExpr::Column {
            index: 0,
            name: "k".to_string(),
            span: None,
        }
    }

    fn eq_residual() -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "k".to_string(),
                span: None,
            }),
            op: arneb_sql_parser::ast::BinaryOp::Eq,
            right: Box::new(PlanExpr::Column {
                index: 1,
                name: "k".to_string(),
                span: None,
            }),
            span: None,
        }
    }

    fn suppkey_not_equal_residual() -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 1,
                name: "l_suppkey".to_string(),
                span: None,
            }),
            op: arneb_sql_parser::ast::BinaryOp::NotEq,
            right: Box::new(PlanExpr::Column {
                index: 4,
                name: "r_suppkey".to_string(),
                span: None,
            }),
            span: None,
        }
    }

    fn right_suppkey_not_equal_residual() -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 4,
                name: "r_suppkey".to_string(),
                span: None,
            }),
            op: arneb_sql_parser::ast::BinaryOp::NotEq,
            right: Box::new(PlanExpr::Column {
                index: 1,
                name: "l_suppkey".to_string(),
                span: None,
            }),
            span: None,
        }
    }

    fn orderkey_expr() -> PlanExpr {
        PlanExpr::Column {
            index: 0,
            name: "orderkey".to_string(),
            span: None,
        }
    }

    fn rowids_from_batches(out: &[RecordBatch]) -> Vec<i32> {
        let mut got = Vec::new();
        for b in out {
            let col = b
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("row_id Int32 col");
            for r in 0..b.num_rows() {
                got.push(col.value(r));
            }
        }
        got.sort();
        got
    }

    fn int64_rowids_from_batches(out: &[RecordBatch]) -> Vec<i64> {
        let mut got = Vec::new();
        for b in out {
            let col = b
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("row_id Int64 col");
            for r in 0..b.num_rows() {
                got.push(col.value(r));
            }
        }
        got.sort();
        got
    }

    fn rows_from_batches(out: &[RecordBatch]) -> Vec<i32> {
        let mut got = Vec::new();
        for b in out {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 col");
            for r in 0..b.num_rows() {
                got.push(col.value(r));
            }
        }
        got.sort();
        got
    }

    fn optional_rows_from_batches(out: &[RecordBatch]) -> Vec<Option<i32>> {
        let mut got = Vec::new();
        for b in out {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 col");
            for r in 0..b.num_rows() {
                got.push((!col.is_null(r)).then_some(col.value(r)));
            }
        }
        got.sort();
        got
    }

    async fn run_singlepass_case(
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        residual: Option<PlanExpr>,
        anti: bool,
        streaming: bool,
    ) -> Vec<i32> {
        if streaming {
            std::env::set_var("ARNEB_STREAM_SEMI_PROBE", "1");
        } else {
            std::env::remove_var("ARNEB_STREAM_SEMI_PROBE");
        }
        let left = scan_of_batches("L", left_batches);
        let right = scan_of_batches("R", right_batches);
        let pool: Arc<dyn MemoryPool> = Arc::new(crate::memory_pool::UnboundedMemoryPool::new());
        let key = key_expr();
        let semi = SemiJoinExec::new(left, right, key.clone(), key, residual, anti, pool);
        let stream = semi.execute(0).await.expect("execute ok");
        let out = collect_stream(stream).await.expect("collect ok");
        rows_from_batches(&out)
    }

    async fn assert_streaming_matches_collect(
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        residual: Option<PlanExpr>,
        anti: bool,
    ) -> (Vec<i32>, Vec<i32>) {
        let _guard = ENV_TEST_LOCK.lock().await;
        let collect = run_singlepass_case(
            left_batches.clone(),
            right_batches.clone(),
            residual.clone(),
            anti,
            false,
        )
        .await;
        let streaming =
            run_singlepass_case(left_batches, right_batches, residual, anti, true).await;
        std::env::remove_var("ARNEB_STREAM_SEMI_PROBE");
        assert_eq!(streaming, collect);
        (collect, streaming)
    }

    async fn run_mark_join_case(
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        residual: Option<PlanExpr>,
        anti: bool,
        gate_on: bool,
    ) -> Vec<Option<i32>> {
        if gate_on {
            std::env::set_var("ARNEB_SEMI_MARK_JOIN", "1");
        } else {
            std::env::remove_var("ARNEB_SEMI_MARK_JOIN");
        }
        let left = scan_of_batches_with_nullable("L", left_batches, true);
        let right = scan_of_batches_with_nullable("R", right_batches, true);
        let pool: Arc<dyn MemoryPool> = Arc::new(crate::memory_pool::UnboundedMemoryPool::new());
        let key = key_expr();
        let semi = SemiJoinExec::new(left, right, key.clone(), key, residual, anti, pool)
            .with_build_left(true);
        let stream = semi.execute(0).await.expect("execute ok");
        let out = collect_stream(stream).await.expect("collect ok");
        optional_rows_from_batches(&out)
    }

    async fn assert_mark_join_matches_build_right(
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        residual: Option<PlanExpr>,
        anti: bool,
    ) -> Vec<Option<i32>> {
        let _guard = ENV_TEST_LOCK.lock().await;
        std::env::remove_var("ARNEB_STREAM_SEMI_PROBE");
        std::env::remove_var("ARNEB_PARTITIONED_SEMI_SPILL");
        let build_right = run_mark_join_case(
            left_batches.clone(),
            right_batches.clone(),
            residual.clone(),
            anti,
            false,
        )
        .await;
        let build_left =
            run_mark_join_case(left_batches, right_batches, residual, anti, true).await;
        std::env::remove_var("ARNEB_SEMI_MARK_JOIN");
        assert_eq!(build_left, build_right);
        build_right
    }

    async fn run_flat_index_equivalence_case(gate_on: bool, anti: bool) -> Vec<i32> {
        if gate_on {
            std::env::set_var("ARNEB_FLAT_SEMI_INDEX", "1");
        } else {
            std::env::remove_var("ARNEB_FLAT_SEMI_INDEX");
        }
        std::env::remove_var("ARNEB_STREAM_SEMI_PROBE");
        std::env::remove_var("ARNEB_PARTITIONED_SEMI_SPILL");
        std::env::remove_var("ARNEB_SEMI_MARK_JOIN");

        let left_batches = vec![int32_batch(
            &["orderkey", "l_suppkey", "row_id"],
            vec![
                vec![1, 1, 2, 3, 4, 5],
                vec![10, 20, 30, 40, 50, 60],
                vec![100, 101, 102, 103, 104, 105],
            ],
        )];
        let right_batches = vec![
            int32_batch(
                &["orderkey", "r_suppkey"],
                vec![vec![1, 1, 2], vec![10, 99, 30]],
            ),
            int32_batch(&["orderkey", "r_suppkey"], vec![vec![3, 5], vec![41, 60]]),
        ];
        let left = scan_of_int32_schema(
            "L",
            &[
                ("orderkey", DataType::Int32),
                ("l_suppkey", DataType::Int32),
                ("row_id", DataType::Int32),
            ],
            left_batches,
        );
        let right = scan_of_int32_schema(
            "R",
            &[
                ("orderkey", DataType::Int32),
                ("r_suppkey", DataType::Int32),
            ],
            right_batches,
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(crate::memory_pool::UnboundedMemoryPool::new());
        let semi = SemiJoinExec::new(
            left,
            right,
            orderkey_expr(),
            orderkey_expr(),
            Some(suppkey_not_equal_residual()),
            anti,
            pool,
        );
        let stream = semi.execute(0).await.expect("execute ok");
        let out = collect_stream(stream).await.expect("collect ok");
        rowids_from_batches(&out)
    }

    async fn run_compact_ne_case(
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        anti: bool,
        gate_on: bool,
    ) -> Vec<i64> {
        if gate_on {
            std::env::set_var("ARNEB_COMPACT_SEMI_NE", "1");
        } else {
            std::env::remove_var("ARNEB_COMPACT_SEMI_NE");
        }
        std::env::remove_var("ARNEB_STREAM_SEMI_PROBE");
        std::env::remove_var("ARNEB_PARTITIONED_SEMI_SPILL");
        std::env::remove_var("ARNEB_SEMI_MARK_JOIN");

        let left = scan_of_batches_with_schema(
            "L",
            vec![
                ColumnInfo {
                    name: "orderkey".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
                ColumnInfo {
                    name: "l_suppkey".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
                ColumnInfo {
                    name: "row_id".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
            ],
            left_batches,
        );
        let right = scan_of_batches_with_schema(
            "R",
            vec![
                ColumnInfo {
                    name: "orderkey".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
                ColumnInfo {
                    name: "r_suppkey".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
            ],
            right_batches,
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(crate::memory_pool::UnboundedMemoryPool::new());
        let join = SemiJoinExec::new(
            left,
            right,
            orderkey_expr(),
            orderkey_expr(),
            Some(right_suppkey_not_equal_residual()),
            anti,
            pool,
        );
        let stream = join.execute(0).await.expect("compact equivalence execute");
        let out = collect_stream(stream)
            .await
            .expect("compact equivalence collect");
        int64_rowids_from_batches(&out)
    }

    fn scan_of_batches_with_schema(
        name: &str,
        schema: Vec<ColumnInfo>,
        batches: Vec<RecordBatch>,
    ) -> Arc<dyn ExecutionPlan> {
        let source: Arc<dyn DataSource> = Arc::new(InMemoryDataSource::new(schema, batches));
        Arc::new(ScanExec {
            source,
            _table_name: name.to_string(),
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

    fn compact_adversarial_inputs() -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        let left = vec![
            nullable_int64_batch(
                &["orderkey", "l_suppkey", "row_id"],
                vec![
                    vec![Some(0), Some(1), Some(1), Some(2), Some(3), Some(4)],
                    vec![Some(9), Some(10), Some(11), Some(20), Some(30), None],
                    (0..6).map(Some).collect(),
                ],
            ),
            nullable_int64_batch(
                &["orderkey", "l_suppkey", "row_id"],
                vec![
                    vec![Some(4), Some(5), Some(6), None, Some(7), Some(8)],
                    vec![Some(40), Some(50), None, Some(60), Some(70), Some(80)],
                    (6..12).map(Some).collect(),
                ],
            ),
        ];
        let right = vec![
            nullable_int64_batch(
                &["orderkey", "r_suppkey"],
                vec![
                    vec![Some(1), Some(1), Some(2), Some(3), Some(4), Some(4)],
                    vec![Some(10), Some(10), Some(20), None, None, Some(40)],
                ],
            ),
            nullable_int64_batch(
                &["orderkey", "r_suppkey"],
                vec![
                    vec![Some(5), Some(5), Some(6), Some(6), Some(6), None],
                    vec![Some(51), Some(52), Some(60), Some(61), Some(62), Some(999)],
                ],
            ),
            nullable_int64_batch(
                &["orderkey", "r_suppkey"],
                vec![
                    vec![Some(5), Some(5), Some(7)],
                    vec![Some(53), Some(53), None],
                ],
            ),
        ];
        (left, right)
    }

    async fn assert_compact_ne_matches_full(anti: bool) {
        let (left, right) = compact_adversarial_inputs();
        let full = run_compact_ne_case(left.clone(), right.clone(), anti, false).await;
        let compact = run_compact_ne_case(left, right, anti, true).await;
        assert_eq!(compact, full);

        let empty_right = Vec::new();
        let (left, _) = compact_adversarial_inputs();
        let full = run_compact_ne_case(left.clone(), empty_right.clone(), anti, false).await;
        let compact = run_compact_ne_case(left, empty_right, anti, true).await;
        assert_eq!(compact, full, "empty build");

        let empty_left = Vec::new();
        let (_, right) = compact_adversarial_inputs();
        let full = run_compact_ne_case(empty_left.clone(), right.clone(), anti, false).await;
        let compact = run_compact_ne_case(empty_left, right, anti, true).await;
        assert_eq!(compact, full, "empty probe");

        let mut seed = 0x5eed_u64;
        let mut next = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            (seed >> 32) as u32
        };
        let mut left_key = Vec::new();
        let mut left_value = Vec::new();
        let mut row_id = Vec::new();
        for id in 0..257 {
            let r = next();
            left_key.push((r % 17 != 0).then_some((r % 23) as i64));
            left_value.push((r % 11 != 0).then_some(((r >> 8) % 13) as i64));
            row_id.push(Some(id));
        }
        let mut right_key = Vec::new();
        let mut right_value = Vec::new();
        for _ in 0..401 {
            let r = next();
            right_key.push((r % 19 != 0).then_some((r % 23) as i64));
            right_value.push((r % 7 != 0).then_some(((r >> 8) % 13) as i64));
        }
        let left = vec![
            nullable_int64_batch(
                &["orderkey", "l_suppkey", "row_id"],
                vec![
                    left_key[..101].to_vec(),
                    left_value[..101].to_vec(),
                    row_id[..101].to_vec(),
                ],
            ),
            nullable_int64_batch(
                &["orderkey", "l_suppkey", "row_id"],
                vec![
                    left_key[101..].to_vec(),
                    left_value[101..].to_vec(),
                    row_id[101..].to_vec(),
                ],
            ),
        ];
        let right = vec![
            nullable_int64_batch(
                &["orderkey", "r_suppkey"],
                vec![right_key[..137].to_vec(), right_value[..137].to_vec()],
            ),
            nullable_int64_batch(
                &["orderkey", "r_suppkey"],
                vec![right_key[137..299].to_vec(), right_value[137..299].to_vec()],
            ),
            nullable_int64_batch(
                &["orderkey", "r_suppkey"],
                vec![right_key[299..].to_vec(), right_value[299..].to_vec()],
            ),
        ];
        let full = run_compact_ne_case(left.clone(), right.clone(), anti, false).await;
        let compact = run_compact_ne_case(left, right, anti, true).await;
        assert_eq!(compact, full, "deterministic randomized input");
    }

    #[tokio::test]
    async fn compact_ne_semi_matches_full_residual_path() {
        let _guard = ENV_TEST_LOCK.lock().await;
        assert_compact_ne_matches_full(false).await;
        std::env::remove_var("ARNEB_COMPACT_SEMI_NE");
    }

    #[tokio::test]
    async fn compact_ne_anti_matches_full_residual_path() {
        let _guard = ENV_TEST_LOCK.lock().await;
        assert_compact_ne_matches_full(true).await;
        std::env::remove_var("ARNEB_COMPACT_SEMI_NE");
    }

    #[tokio::test]
    async fn flat_index_matches_hash_index_for_residual_semi_and_anti() {
        let _guard = ENV_TEST_LOCK.lock().await;

        let colliding =
            FlatPartition::from_entries_with_head_len(&[(0, (0, 0)), (2, (0, 1)), (0, (0, 2))], 1);
        let mut hash0: Vec<(u32, u32)> = colliding.matches(0).collect();
        hash0.sort();
        assert_eq!(hash0, vec![(0, 0), (0, 2)]);
        assert_eq!(colliding.matches(2).collect::<Vec<_>>(), vec![(0, 1)]);

        let semi_hash = run_flat_index_equivalence_case(false, false).await;
        let semi_flat = run_flat_index_equivalence_case(true, false).await;
        assert_eq!(semi_hash, vec![100, 101, 103]);
        assert_eq!(semi_flat, semi_hash);

        let anti_hash = run_flat_index_equivalence_case(false, true).await;
        let anti_flat = run_flat_index_equivalence_case(true, true).await;
        assert_eq!(anti_hash, vec![102, 104, 105]);
        assert_eq!(anti_flat, anti_hash);

        std::env::remove_var("ARNEB_FLAT_SEMI_INDEX");
    }

    /// Drives the spill path: 4 right batches of 100 ints each (~400 B
    /// per batch with Arrow overhead, but a tiny pool budget forces a
    /// spill after the first batch). The semi-join output must include
    /// every left row whose key has a corresponding right key,
    /// regardless of which chunk the right key landed in.
    #[tokio::test]
    async fn spill_path_semi_join_correctness() {
        // Left = [1,2,3,4,5,6,7,8,9,10] in one batch.
        let left = scan_of_batches("L", vec![one_col_batch((1..=10).collect())]);
        // Right side spread across 4 batches: each batch carries 100
        // arbitrary i32s, but the values 3, 5, 7 only appear (once
        // each) in BATCHES 2, 3, 4 respectively so we know they survive
        // multi-chunk reassembly. Filler 1000+ values guarantee no
        // collision with the left key range.
        let r1: Vec<i32> = (1000..1100).collect();
        let mut r2: Vec<i32> = (2000..2100).collect();
        r2.push(3);
        let mut r3: Vec<i32> = (3000..3100).collect();
        r3.push(5);
        let mut r4: Vec<i32> = (4000..4100).collect();
        r4.push(7);
        let right = scan_of_batches(
            "R",
            vec![
                one_col_batch(r1),
                one_col_batch(r2),
                one_col_batch(r3),
                one_col_batch(r4),
            ],
        );
        // Pool tight enough that ~one batch fits at a time.
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2048));
        let key = PlanExpr::Column {
            index: 0,
            name: "k".to_string(),
            span: None,
        };
        let semi = SemiJoinExec::new(left, right, key.clone(), key, None, false, pool);
        let stream = semi.execute(0).await.expect("execute ok");
        let out_batches = collect_stream(stream).await.expect("collect ok");
        let mut got: Vec<i32> = Vec::new();
        for b in &out_batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 col");
            for r in 0..b.num_rows() {
                got.push(col.value(r));
            }
        }
        got.sort();
        assert_eq!(
            got,
            vec![3, 5, 7],
            "semi-join must surface every left row whose key appears in ANY right chunk"
        );
    }

    /// Anti-join semantics through the spill path. With the same right
    /// side, the AntiJoin output is the left rows NOT found in any
    /// chunk (i.e. {1,2,4,6,8,9,10}).
    #[tokio::test]
    async fn spill_path_anti_join_correctness() {
        let left = scan_of_batches("L", vec![one_col_batch((1..=10).collect())]);
        let r1: Vec<i32> = (1000..1100).collect();
        let mut r2: Vec<i32> = (2000..2100).collect();
        r2.push(3);
        let mut r3: Vec<i32> = (3000..3100).collect();
        r3.push(5);
        let mut r4: Vec<i32> = (4000..4100).collect();
        r4.push(7);
        let right = scan_of_batches(
            "R",
            vec![
                one_col_batch(r1),
                one_col_batch(r2),
                one_col_batch(r3),
                one_col_batch(r4),
            ],
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2048));
        let key = PlanExpr::Column {
            index: 0,
            name: "k".to_string(),
            span: None,
        };
        let anti = SemiJoinExec::new(left, right, key.clone(), key, None, true, pool);
        let stream = anti.execute(0).await.expect("execute ok");
        let out_batches = collect_stream(stream).await.expect("collect ok");
        let mut got: Vec<i32> = Vec::new();
        for b in &out_batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 col");
            for r in 0..b.num_rows() {
                got.push(col.value(r));
            }
        }
        got.sort();
        assert_eq!(
            got,
            vec![1, 2, 4, 6, 8, 9, 10],
            "anti-join must surface left rows absent from EVERY chunk"
        );
    }

    /// Z (2026-06-05): the per-chunk spill cap forces the build to spill
    /// into multiple SMALL chunks even when the memory pool is generous
    /// enough to hold the whole build in one piece. This bounds the
    /// multi-pass reload working set — the q21 SF30 fix where a single
    /// ~1.8 GB chunk was loaded back WHOLE and blew the budget when two
    /// big joins reloaded concurrently. We assert both correctness AND
    /// that the peak resident build bytes stayed far below the full build
    /// (proving the cap, not the pool, drove chunking).
    #[tokio::test]
    async fn spill_chunk_cap_bounds_resident_under_generous_pool() {
        let left = scan_of_batches("L", vec![one_col_batch((1..=10).collect())]);
        let r1: Vec<i32> = (1000..1100).collect();
        let mut r2: Vec<i32> = (2000..2100).collect();
        r2.push(3);
        let mut r3: Vec<i32> = (3000..3100).collect();
        r3.push(5);
        let mut r4: Vec<i32> = (4000..4100).collect();
        r4.push(7);
        let right = scan_of_batches(
            "R",
            vec![
                one_col_batch(r1),
                one_col_batch(r2),
                one_col_batch(r3),
                one_col_batch(r4),
            ],
        );
        // Generous pool — `try_grow` NEVER fails, so WITHOUT a cap the
        // whole build stays resident (single-pass, no spill). A tiny cap
        // must still force a multi-chunk spill → multi-pass probe.
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        let key = PlanExpr::Column {
            index: 0,
            name: "k".to_string(),
            span: None,
        };
        let semi = SemiJoinExec::new(left, right, key.clone(), key, None, false, pool)
            .with_spill_chunk_bytes(16); // 16 bytes → each batch is its own chunk
        let stream = semi.execute(0).await.expect("execute ok");
        let out_batches = collect_stream(stream).await.expect("collect ok");
        let mut got: Vec<i32> = Vec::new();
        for b in &out_batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 col");
            for r in 0..b.num_rows() {
                got.push(col.value(r));
            }
        }
        got.sort();
        assert_eq!(
            got,
            vec![3, 5, 7],
            "cap-driven multi-pass spill must stay correct"
        );
        // The peak RESIDENT build state must be far below the full build
        // (4 batches × ~100 i32 ≈ 1600 bytes). With a 16-byte cap each
        // batch spills immediately, so the resident peak ≈ one batch.
        let full_build_lower_bound: usize = 4 * 100 * 4;
        assert!(
            semi.peak_bytes_reserved() < full_build_lower_bound,
            "cap must bound resident build memory; got peak={} >= full~{}",
            semi.peak_bytes_reserved(),
            full_build_lower_bound,
        );
    }

    /// Phase 2b.2: residual + spill works end-to-end. The residual is
    /// a trivial `true` literal so every candidate match passes —
    /// semantically equivalent to the no-residual case, but exercises
    /// `probe_chunk_residual` (and thus `build_partitioned_index` +
    /// `probe_batch_residual`) rather than `probe_chunk_no_residual`.
    #[tokio::test]
    async fn spill_with_residual_semi_join_correctness() {
        let left = scan_of_batches("L", vec![one_col_batch((1..=10).collect())]);
        let mut r2: Vec<i32> = (2000..2100).collect();
        r2.push(3);
        let mut r3: Vec<i32> = (3000..3100).collect();
        r3.push(5);
        let mut r4: Vec<i32> = (4000..4100).collect();
        r4.push(7);
        let right = scan_of_batches(
            "R",
            vec![
                one_col_batch((1000..1100).collect()),
                one_col_batch(r2),
                one_col_batch(r3),
                one_col_batch(r4),
            ],
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2048));
        let key = PlanExpr::Column {
            index: 0,
            name: "k".to_string(),
            span: None,
        };
        // Realistic residual: `l.k == r.k`. Every (left, right)
        // candidate pair that matches by hash also has identical key
        // values (we hashed on the same key), so the residual passes
        // for every candidate. Exercises `probe_chunk_residual` and
        // ensures the right-side column gets included in the residual
        // pruning step.
        let residual = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "k".to_string(),
                span: None,
            }),
            op: arneb_sql_parser::ast::BinaryOp::Eq,
            right: Box::new(PlanExpr::Column {
                index: 1,
                name: "k".to_string(),
                span: None,
            }),
            span: None,
        };
        let semi = SemiJoinExec::new(left, right, key.clone(), key, Some(residual), false, pool);
        let stream = semi.execute(0).await.expect("execute ok");
        let out_batches = collect_stream(stream).await.expect("collect ok");
        let mut got: Vec<i32> = Vec::new();
        for b in &out_batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 col");
            for r in 0..b.num_rows() {
                got.push(col.value(r));
            }
        }
        got.sort();
        assert_eq!(
            got,
            vec![3, 5, 7],
            "residual+spill SEMI must surface every left row whose key matched in ANY chunk"
        );
    }

    /// Single-batch right side with budget large enough — no spill —
    /// verifies the single-pass code path still works after Phase 2b
    /// refactor.
    #[tokio::test]
    async fn no_spill_path_still_correct() {
        let left = scan_of_batches("L", vec![one_col_batch((1..=5).collect())]);
        let right = scan_of_batches("R", vec![one_col_batch(vec![2, 4, 6])]);
        let pool: Arc<dyn MemoryPool> = Arc::new(crate::memory_pool::UnboundedMemoryPool::new());
        let key = PlanExpr::Column {
            index: 0,
            name: "k".to_string(),
            span: None,
        };
        let semi = SemiJoinExec::new(left, right, key.clone(), key, None, false, pool);
        let stream = semi.execute(0).await.expect("execute ok");
        let out = collect_stream(stream).await.expect("collect ok");
        let mut got: Vec<i32> = Vec::new();
        for b in &out {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32 col");
            for r in 0..b.num_rows() {
                got.push(col.value(r));
            }
        }
        got.sort();
        assert_eq!(got, vec![2, 4]);
    }

    #[tokio::test]
    async fn streaming_probe_matches_collect_semi_no_residual() {
        let (collect, _) = assert_streaming_matches_collect(
            vec![one_col_batch(vec![1, 2, 2, 3, 4])],
            vec![one_col_batch(vec![2, 3])],
            None,
            false,
        )
        .await;
        assert_eq!(collect, vec![2, 2, 3]);
    }

    #[tokio::test]
    async fn streaming_probe_matches_collect_anti_no_residual() {
        let (collect, _) = assert_streaming_matches_collect(
            vec![one_col_batch(vec![1, 2, 2, 3, 4])],
            vec![one_col_batch(vec![2, 3])],
            None,
            true,
        )
        .await;
        assert_eq!(collect, vec![1, 4]);
    }

    #[tokio::test]
    async fn streaming_probe_matches_collect_semi_with_residual() {
        let (collect, _) = assert_streaming_matches_collect(
            vec![one_col_batch(vec![1, 2, 2, 3, 4])],
            vec![one_col_batch(vec![2, 3])],
            Some(eq_residual()),
            false,
        )
        .await;
        assert_eq!(collect, vec![2, 2, 3]);
    }

    #[tokio::test]
    async fn streaming_probe_matches_collect_anti_with_residual() {
        let (collect, _) = assert_streaming_matches_collect(
            vec![one_col_batch(vec![1, 2, 2, 3, 4])],
            vec![one_col_batch(vec![2, 3])],
            Some(eq_residual()),
            true,
        )
        .await;
        assert_eq!(collect, vec![1, 4]);
    }

    #[tokio::test]
    async fn streaming_probe_matches_collect_empty_right() {
        let left = vec![one_col_batch(vec![1, 2, 3])];
        let right: Vec<RecordBatch> = Vec::new();

        let (semi_collect, _) =
            assert_streaming_matches_collect(left.clone(), right.clone(), None, false).await;
        assert!(semi_collect.is_empty());

        let (anti_collect, _) = assert_streaming_matches_collect(left, right, None, true).await;
        assert_eq!(anti_collect, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn streaming_probe_matches_collect_multi_batch_left() {
        let (collect, _) = assert_streaming_matches_collect(
            vec![
                one_col_batch(vec![1, 2]),
                one_col_batch(vec![3, 4]),
                one_col_batch(vec![2, 5]),
            ],
            vec![one_col_batch(vec![2, 4, 5])],
            None,
            false,
        )
        .await;
        assert_eq!(collect, vec![2, 2, 4, 5]);
    }

    fn mark_join_left_small_batches() -> Vec<RecordBatch> {
        vec![one_col_batch_opt(vec![
            None,
            Some(1),
            Some(2),
            Some(2),
            Some(3),
            Some(4),
        ])]
    }

    fn mark_join_right_large_batches() -> Vec<RecordBatch> {
        let mut values: Vec<Option<i32>> = (1000..1090).map(Some).collect();
        values.extend([
            Some(2),
            Some(2),
            Some(3),
            None,
            Some(2000),
            Some(2001),
            Some(2002),
            Some(2003),
            Some(2004),
            Some(2005),
        ]);
        vec![one_col_batch_opt(values)]
    }

    #[tokio::test]
    async fn mark_join_build_left_matches_build_right_semi_no_residual() {
        let got = assert_mark_join_matches_build_right(
            mark_join_left_small_batches(),
            mark_join_right_large_batches(),
            None,
            false,
        )
        .await;
        assert_eq!(got, vec![Some(2), Some(2), Some(3)]);
    }

    #[tokio::test]
    async fn mark_join_build_left_matches_build_right_anti_no_residual() {
        let got = assert_mark_join_matches_build_right(
            mark_join_left_small_batches(),
            mark_join_right_large_batches(),
            None,
            true,
        )
        .await;
        assert_eq!(got, vec![None, Some(1), Some(4)]);
    }

    #[tokio::test]
    async fn mark_join_build_left_matches_build_right_semi_with_residual() {
        let got = assert_mark_join_matches_build_right(
            mark_join_left_small_batches(),
            mark_join_right_large_batches(),
            Some(eq_residual()),
            false,
        )
        .await;
        assert_eq!(got, vec![Some(2), Some(2), Some(3)]);
    }

    #[tokio::test]
    async fn mark_join_build_left_matches_build_right_anti_with_residual() {
        let got = assert_mark_join_matches_build_right(
            mark_join_left_small_batches(),
            mark_join_right_large_batches(),
            Some(eq_residual()),
            true,
        )
        .await;
        assert_eq!(got, vec![None, Some(1), Some(4)]);
    }

    #[tokio::test]
    async fn mark_join_build_left_matches_build_right_empty_left() {
        for anti in [false, true] {
            for residual in [None, Some(eq_residual())] {
                let got = assert_mark_join_matches_build_right(
                    vec![one_col_batch_opt(Vec::new())],
                    mark_join_right_large_batches(),
                    residual,
                    anti,
                )
                .await;
                assert!(got.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn mark_join_build_left_matches_build_right_empty_right() {
        for residual in [None, Some(eq_residual())] {
            let semi = assert_mark_join_matches_build_right(
                mark_join_left_small_batches(),
                vec![one_col_batch_opt(Vec::new())],
                residual.clone(),
                false,
            )
            .await;
            assert!(semi.is_empty());

            let anti = assert_mark_join_matches_build_right(
                mark_join_left_small_batches(),
                vec![one_col_batch_opt(Vec::new())],
                residual,
                true,
            )
            .await;
            assert_eq!(
                anti,
                vec![None, Some(1), Some(2), Some(2), Some(3), Some(4)]
            );
        }
    }

    // =====================================================================
    // ARNEB_PARTITIONED_SEMI_SPILL tests (symmetric grace-partition spill)
    // =====================================================================

    /// Helper: run the SemiJoinExec through the ARNEB_PARTITIONED_SEMI_SPILL
    /// path with a tight memory pool (forces build to spill) and compare
    /// output against the baseline (gate OFF) as a sorted multiset.
    async fn run_partitioned_spill_case(
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        residual: Option<PlanExpr>,
        anti: bool,
        pool_bytes: usize,
    ) -> (Vec<i32>, Vec<i32>) {
        let _guard = ENV_TEST_LOCK.lock().await;

        // Baseline: gate OFF.
        std::env::remove_var("ARNEB_PARTITIONED_SEMI_SPILL");
        let left = scan_of_batches("L", left_batches.clone());
        let right = scan_of_batches("R", right_batches.clone());
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_bytes));
        let key = key_expr();
        let semi_base =
            SemiJoinExec::new(left, right, key.clone(), key, residual.clone(), anti, pool);
        let stream_base = semi_base.execute(0).await.expect("baseline execute ok");
        let out_base = collect_stream(stream_base)
            .await
            .expect("baseline collect ok");
        let baseline = rows_from_batches(&out_base);

        // Partitioned: gate ON.
        std::env::set_var("ARNEB_PARTITIONED_SEMI_SPILL", "1");
        let left2 = scan_of_batches("L", left_batches);
        let right2 = scan_of_batches("R", right_batches);
        let pool2: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_bytes));
        let key2 = key_expr();
        let semi_part = SemiJoinExec::new(left2, right2, key2.clone(), key2, residual, anti, pool2);
        let stream_part = semi_part.execute(0).await.expect("partitioned execute ok");
        let out_part = collect_stream(stream_part)
            .await
            .expect("partitioned collect ok");
        let partitioned = rows_from_batches(&out_part);

        std::env::remove_var("ARNEB_PARTITIONED_SEMI_SPILL");
        (baseline, partitioned)
    }

    /// Semi + no-residual: output multiset must match baseline under spill.
    #[tokio::test]
    async fn partitioned_spill_semi_no_residual_matches_baseline() {
        let left = vec![one_col_batch((1..=10).collect())];
        let mut r2: Vec<i32> = (2000..2100).collect();
        r2.push(3);
        let mut r3: Vec<i32> = (3000..3100).collect();
        r3.push(5);
        let mut r4: Vec<i32> = (4000..4100).collect();
        r4.push(7);
        let right = vec![
            one_col_batch((1000..1100).collect()),
            one_col_batch(r2),
            one_col_batch(r3),
            one_col_batch(r4),
        ];
        let (baseline, partitioned) =
            run_partitioned_spill_case(left, right, None, false, 2048).await;
        assert_eq!(
            baseline, partitioned,
            "partitioned-semi-spill SEMI no-residual must match baseline"
        );
        assert_eq!(baseline, vec![3, 5, 7]);
    }

    /// Anti + no-residual: output multiset must match baseline under spill.
    #[tokio::test]
    async fn partitioned_spill_anti_no_residual_matches_baseline() {
        let left = vec![one_col_batch((1..=10).collect())];
        let mut r2: Vec<i32> = (2000..2100).collect();
        r2.push(3);
        let mut r3: Vec<i32> = (3000..3100).collect();
        r3.push(5);
        let mut r4: Vec<i32> = (4000..4100).collect();
        r4.push(7);
        let right = vec![
            one_col_batch((1000..1100).collect()),
            one_col_batch(r2),
            one_col_batch(r3),
            one_col_batch(r4),
        ];
        let (baseline, partitioned) =
            run_partitioned_spill_case(left, right, None, true, 2048).await;
        assert_eq!(
            baseline, partitioned,
            "partitioned-semi-spill ANTI no-residual must match baseline"
        );
        assert_eq!(baseline, vec![1, 2, 4, 6, 8, 9, 10]);
    }

    /// Semi + residual: partitioned output must match baseline.
    #[tokio::test]
    async fn partitioned_spill_semi_with_residual_matches_baseline() {
        let left = vec![one_col_batch((1..=10).collect())];
        let mut r2: Vec<i32> = (2000..2100).collect();
        r2.push(3);
        let mut r3: Vec<i32> = (3000..3100).collect();
        r3.push(5);
        let mut r4: Vec<i32> = (4000..4100).collect();
        r4.push(7);
        let right = vec![
            one_col_batch((1000..1100).collect()),
            one_col_batch(r2),
            one_col_batch(r3),
            one_col_batch(r4),
        ];
        let residual = Some(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "k".to_string(),
                span: None,
            }),
            op: arneb_sql_parser::ast::BinaryOp::Eq,
            right: Box::new(PlanExpr::Column {
                index: 1,
                name: "k".to_string(),
                span: None,
            }),
            span: None,
        });
        let (baseline, partitioned) =
            run_partitioned_spill_case(left, right, residual, false, 2048).await;
        assert_eq!(
            baseline, partitioned,
            "partitioned-semi-spill SEMI with-residual must match baseline"
        );
        assert_eq!(baseline, vec![3, 5, 7]);
    }

    /// Anti + residual: partitioned output must match baseline.
    #[tokio::test]
    async fn partitioned_spill_anti_with_residual_matches_baseline() {
        let left = vec![one_col_batch((1..=10).collect())];
        let mut r2: Vec<i32> = (2000..2100).collect();
        r2.push(3);
        let mut r3: Vec<i32> = (3000..3100).collect();
        r3.push(5);
        let mut r4: Vec<i32> = (4000..4100).collect();
        r4.push(7);
        let right = vec![
            one_col_batch((1000..1100).collect()),
            one_col_batch(r2),
            one_col_batch(r3),
            one_col_batch(r4),
        ];
        let residual = Some(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "k".to_string(),
                span: None,
            }),
            op: arneb_sql_parser::ast::BinaryOp::Eq,
            right: Box::new(PlanExpr::Column {
                index: 1,
                name: "k".to_string(),
                span: None,
            }),
            span: None,
        });
        let (baseline, partitioned) =
            run_partitioned_spill_case(left, right, residual, true, 2048).await;
        assert_eq!(
            baseline, partitioned,
            "partitioned-semi-spill ANTI with-residual must match baseline"
        );
        assert_eq!(baseline, vec![1, 2, 4, 6, 8, 9, 10]);
    }

    /// Empty build: SEMI emits nothing, ANTI emits all left rows.
    #[tokio::test]
    async fn partitioned_spill_empty_build() {
        let _guard = ENV_TEST_LOCK.lock().await;
        std::env::set_var("ARNEB_PARTITIONED_SEMI_SPILL", "1");

        // SEMI + empty right → empty output.
        let left = scan_of_batches("L", vec![one_col_batch((1..=5).collect())]);
        let right = scan_of_batches("R", vec![]);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2048));
        let key = key_expr();
        let semi = SemiJoinExec::new(left, right, key.clone(), key, None, false, pool);
        // This won't trigger spill (empty right), but gate is ON → exercises empty-build branch.
        let stream = semi.execute(0).await.expect("execute ok");
        let out = collect_stream(stream).await.expect("collect ok");
        assert!(
            rows_from_batches(&out).is_empty(),
            "SEMI empty build → empty output"
        );

        // ANTI + empty right → emit all left rows.
        let left2 = scan_of_batches("L", vec![one_col_batch((1..=5).collect())]);
        let right2 = scan_of_batches("R", vec![]);
        let pool2: Arc<dyn MemoryPool> = Arc::new(crate::memory_pool::UnboundedMemoryPool::new());
        let key2 = key_expr();
        let anti = SemiJoinExec::new(left2, right2, key2.clone(), key2, None, true, pool2);
        let stream2 = anti.execute(0).await.expect("execute ok");
        let out2 = collect_stream(stream2).await.expect("collect ok");
        assert_eq!(
            rows_from_batches(&out2),
            vec![1, 2, 3, 4, 5],
            "ANTI empty build → emit all left"
        );

        std::env::remove_var("ARNEB_PARTITIONED_SEMI_SPILL");
    }

    /// Empty probe: no output in any case.
    #[tokio::test]
    async fn partitioned_spill_empty_probe() {
        let _guard = ENV_TEST_LOCK.lock().await;
        std::env::set_var("ARNEB_PARTITIONED_SEMI_SPILL", "1");

        for anti in [false, true] {
            let left = scan_of_batches("L", vec![]);
            let right = scan_of_batches(
                "R",
                vec![
                    one_col_batch((1000..1100).collect()),
                    one_col_batch((2000..2100).collect()),
                ],
            );
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2048));
            let key = key_expr();
            let semi = SemiJoinExec::new(left, right, key.clone(), key, None, anti, pool);
            let stream = semi.execute(0).await.expect("execute ok");
            let out = collect_stream(stream).await.expect("collect ok");
            assert!(
                rows_from_batches(&out).is_empty(),
                "empty probe → no output (anti={anti})"
            );
        }

        std::env::remove_var("ARNEB_PARTITIONED_SEMI_SPILL");
    }

    /// NULL join keys: SEMI drops them, ANTI emits them — matches baseline.
    #[tokio::test]
    async fn partitioned_spill_null_keys_match_baseline() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};

        let _guard = ENV_TEST_LOCK.lock().await;

        // Build batches with nullable INT32 schema.
        fn nullable_batch(values: &[Option<i32>]) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "k",
                ArrowDataType::Int32,
                true, // nullable
            )]));
            let arr: Int32Array = values.iter().copied().collect();
            RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
        }

        fn scan_nullable(name: &str, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
            let source: Arc<dyn crate::datasource::DataSource> =
                Arc::new(crate::datasource::InMemoryDataSource::new(
                    vec![ColumnInfo {
                        name: "k".to_string(),
                        data_type: DataType::Int32,
                        nullable: true,
                    }],
                    batches,
                ));
            Arc::new(ScanExec {
                source,
                _table_name: name.to_string(),
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

        fn collect_nullable_rows(out: &[RecordBatch]) -> (Vec<i32>, usize) {
            let mut values = Vec::new();
            let mut null_count = 0;
            for b in out {
                let col = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..b.num_rows() {
                    if col.is_null(i) {
                        null_count += 1;
                    } else {
                        values.push(col.value(i));
                    }
                }
            }
            values.sort();
            (values, null_count)
        }

        // Left: 1, 2, NULL, 4, NULL, 6. Right: spilled — 2 and 4 appear across chunks.
        let left_batch = {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "k",
                ArrowDataType::Int32,
                true,
            )]));
            let arr: Int32Array = vec![Some(1), Some(2), None, Some(4), None, Some(6)]
                .into_iter()
                .collect();
            RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap()
        };
        let r1 = nullable_batch(&[Some(1000), Some(1001), Some(2)]);
        let r2 = nullable_batch(&[Some(2000), Some(2001), Some(4)]);

        for anti in [false, true] {
            // Baseline (gate OFF).
            std::env::remove_var("ARNEB_PARTITIONED_SEMI_SPILL");
            let left_b = scan_nullable("L", vec![left_batch.clone()]);
            let right_b = scan_nullable("R", vec![r1.clone(), r2.clone()]);
            let pool_b: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(512));
            let key_b = key_expr();
            let semi_b =
                SemiJoinExec::new(left_b, right_b, key_b.clone(), key_b, None, anti, pool_b);
            let stream_b = semi_b.execute(0).await.expect("baseline execute ok");
            let out_b = collect_stream(stream_b).await.expect("baseline collect ok");
            let (baseline_vals, baseline_nulls) = collect_nullable_rows(&out_b);

            // Partitioned (gate ON).
            std::env::set_var("ARNEB_PARTITIONED_SEMI_SPILL", "1");
            let left_p = scan_nullable("L", vec![left_batch.clone()]);
            let right_p = scan_nullable("R", vec![r1.clone(), r2.clone()]);
            let pool_p: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(512));
            let key_p = key_expr();
            let semi_p =
                SemiJoinExec::new(left_p, right_p, key_p.clone(), key_p, None, anti, pool_p);
            let stream_p = semi_p.execute(0).await.expect("partitioned execute ok");
            let out_p = collect_stream(stream_p)
                .await
                .expect("partitioned collect ok");
            let (part_vals, part_nulls) = collect_nullable_rows(&out_p);

            assert_eq!(
                baseline_vals, part_vals,
                "NULL-key test: non-null output values mismatch (anti={anti})"
            );
            assert_eq!(
                baseline_nulls, part_nulls,
                "NULL-key test: null output row count mismatch (anti={anti})"
            );
        }

        std::env::remove_var("ARNEB_PARTITIONED_SEMI_SPILL");
    }

    /// Keys spanning multiple partitions: dense left 1..50, right contains
    /// even numbers only. Both SEMI and ANTI must match baseline.
    #[tokio::test]
    async fn partitioned_spill_multi_bucket_dense_keys_match_baseline() {
        let left: Vec<RecordBatch> = (0..5)
            .map(|i| one_col_batch(((i * 10 + 1)..=(i * 10 + 10)).collect()))
            .collect();
        // Right: three spill-forcing batches.
        let right: Vec<RecordBatch> = vec![
            one_col_batch((2..=20).filter(|x| x % 2 == 0).collect()),
            one_col_batch((22..=40).filter(|x| x % 2 == 0).collect()),
            one_col_batch((42..=50).filter(|x| x % 2 == 0).collect()),
        ];

        // Use a generous pool for baseline (multipass), tight for partitioned.
        // We compare sorted multisets: correctness, not performance.
        for anti in [false, true] {
            let _guard = ENV_TEST_LOCK.lock().await;
            std::env::remove_var("ARNEB_PARTITIONED_SEMI_SPILL");
            let left_b = scan_of_batches("L", left.clone());
            let right_b = scan_of_batches("R", right.clone());
            let pool_b: Arc<dyn MemoryPool> =
                Arc::new(crate::memory_pool::UnboundedMemoryPool::new());
            let key_b = key_expr();
            let semi_b =
                SemiJoinExec::new(left_b, right_b, key_b.clone(), key_b, None, anti, pool_b);
            let stream_b = semi_b.execute(0).await.expect("baseline execute ok");
            let out_b = collect_stream(stream_b).await.expect("baseline collect ok");
            let baseline = rows_from_batches(&out_b);

            std::env::set_var("ARNEB_PARTITIONED_SEMI_SPILL", "1");
            let left_p = scan_of_batches("L", left.clone());
            let right_p = scan_of_batches("R", right.clone());
            // Generous pool so left collect doesn't fail; force spill via tiny chunk cap.
            let pool_p: Arc<dyn MemoryPool> =
                Arc::new(crate::memory_pool::UnboundedMemoryPool::new());
            let key_p = key_expr();
            let semi_p =
                SemiJoinExec::new(left_p, right_p, key_p.clone(), key_p, None, anti, pool_p)
                    .with_spill_chunk_bytes(16); // tiny cap → forces multi-chunk spill
            let stream_p = semi_p.execute(0).await.expect("partitioned execute ok");
            let out_p = collect_stream(stream_p)
                .await
                .expect("partitioned collect ok");
            let partitioned = rows_from_batches(&out_p);

            std::env::remove_var("ARNEB_PARTITIONED_SEMI_SPILL");
            assert_eq!(
                baseline, partitioned,
                "multi-bucket dense keys mismatch (anti={anti})"
            );
        }
    }
}
