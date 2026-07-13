//! On-disk spill files for spillable operators.
//!
//! Wraps Arrow IPC stream format so a spillable operator can write
//! `RecordBatch`es to a temp file and read them back later. Inspired
//! by DataFusion's `InProgressSpillFile` + `IPCStreamWriter`
//! (`physical-plan/src/spill/in_progress_spill_file.rs`, Apache-2.0).
//!
//! Lifecycle:
//!   1. `SpillWriter::new(schema)` — creates a unique temp file and
//!      opens an IPC stream writer.
//!   2. `writer.write(batch)` — append, repeat as needed.
//!   3. `writer.finish()` — flush IPC footer, return a `SpillFile`
//!      handle. The file persists until the `SpillFile` is dropped.
//!   4. `spill_file.open_reader()?` — stream batches back as an
//!      iterator. Multiple readers can be opened sequentially.
//!   5. Drop the `SpillFile` — the underlying file is removed.
//!
//! Differences from DataFusion (intentional):
//!   - No `view-array GC` compaction pre-write. arneb's batches don't
//!     use StringView types extensively; the cost of compaction
//!     outweighs the savings for our workload.
//!   - No streaming k-way merge — this is purely a write-then-read
//!     buffer. Higher-level operators (SemiJoinExec, HashJoinExec)
//!     compose multiple `SpillFile`s for their algorithms.

use std::fs::File;
use std::io::{BufReader, BufWriter};
#[cfg(all(unix, target_os = "linux"))]
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use arneb_common::error::ExecutionError;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::CompressionType;

fn spill_compression_codec() -> Option<CompressionType> {
    static CODEC: OnceLock<Option<CompressionType>> = OnceLock::new();
    *CODEC.get_or_init(|| {
        let codec = match std::env::var("ARNEB_SPILL_COMPRESSION").as_deref() {
            Ok("lz4" | "1" | "true") => Some(CompressionType::LZ4_FRAME),
            Ok("zstd") => Some(CompressionType::ZSTD),
            _ => None,
        };
        let effective = match codec {
            Some(CompressionType::LZ4_FRAME) => "lz4",
            Some(CompressionType::ZSTD) => "zstd",
            _ => "off",
        };
        tracing::info!(
            target: "arneb::config",
            ARNEB_SPILL_COMPRESSION = effective,
            "ARNEB_SPILL_COMPRESSION effective value (default off; lz4/1/true or zstd)"
        );
        codec
    })
}

fn spill_fadvise_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_SPILL_FADVISE").is_ok_and(|v| v == "1");
        tracing::info!(
            target: "arneb::config",
            ARNEB_SPILL_FADVISE = enabled,
            "ARNEB_SPILL_FADVISE effective value (default off; =1 to evict spill-file page cache with POSIX_FADV_DONTNEED)"
        );
        enabled
    })
}

fn spill_fadvise_buildwrite_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_SPILL_FADVISE_BUILDWRITE").is_ok_and(|v| v == "1");
        let state = if enabled { "on" } else { "off" };
        tracing::info!(
            target: "arneb::config",
            "ARNEB_SPILL_FADVISE_BUILDWRITE={state}"
        );
        enabled
    })
}

#[cfg(unix)]
fn drop_page_cache(file: &File) {
    if let Err(e) = file.sync_data() {
        tracing::debug!("spill: sync_data before page-cache eviction failed: {e}");
    }

    #[cfg(target_os = "linux")]
    {
        // SAFETY: posix_fadvise is a kernel advisory hint over a valid open fd
        // (the spill File we own); it touches no Rust memory and cannot violate
        // aliasing. The crate denies unsafe by default; this single FFI call is
        // the only exception, scoped to the Linux spill page-cache eviction.
        #[allow(unsafe_code)]
        let ret = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        if ret != 0 {
            tracing::debug!(
                "spill: POSIX_FADV_DONTNEED failed: {}",
                std::io::Error::from_raw_os_error(ret)
            );
        }
    }
}

/// Generate a unique temp-file path under `/tmp` (or the platform's
/// equivalent). Format: `arneb-<prefix>-<pid>-<counter>.arrow`. The
/// counter is process-local; combined with the pid it's safe across
/// concurrent processes too.
fn unique_spill_path(prefix: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let mut path = std::env::temp_dir();
    path.push(format!("arneb-{prefix}-{pid}-{n}.arrow"));
    path
}

/// Append-only writer over a temp Arrow IPC stream file.
///
/// Created via `SpillWriter::new(schema, prefix)`. Call `write(batch)`
/// repeatedly, then `finish()` to seal and obtain a [`SpillFile`].
pub struct SpillWriter {
    path: PathBuf,
    writer: Option<StreamWriter<BufWriter<File>>>,
    schema: SchemaRef,
    bytes_written: usize,
    num_batches: usize,
    /// Whether `Drop` should remove the temp file. Set to `false` by
    /// `finish()` so the resulting `SpillFile` owns the cleanup.
    cleanup_on_drop: bool,
}

impl SpillWriter {
    /// Create a new spill file for batches with the given schema.
    /// `prefix` is included in the filename to ease debugging
    /// (e.g. `"semi_join_build_part3"`).
    pub fn new(schema: SchemaRef, prefix: &str) -> Result<Self, ExecutionError> {
        Self::new_with_compression(schema, prefix, spill_compression_codec())
    }

    fn new_with_compression(
        schema: SchemaRef,
        prefix: &str,
        compression: Option<CompressionType>,
    ) -> Result<Self, ExecutionError> {
        let path = unique_spill_path(prefix);
        let file = File::create(&path).map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "spill: failed to create temp file {}: {e}",
                path.display()
            ))
        })?;
        let writer = match compression {
            Some(codec) => IpcWriteOptions::default()
                .try_with_compression(Some(codec))
                .and_then(|options| {
                    StreamWriter::try_new_with_options(BufWriter::new(file), &schema, options)
                }),
            None => StreamWriter::try_new(BufWriter::new(file), &schema),
        }
        .map_err(|e| {
            ExecutionError::InvalidOperation(format!("spill: arrow IPC writer failed: {e}"))
        })?;
        Ok(Self {
            path,
            writer: Some(writer),
            schema,
            bytes_written: 0,
            num_batches: 0,
            cleanup_on_drop: true,
        })
    }

    /// Append `batch` to the spill file. Returns the on-wire bytes
    /// estimate so the caller can update its memory accounting.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<usize, ExecutionError> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            ExecutionError::InvalidOperation("spill: write after finish".to_string())
        })?;
        writer.write(batch).map_err(|e| {
            ExecutionError::InvalidOperation(format!("spill: arrow IPC write failed: {e}"))
        })?;
        // Approximate on-wire bytes via in-memory batch size. Arrow IPC
        // streams without compression are within ~5% of in-memory.
        let size = batch_in_memory_bytes(batch);
        self.bytes_written += size;
        self.num_batches += 1;
        Ok(size)
    }

    /// Flush the IPC footer and return a sealed [`SpillFile`] handle.
    /// The temp file's cleanup responsibility transfers to the
    /// returned `SpillFile`.
    pub fn finish(mut self) -> Result<SpillFile, ExecutionError> {
        let mut writer = self.writer.take().ok_or_else(|| {
            ExecutionError::InvalidOperation("spill: finish called twice".to_string())
        })?;
        writer.finish().map_err(|e| {
            ExecutionError::InvalidOperation(format!("spill: arrow IPC finish failed: {e}"))
        })?;
        let spill_fadvise = spill_fadvise_enabled();
        let spill_fadvise_buildwrite = spill_fadvise_buildwrite_enabled();
        if spill_fadvise || spill_fadvise_buildwrite {
            match writer.into_inner() {
                Ok(buf_writer) => match buf_writer.into_inner() {
                    Ok(file) => {
                        #[cfg(unix)]
                        {
                            if spill_fadvise {
                                drop_page_cache(&file);
                            }
                            if spill_fadvise_buildwrite {
                                let filename =
                                    self.path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                                if !filename.contains("probe") {
                                    drop_page_cache(&file);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!("spill: file unwrap before page-cache eviction failed: {e}")
                    }
                },
                Err(e) => {
                    tracing::debug!(
                        "spill: IPC writer unwrap before page-cache eviction failed: {e}"
                    )
                }
            }
        }
        // Hand cleanup off to SpillFile so Drop here is a no-op.
        self.cleanup_on_drop = false;
        // `std::mem::take` swaps in a default-empty PathBuf — safe
        // because `Drop` won't touch it (cleanup_on_drop is false).
        let path = std::mem::take(&mut self.path);
        Ok(SpillFile {
            path,
            schema: self.schema.clone(),
            bytes_written: self.bytes_written,
            num_batches: self.num_batches,
        })
    }

    /// Bytes written so far (in-memory estimate; on-wire is similar).
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    /// Number of batches written so far.
    pub fn num_batches(&self) -> usize {
        self.num_batches
    }
}

impl Drop for SpillWriter {
    fn drop(&mut self) {
        // If `finish()` wasn't called the file may be truncated; remove
        // it so we don't leave junk in /tmp. After `finish()` the
        // `SpillFile` owns the cleanup, so we skip.
        if self.cleanup_on_drop {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// A sealed spill file handle. Open a reader to stream the batches
/// back. The underlying file is removed when this handle is dropped.
#[derive(Debug)]
pub struct SpillFile {
    path: PathBuf,
    schema: SchemaRef,
    bytes_written: usize,
    num_batches: usize,
}

impl SpillFile {
    /// Open a reader over this file's batches. Multiple readers can be
    /// opened sequentially (each starts at the beginning).
    pub fn open_reader(&self) -> Result<SpillReader, ExecutionError> {
        let file = File::open(&self.path).map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "spill: failed to open temp file {}: {e}",
                self.path.display()
            ))
        })?;
        let reader = StreamReader::try_new(BufReader::new(file), None).map_err(|e| {
            ExecutionError::InvalidOperation(format!("spill: arrow IPC reader failed: {e}"))
        })?;
        Ok(SpillReader {
            reader,
            page_cache_dropped: false,
        })
    }

    /// Schema of the spilled batches.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Approximate bytes written.
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    /// Number of batches in the file.
    pub fn num_batches(&self) -> usize {
        self.num_batches
    }

    /// On-disk path (for logging/debugging).
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Best-effort page-cache eviction for this sealed spill file.
    ///
    /// Linux uses POSIX_FADV_DONTNEED. Other platforms intentionally no-op.
    /// Failures are diagnostic only; this is a cache hint and must not affect
    /// query correctness.
    pub fn evict_page_cache(&self) {
        #[cfg(target_os = "linux")]
        {
            match File::open(&self.path) {
                Ok(file) => drop_page_cache(&file),
                Err(e) => tracing::debug!(
                    "spill: failed to open {} for page-cache eviction: {e}",
                    self.path.display()
                ),
            }
        }
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Iterator over the batches in a [`SpillFile`].
pub struct SpillReader {
    reader: StreamReader<BufReader<File>>,
    page_cache_dropped: bool,
}

impl SpillReader {
    fn drop_page_cache_once(&mut self) {
        if self.page_cache_dropped || !spill_fadvise_enabled() {
            return;
        }
        self.page_cache_dropped = true;
        #[cfg(unix)]
        drop_page_cache(self.reader.get_ref().get_ref());
    }
}

impl Iterator for SpillReader {
    type Item = Result<RecordBatch, ExecutionError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next() {
            Some(r) => Some(r.map_err(|e| {
                ExecutionError::InvalidOperation(format!("spill: arrow IPC read failed: {e}"))
            })),
            None => {
                self.drop_page_cache_once();
                None
            }
        }
    }
}

impl Drop for SpillReader {
    fn drop(&mut self) {
        self.drop_page_cache_once();
    }
}

/// In-memory byte size estimate for a [`RecordBatch`]. Sums each
/// column's buffer bytes (plus null buffer if present). Used to
/// account for spill bytes written and to drive the budget threshold.
fn batch_in_memory_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|col| col.get_array_memory_size())
        .sum()
}

// ===========================================================================
// PartitionedSpillWriter / PartitionedSpillFile (Phase 3b.5b, 2026-05-21)
// ===========================================================================
//
// Grace Hash Join needs to spill the build side AND probe side into the
// SAME partition layout so Pass 2 can pair them up. `SpillWriter` is a
// single stream; this is `N` of them indexed by partition id, lazily
// created on first write so empty partitions cost nothing.

/// Append-only writer over `N` per-partition Arrow IPC stream files.
/// Each `write_partition(p, batch)` lazily opens a `SpillWriter` for
/// partition `p` if it hasn't been written to yet; empty partitions
/// never touch disk.
///
/// Used by Grace Hash Join to spill the build side and the probe side
/// into the same partition layout so Pass 2 can match `(build_p,
/// probe_p)` pairs without rescanning.
pub struct PartitionedSpillWriter {
    /// One slot per partition; `None` until first `write_partition` for
    /// that partition opens the underlying writer.
    writers: Vec<Option<SpillWriter>>,
    schema: SchemaRef,
    prefix: String,
    /// Total bytes written across all partition files (sum of each
    /// inner `SpillWriter::bytes_written`).
    bytes_written: usize,
}

impl PartitionedSpillWriter {
    /// Create a fresh partitioned writer with `n_partitions` lazy slots.
    /// `prefix` is included in each per-partition filename to ease
    /// debugging (e.g. `"hash_join_grace_build"` → files like
    /// `arneb-hash_join_grace_build-{pid}-{counter}.arrow` per partition).
    pub fn new(schema: SchemaRef, n_partitions: usize, prefix: &str) -> Self {
        let mut writers = Vec::with_capacity(n_partitions);
        for _ in 0..n_partitions {
            writers.push(None);
        }
        Self {
            writers,
            schema,
            prefix: prefix.to_string(),
            bytes_written: 0,
        }
    }

    /// Number of partition slots (whether or not each has been written).
    pub fn n_partitions(&self) -> usize {
        self.writers.len()
    }

    /// Append `batch` to partition `p`'s spill file. Lazily opens the
    /// underlying `SpillWriter` on first call. No-op if `batch` has
    /// zero rows.
    pub fn write_partition(
        &mut self,
        p: usize,
        batch: &RecordBatch,
    ) -> Result<usize, ExecutionError> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        if p >= self.writers.len() {
            return Err(ExecutionError::InvalidOperation(format!(
                "partitioned spill: partition {p} out of range (have {})",
                self.writers.len()
            )));
        }
        if self.writers[p].is_none() {
            let per_partition_prefix = format!("{}_p{p}", self.prefix);
            self.writers[p] = Some(SpillWriter::new(
                self.schema.clone(),
                &per_partition_prefix,
            )?);
        }
        let written = self.writers[p].as_mut().unwrap().write(batch)?;
        self.bytes_written += written;
        Ok(written)
    }

    /// Flush every open partition writer and seal them as
    /// [`SpillFile`]s, returning a [`PartitionedSpillFile`] handle.
    /// Partitions that were never written to remain `None` in the
    /// resulting `files` slot — Pass 2 should skip them.
    pub fn finish(self) -> Result<PartitionedSpillFile, ExecutionError> {
        let n = self.writers.len();
        let mut files: Vec<Option<SpillFile>> = Vec::with_capacity(n);
        for writer_slot in self.writers {
            match writer_slot {
                Some(w) => files.push(Some(w.finish()?)),
                None => files.push(None),
            }
        }
        Ok(PartitionedSpillFile {
            files,
            schema: self.schema,
        })
    }

    /// Bytes written across all partition files so far.
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }
}

/// `N` per-partition sealed spill files. Each slot is either a
/// `SpillFile` or `None` if that partition received no writes.
#[derive(Debug)]
pub struct PartitionedSpillFile {
    files: Vec<Option<SpillFile>>,
    schema: SchemaRef,
}

impl PartitionedSpillFile {
    /// Borrow the `SpillFile` for partition `p`, or `None` if that
    /// partition was empty.
    pub fn partition(&self, p: usize) -> Option<&SpillFile> {
        self.files.get(p).and_then(|s| s.as_ref())
    }

    /// Take ownership of partition `p`'s file (leaving `None` in the
    /// slot). Used in Pass 2 so the file is dropped (and its on-disk
    /// path removed) as soon as that partition is processed.
    pub fn take_partition(&mut self, p: usize) -> Option<SpillFile> {
        self.files.get_mut(p).and_then(|s| s.take())
    }

    /// Number of partition slots (matches the writer's count).
    pub fn n_partitions(&self) -> usize {
        self.files.len()
    }

    /// True if partition `p` was written to at least once.
    pub fn has_partition(&self, p: usize) -> bool {
        self.files.get(p).map(|s| s.is_some()).unwrap_or(false)
    }

    /// Shared schema for every partition's spilled batches.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Sum of `bytes_written` across all non-empty partitions.
    pub fn total_bytes(&self) -> usize {
        self.files
            .iter()
            .filter_map(|s| s.as_ref().map(|f| f.bytes_written()))
            .sum()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema_int_str() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batch_int_str(ids: &[i32], names: &[Option<&str>]) -> RecordBatch {
        let schema = schema_int_str();
        let id_arr = Int32Array::from(ids.to_vec());
        let name_arr = StringArray::from(names.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(id_arr), Arc::new(name_arr)]).unwrap()
    }

    fn assert_batches_equal_field_by_field(expected: &[RecordBatch], actual: &[RecordBatch]) {
        assert_eq!(actual.len(), expected.len());
        for (expected_batch, actual_batch) in expected.iter().zip(actual) {
            assert_eq!(actual_batch.schema(), expected_batch.schema());
            assert_eq!(actual_batch.num_rows(), expected_batch.num_rows());
            assert_eq!(actual_batch.num_columns(), expected_batch.num_columns());
            for column_index in 0..expected_batch.num_columns() {
                assert_eq!(
                    actual_batch.column(column_index).to_data(),
                    expected_batch.column(column_index).to_data()
                );
            }
        }
    }

    #[test]
    fn test_spill_compression_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batches = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")])),
                    Arc::new(Float64Array::from(vec![Some(1.5), Some(-2.0), None])),
                    Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![4, 5])),
                    Arc::new(StringArray::from(vec![Some("delta"), Some("")])),
                    Arc::new(Float64Array::from(vec![Some(0.0), Some(f64::MAX)])),
                    Arc::new(BooleanArray::from(vec![Some(false), Some(true)])),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![6])),
                    Arc::new(StringArray::from(vec![None::<&str>])),
                    Arc::new(Float64Array::from(vec![Some(f64::MIN)])),
                    Arc::new(BooleanArray::from(vec![None])),
                ],
            )
            .unwrap(),
        ];

        for (prefix, compression) in [
            ("test_compression_lz4", Some(CompressionType::LZ4_FRAME)),
            ("test_compression_off", None),
        ] {
            let mut writer =
                SpillWriter::new_with_compression(schema.clone(), prefix, compression).unwrap();
            for batch in &batches {
                writer.write(batch).unwrap();
            }
            let file = writer.finish().unwrap();
            let actual: Vec<_> = file
                .open_reader()
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            assert_batches_equal_field_by_field(&batches, &actual);
        }
    }

    #[test]
    fn write_then_read_single_batch() {
        let schema = schema_int_str();
        let mut writer = SpillWriter::new(schema.clone(), "test").unwrap();
        let batch = batch_int_str(&[1, 2, 3], &[Some("a"), Some("b"), None]);
        writer.write(&batch).unwrap();
        let file = writer.finish().unwrap();
        assert_eq!(file.num_batches(), 1);

        let mut reader = file.open_reader().unwrap();
        let got = reader.next().unwrap().unwrap();
        assert_eq!(got.num_rows(), 3);
        assert_eq!(got.schema(), schema);
        assert!(reader.next().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn page_cache_drop_round_trips_batch() {
        let schema = schema_int_str();
        let batch = batch_int_str(&[7, 8, 9], &[Some("x"), None, Some("z")]);
        let mut writer = SpillWriter::new(schema, "test_fadvise").unwrap();
        writer.write(&batch).unwrap();
        let file = writer.finish().unwrap();

        let write_file = File::open(file.path()).unwrap();
        drop_page_cache(&write_file);

        let mut reader = file.open_reader().unwrap();
        let got = reader.next().unwrap().unwrap();
        assert_eq!(got, batch);
        assert!(reader.next().is_none());

        let read_file = File::open(file.path()).unwrap();
        drop_page_cache(&read_file);
    }

    #[test]
    fn spill_file_evict_after_read_round_trips_batch() {
        let schema = schema_int_str();
        let batch = batch_int_str(&[11, 12, 13], &[Some("aa"), None, Some("cc")]);
        let mut writer = SpillWriter::new(schema, "test_build_only_fadvise").unwrap();
        writer.write(&batch).unwrap();
        let file = writer.finish().unwrap();

        let mut reader = file.open_reader().unwrap();
        let got = reader.next().unwrap().unwrap();
        assert_eq!(got, batch);
        assert!(reader.next().is_none());

        file.evict_page_cache();
    }

    #[test]
    fn test_spill_fadvise_buildwrite_roundtrip() {
        if std::env::var_os("ARNEB_SPILL_FADVISE_BUILDWRITE").is_none() {
            std::env::set_var("ARNEB_SPILL_FADVISE_BUILDWRITE", "1");
        }

        let schema = schema_int_str();
        let build_batch = batch_int_str(&[21, 22, 23], &[Some("build-a"), None, Some("build-c")]);
        let probe_batch = batch_int_str(&[31, 32], &[Some("probe-a"), Some("probe-b")]);

        let mut build_writer = SpillWriter::new(schema.clone(), "build_chunk_0").unwrap();
        build_writer.write(&build_batch).unwrap();
        let build_file = build_writer.finish().unwrap();

        let mut probe_writer = SpillWriter::new(schema, "semijoin_probe_0").unwrap();
        probe_writer.write(&probe_batch).unwrap();
        let probe_file = probe_writer.finish().unwrap();

        let build_batches: Vec<_> = build_file
            .open_reader()
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let probe_batches: Vec<_> = probe_file
            .open_reader()
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(build_batches, vec![build_batch]);
        assert_eq!(probe_batches, vec![probe_batch]);
    }

    #[test]
    fn write_then_read_multi_batch() {
        let schema = schema_int_str();
        let mut writer = SpillWriter::new(schema, "test").unwrap();
        for i in 0..5 {
            let b = batch_int_str(&[i, i + 1], &[Some("x"), None]);
            writer.write(&b).unwrap();
        }
        let file = writer.finish().unwrap();
        assert_eq!(file.num_batches(), 5);

        let reader = file.open_reader().unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 5);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 10);
    }

    #[test]
    fn spill_file_removed_on_drop() {
        let schema = schema_int_str();
        let mut writer = SpillWriter::new(schema, "test").unwrap();
        writer.write(&batch_int_str(&[1], &[Some("a")])).unwrap();
        let file = writer.finish().unwrap();
        let path = file.path().clone();
        assert!(path.exists());
        drop(file);
        assert!(!path.exists(), "spill file must be removed on Drop");
    }

    #[test]
    fn writer_drop_without_finish_removes_file() {
        let schema = schema_int_str();
        let writer = SpillWriter::new(schema, "test").unwrap();
        let path = writer.path.clone();
        assert!(path.exists());
        drop(writer);
        assert!(!path.exists(), "writer Drop without finish must clean up");
    }

    #[test]
    fn bytes_written_increments() {
        let schema = schema_int_str();
        let mut writer = SpillWriter::new(schema, "test").unwrap();
        let batch = batch_int_str(&[1, 2, 3, 4, 5], &[Some("aa"); 5]);
        let written = writer.write(&batch).unwrap();
        assert!(written > 0);
        assert_eq!(writer.bytes_written(), written);
    }

    #[test]
    fn second_reader_starts_from_beginning() {
        let schema = schema_int_str();
        let mut writer = SpillWriter::new(schema, "test").unwrap();
        writer
            .write(&batch_int_str(&[1, 2], &[Some("x"), Some("y")]))
            .unwrap();
        let file = writer.finish().unwrap();

        let r1: Vec<_> = file
            .open_reader()
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let r2: Vec<_> = file
            .open_reader()
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
        assert_eq!(r1[0].num_rows(), r2[0].num_rows());
    }

    #[test]
    fn unique_paths_distinct_for_each_call() {
        let p1 = unique_spill_path("test");
        let p2 = unique_spill_path("test");
        assert_ne!(p1, p2);
    }

    // -------------------------------------------------------------------
    // PartitionedSpillWriter / PartitionedSpillFile (Phase 3b.5b)
    // -------------------------------------------------------------------

    #[test]
    fn partitioned_writer_lazily_opens_files() {
        let schema = schema_int_str();
        let mut w = PartitionedSpillWriter::new(schema.clone(), 4, "test_grace");
        // Only partition 1 and 3 see writes; 0 and 2 stay empty.
        w.write_partition(1, &batch_int_str(&[10, 11], &[Some("a"), None]))
            .unwrap();
        w.write_partition(
            3,
            &batch_int_str(&[30, 31, 32], &[Some("c"), Some("d"), None]),
        )
        .unwrap();
        let file = w.finish().unwrap();
        assert!(!file.has_partition(0));
        assert!(file.has_partition(1));
        assert!(!file.has_partition(2));
        assert!(file.has_partition(3));
        assert_eq!(file.n_partitions(), 4);
    }

    #[test]
    fn partitioned_writer_writes_zero_row_batches_as_noop() {
        let schema = schema_int_str();
        let mut w = PartitionedSpillWriter::new(schema.clone(), 2, "test_grace_noop");
        let empty_batch = RecordBatch::new_empty(schema);
        let n = w.write_partition(0, &empty_batch).unwrap();
        assert_eq!(n, 0);
        let file = w.finish().unwrap();
        assert!(
            !file.has_partition(0),
            "zero-row batch must not open a file"
        );
    }

    #[test]
    fn partitioned_writer_roundtrip_per_partition() {
        let schema = schema_int_str();
        let mut w = PartitionedSpillWriter::new(schema.clone(), 3, "test_grace_rt");
        w.write_partition(0, &batch_int_str(&[1, 2], &[Some("a"), Some("b")]))
            .unwrap();
        w.write_partition(
            2,
            &batch_int_str(&[20, 21, 22], &[None, Some("c"), Some("d")]),
        )
        .unwrap();
        w.write_partition(0, &batch_int_str(&[3], &[Some("e")]))
            .unwrap();
        let file = w.finish().unwrap();

        // Partition 0 should have 2 batches (3 rows total).
        let p0 = file.partition(0).expect("partition 0 should exist");
        let p0_batches: Vec<_> = p0.open_reader().unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(p0_batches.len(), 2);
        assert_eq!(p0_batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3,);

        // Partition 1 should be absent.
        assert!(file.partition(1).is_none());

        // Partition 2 should have 1 batch (3 rows).
        let p2 = file.partition(2).expect("partition 2 should exist");
        let p2_batches: Vec<_> = p2.open_reader().unwrap().collect::<Result<_, _>>().unwrap();
        assert_eq!(p2_batches.len(), 1);
        assert_eq!(p2_batches[0].num_rows(), 3);
    }

    #[test]
    fn partitioned_writer_rejects_out_of_range_partition() {
        let schema = schema_int_str();
        let mut w = PartitionedSpillWriter::new(schema, 2, "test_grace_oob");
        let result = w.write_partition(5, &batch_int_str(&[1], &[Some("a")]));
        assert!(result.is_err());
    }

    #[test]
    fn partitioned_file_take_partition_yields_ownership() {
        let schema = schema_int_str();
        let mut w = PartitionedSpillWriter::new(schema, 2, "test_grace_take");
        w.write_partition(0, &batch_int_str(&[1], &[Some("a")]))
            .unwrap();
        w.write_partition(1, &batch_int_str(&[2], &[Some("b")]))
            .unwrap();
        let mut file = w.finish().unwrap();

        let p0 = file.take_partition(0).expect("partition 0 must take");
        let path = p0.path().clone();
        assert!(path.exists());
        // Dropping the taken SpillFile must remove the on-disk file.
        drop(p0);
        assert!(!path.exists());
        // After take, partition 0 is gone but partition 1 stays.
        assert!(!file.has_partition(0));
        assert!(file.has_partition(1));
    }
}
