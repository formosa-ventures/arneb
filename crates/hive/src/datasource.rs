//! Hive data source and connector factory.
//!
//! [`HiveDataSource`] reads the Parquet or ORC data files of a Hive table
//! from an object store location (as discovered from Hive Metastore
//! metadata) and implements the [`DataSource`] trait for the execution
//! engine. The file format is taken from the table's HMS storage
//! descriptor (see [`HiveFileFormat`]); everything else — file listing,
//! partition values, splits, projection — is shared by both formats.
//!
//! [`HiveConnectorFactory`] creates [`HiveDataSource`] instances from the
//! scan's properties: an unpartitioned table lists the files at its
//! location; a partitioned table lists the files of each partition
//! registered in HMS, at that partition's own location.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{new_null_array, ArrayRef, StringArray};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::DataType as ArrowDataType;
use async_trait::async_trait;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use tracing::debug;

use arneb_common::error::{ConnectorError, ExecutionError};
use arneb_common::stream::{stream_from_batches, SendableRecordBatchStream};
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_connectors::parquet_scan::{self, ParquetBatchStream};
use arneb_connectors::scan_adapter::{adapt_batch, remap_filter, ColumnSource};
use arneb_connectors::storage::{StorageRegistry, StorageUri};
use arneb_connectors::ConnectorFactory;
use arneb_execution::{DataSource, ScanContext};
use arneb_planner::PlanExpr;

use crate::orc;

/// Table-scan property keys set by
/// [`HiveTableProvider`](crate::catalog::HiveTableProvider) and read by
/// [`HiveConnectorFactory`]. They travel with the plan to workers.
pub mod props {
    /// Object-store URI of the table's data directory.
    pub const LOCATION: &str = "location";
    /// HMS storage descriptor input format class.
    pub const INPUT_FORMAT: &str = "input_format";
    /// HMS storage descriptor SerDe class.
    pub const SERDE_LIB: &str = "serde_lib";
    /// Number of partition key columns; they are the last columns of the
    /// table schema.
    pub const PARTITION_COLUMNS: &str = "partition_columns";
    /// The partitions registered in HMS, as a JSON array of
    /// [`HivePartition`](super::HivePartition). Absent on a partitioned
    /// table built without HMS, whose scan then discovers partitions from
    /// `key=value` directories under the table location.
    pub const PARTITIONS: &str = "partitions";
    /// `"true"` for Hive ACID (transactional) tables.
    pub const TRANSACTIONAL: &str = "transactional";
}

/// Hive's marker directory value for a NULL partition key.
const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Data file format of a Hive table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiveFileFormat {
    /// Apache Parquet (`MapredParquetInputFormat` / `ParquetHiveSerDe`).
    Parquet,
    /// Apache ORC (`OrcInputFormat` / `OrcSerde`).
    Orc,
}

impl HiveFileFormat {
    /// Detect the format from the HMS input format and SerDe classes.
    /// Tables without either (e.g. registered by hand in tests) are read as
    /// Parquet; any other format (text, Avro, RCFile, ...) is rejected.
    pub fn detect(input_format: &str, serde_lib: &str) -> Result<Self, ConnectorError> {
        let classes = format!("{input_format} {serde_lib}").to_ascii_lowercase();
        if classes.contains("orcinputformat") || classes.contains("orcserde") {
            Ok(Self::Orc)
        } else if classes.contains("parquet") || classes.trim().is_empty() {
            Ok(Self::Parquet)
        } else {
            Err(ConnectorError::UnsupportedOperation(format!(
                "unsupported Hive storage format (input format '{input_format}', \
                 serde '{serde_lib}'): only Parquet and ORC tables can be read"
            )))
        }
    }
}

/// A partition registered in Hive Metastore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HivePartition {
    /// Raw partition values, one per partition column, in column order.
    pub values: Vec<String>,
    /// Object-store URI of the partition's data directory. Empty when HMS
    /// recorded none, in which case Hive's default layout under the table
    /// location applies.
    pub location: String,
}

/// Encode partitions for the [`props::PARTITIONS`] scan property.
pub fn encode_partitions(partitions: &[HivePartition]) -> String {
    serde_json::to_string(partitions).expect("HivePartition serializes to JSON")
}

fn decode_partitions(json: &str) -> Result<Vec<HivePartition>, ConnectorError> {
    serde_json::from_str(json)
        .map_err(|e| ConnectorError::ReadError(format!("invalid Hive partitions property: {e}")))
}

/// One data file and the values of the table's partition columns for it.
#[derive(Debug, Clone)]
struct HiveFile {
    /// Store holding the file: a partition may live in another bucket than
    /// the table.
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    /// One-row array per partition column, in the column's table type.
    partition_values: Vec<ArrayRef>,
}

// ---------------------------------------------------------------------------
// HiveDataSource
// ---------------------------------------------------------------------------

/// Data source that reads the data files of a Hive table.
///
/// The file list is computed once at construction time (by
/// [`HiveConnectorFactory::create_data_source`]). Each file is exposed as
/// `splits_per_file` scan partitions; [`scan`](DataSource::scan) for
/// partition `i` reads one split of one file.
///
/// The schema is the table's data columns followed by its partition
/// columns, whose values come from each file's HMS partition (or, without
/// HMS partitions, from the `key=value` directories it lives under).
pub struct HiveDataSource {
    /// Column schema from HMS metadata: data columns, then partition columns.
    column_schema: Vec<ColumnInfo>,
    /// Number of trailing partition columns in `column_schema`.
    partition_column_count: usize,
    /// Data file format.
    format: HiveFileFormat,
    /// Pre-listed data files of the table.
    files: Vec<HiveFile>,
    /// Number of logical sub-partitions per file (1 = legacy
    /// one-partition-per-file). Each sub-partition reads a slice of the
    /// file (Parquet: a row range; ORC: a stripe or row range), exposing
    /// more parallel scan tasks than the raw file count when the workload
    /// has many CPU cores and few files.
    splits_per_file: usize,
}

impl HiveDataSource {
    /// Create an unpartitioned Parquet data source with a pre-listed file set.
    ///
    /// - `store`: the object store for the table location.
    /// - `column_schema`: column metadata from HMS.
    /// - `file_paths`: pre-listed Parquet files.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        column_schema: Vec<ColumnInfo>,
        file_paths: Vec<ObjectPath>,
    ) -> Self {
        let files = file_paths
            .into_iter()
            .map(|path| HiveFile {
                store: store.clone(),
                path,
                partition_values: Vec::new(),
            })
            .collect();
        Self::with_files(column_schema, 0, HiveFileFormat::Parquet, files)
    }

    fn with_files(
        column_schema: Vec<ColumnInfo>,
        partition_column_count: usize,
        format: HiveFileFormat,
        files: Vec<HiveFile>,
    ) -> Self {
        let splits_per_file = parquet_scan::splits_per_file(files.len());
        Self {
            column_schema,
            partition_column_count,
            format,
            files,
            splits_per_file,
        }
    }

    /// Async constructor that lists the Parquet files under `prefix`
    /// before building an unpartitioned Parquet source. Convenience for
    /// callers and tests that don't already have the file list in hand.
    pub async fn from_prefix(
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
        column_schema: Vec<ColumnInfo>,
    ) -> Result<Self, ExecutionError> {
        Self::open(store, prefix, column_schema, HiveFileFormat::Parquet, 0).await
    }

    /// List the data files under `prefix` and resolve each file's partition
    /// values from its `key=value` directories.
    ///
    /// `column_schema` is the table's data columns followed by its
    /// `partition_column_count` partition columns. Fails on Hive ACID
    /// directory layouts (`base_N` / `delta_N_M`). For a partitioned table,
    /// files not under a directory for every partition column are skipped,
    /// as Trino ignores them. Partitioned tables from HMS use
    /// [`open_partitions`](Self::open_partitions) instead.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
        column_schema: Vec<ColumnInfo>,
        format: HiveFileFormat,
        partition_column_count: usize,
    ) -> Result<Self, ExecutionError> {
        let partition_columns = partition_columns(&column_schema, partition_column_count)?;
        let mut files = Vec::new();
        for path in list_data_files(&store, &prefix).await? {
            let Some(raw) = directory_partition_values(&prefix, &path, partition_columns) else {
                debug!("skipping '{path}': not under a directory for every partition column");
                continue;
            };
            let partition_values = typed_partition_values(&raw, partition_columns, &path)?;
            files.push(HiveFile {
                store: store.clone(),
                path,
                partition_values,
            });
        }
        Ok(Self::with_files(
            column_schema,
            partition_column_count,
            format,
            files,
        ))
    }

    /// List the data files of each registered partition, at that
    /// partition's own location, the way Trino and Hive read a partitioned
    /// table. Files under the table location that belong to no registered
    /// partition are never read.
    ///
    /// A partition without a location falls back to Hive's default layout,
    /// `<table location>/<key>=<value>/...`.
    pub async fn open_partitions(
        registry: &StorageRegistry,
        table_location: &str,
        column_schema: Vec<ColumnInfo>,
        format: HiveFileFormat,
        partition_column_count: usize,
        partitions: &[HivePartition],
    ) -> Result<Self, ConnectorError> {
        let partition_columns = partition_columns(&column_schema, partition_column_count)
            .map_err(|e| ConnectorError::ReadError(e.to_string()))?;
        let mut files = Vec::new();
        for partition in partitions {
            if partition.values.len() != partition_columns.len() {
                return Err(ConnectorError::ReadError(format!(
                    "Hive partition {:?} has {} values but the table has {} partition columns",
                    partition.values,
                    partition.values.len(),
                    partition_columns.len()
                )));
            }
            let location = if partition.location.is_empty() {
                default_partition_location(table_location, partition_columns, &partition.values)
            } else {
                partition.location.clone()
            };
            let uri = StorageUri::parse(&location)?;
            let store = registry.get_store(&uri)?;
            let prefix = literal_object_path(&uri);
            let paths = list_data_files(&store, &prefix)
                .await
                .map_err(|e| ConnectorError::ReadError(e.to_string()))?;
            if paths.is_empty() {
                continue;
            }
            let raw: Vec<&str> = partition.values.iter().map(String::as_str).collect();
            let partition_values = typed_partition_values(&raw, partition_columns, &location)
                .map_err(|e| ConnectorError::ReadError(e.to_string()))?;
            files.extend(paths.into_iter().map(|path| HiveFile {
                store: store.clone(),
                path,
                partition_values: partition_values.clone(),
            }));
        }
        Ok(Self::with_files(
            column_schema,
            partition_column_count,
            format,
            files,
        ))
    }

    /// Scan one split of a Parquet file of a partitioned table: read the
    /// projected data columns, append the file's partition values.
    async fn scan_partitioned_parquet(
        &self,
        ctx: &ScanContext,
        file: &HiveFile,
        projection: &[usize],
        output_schema: arrow::datatypes::SchemaRef,
        split_idx: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let n_data = self.column_schema.len() - self.partition_column_count;
        let mut roots: Vec<usize> = projection.iter().copied().filter(|&i| i < n_data).collect();
        roots.sort_unstable();
        roots.dedup();
        let sources: Vec<ColumnSource> = projection
            .iter()
            .map(|&i| match i.checked_sub(n_data) {
                None => ColumnSource::File(roots.binary_search(&i).unwrap_or_default()),
                Some(p) => ColumnSource::Constant(file.partition_values[p].clone()),
            })
            .collect();
        // Filters on partition columns stay above the scan.
        let data_filters: Vec<PlanExpr> = ctx
            .filters
            .iter()
            .filter_map(|f| remap_filter(f, &|i| (i < n_data).then_some(i)))
            .collect();
        let builder = parquet_scan::open_parquet_builder(&file.store, &file.path).await?;
        let stream = parquet_scan::build_split_stream(
            builder,
            &file.path,
            &data_filters,
            Some(&roots),
            ctx.batch_size,
            split_idx,
            self.splits_per_file,
        )?;
        let schema = output_schema.clone();
        let path = file.path.to_string();
        Ok(Box::pin(
            ParquetBatchStream::new(output_schema, stream, path.clone()).with_adapter(Box::new(
                move |batch| adapt_batch(&schema, &sources, &path, batch),
            )),
        ))
    }
}

impl fmt::Debug for HiveDataSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HiveDataSource")
            .field("format", &self.format)
            .field("files", &self.files.len())
            .field("columns", &self.column_schema.len())
            .field("partition_columns", &self.partition_column_count)
            .finish()
    }
}

#[async_trait]
impl DataSource for HiveDataSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.column_schema.clone()
    }

    fn partition_count(&self) -> usize {
        (self.files.len() * self.splits_per_file).max(1)
    }

    async fn scan(
        &self,
        ctx: &ScanContext,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let full_schema = column_info_to_arrow_schema(&self.column_schema);
        let projection: Vec<usize> = match &ctx.projection {
            Some(p) => p.clone(),
            None => (0..self.column_schema.len()).collect(),
        };
        if let Some(bad) = projection
            .iter()
            .find(|&&i| i >= full_schema.fields().len())
        {
            return Err(ExecutionError::InvalidOperation(format!(
                "HiveDataSource: projection index {bad} out of range"
            )));
        }
        let output_schema = Arc::new(arrow::datatypes::Schema::new(
            projection
                .iter()
                .map(|&i| full_schema.field(i).clone())
                .collect::<Vec<_>>(),
        ));

        if self.files.is_empty() {
            debug!("no data files registered on HiveDataSource");
            return Ok(stream_from_batches(output_schema, vec![]));
        }

        let (file_idx, split_idx) = parquet_scan::split_index(
            "HiveDataSource",
            partition,
            self.files.len(),
            self.splits_per_file,
        )?;
        let file = &self.files[file_idx];

        match self.format {
            HiveFileFormat::Orc => {
                let n_data = self.column_schema.len() - self.partition_column_count;
                orc::scan(orc::OrcScan {
                    store: &file.store,
                    path: &file.path,
                    data_columns: &self.column_schema[..n_data],
                    projection: &projection,
                    partition_values: &file.partition_values,
                    output_schema,
                    batch_size: ctx
                        .batch_size
                        .unwrap_or_else(arneb_connectors::file::scan_default_batch_size),
                    split_idx,
                    splits_per_file: self.splits_per_file,
                })
                .await
            }
            HiveFileFormat::Parquet if self.partition_column_count > 0 => {
                self.scan_partitioned_parquet(ctx, file, &projection, output_schema, split_idx)
                    .await
            }
            HiveFileFormat::Parquet => {
                let file_path = &file.path;
                let builder = parquet_scan::open_parquet_builder(&file.store, file_path).await?;
                let stream = parquet_scan::build_split_stream(
                    builder,
                    file_path,
                    &ctx.filters,
                    ctx.projection.as_deref(),
                    ctx.batch_size,
                    split_idx,
                    self.splits_per_file,
                )?;

                // True pipelined streaming: yield Parquet batches as they're
                // produced instead of collecting the whole partition's output
                // into a Vec first. The earlier `collect → stream_from_batches`
                // path held an entire partition's worth of decoded Arrow
                // batches in memory before the downstream operator saw the
                // first row — for a 6M-row lineitem scan with 7 projected
                // columns that's ~336 MB per query, which dominated the
                // single-table-aggregate work-memory delta vs Trino.
                Ok(Box::pin(ParquetBatchStream::new(
                    output_schema,
                    stream,
                    file_path.to_string(),
                )))
            }
        }
    }
}

/// `true` for a Hive ACID directory name: `base_N`, `delta_N_M[_S]`,
/// `delete_delta_N_M[_S]` (optionally with a `_vN` visibility suffix).
fn is_acid_dir(name: &str) -> bool {
    let rest = name
        .strip_prefix("base_")
        .or_else(|| name.strip_prefix("delete_delta_"))
        .or_else(|| name.strip_prefix("delta_"));
    rest.is_some_and(|r| {
        r.split('_')
            .next()
            .is_some_and(|n| n.parse::<u64>().is_ok())
    })
}

/// List all data files under a given prefix in an object store.
///
/// Skips hidden entries following the Hadoop/Hive convention: any file or
/// directory whose name starts with `.` or `_` is hidden (e.g. `_SUCCESS`,
/// `_committed_*`, `.part-xxx.parquet.crc`, `.trino-staging/`). This
/// matches Trino's `HiveFileIterator`. No extension-based filtering is
/// applied — the table's storage format decides how to read the remaining
/// files. Hive ACID layouts are rejected rather than misread.
async fn list_data_files(
    store: &Arc<dyn ObjectStore>,
    prefix: &ObjectPath,
) -> Result<Vec<ObjectPath>, ExecutionError> {
    let mut paths = Vec::new();
    let mut listing = store.list(Some(prefix));
    while let Some(result) = listing.next().await {
        let meta = result.map_err(|e| {
            ExecutionError::InvalidOperation(format!("failed to list files at '{}': {}", prefix, e))
        })?;
        let parts: Vec<String> = relative_parts(prefix, &meta.location);
        if parts
            .iter()
            .any(|p| p.starts_with('.') || p.starts_with('_'))
        {
            continue;
        }
        let dirs = &parts[..parts.len().saturating_sub(1)];
        if let Some(acid) = dirs.iter().find(|d| is_acid_dir(d)) {
            return Err(ExecutionError::InvalidOperation(format!(
                "Hive ACID (transactional) tables are not supported: found '{acid}' \
                 directory under '{prefix}'"
            )));
        }
        if meta.size > 0 {
            paths.push(meta.location);
        }
    }
    Ok(paths)
}

/// Path segments of `path` below `prefix` (directories, then the file name).
fn relative_parts(prefix: &ObjectPath, path: &ObjectPath) -> Vec<String> {
    match path.prefix_match(prefix) {
        Some(parts) => parts.map(|p| p.as_ref().to_string()).collect(),
        None => path.parts().map(|p| p.as_ref().to_string()).collect(),
    }
}

/// Undo Hive's partition-path escaping (`%XX` hex escapes).
fn unescape_path_name(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = |b: u8| (b as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The trailing `count` partition columns of a table schema.
fn partition_columns(
    column_schema: &[ColumnInfo],
    count: usize,
) -> Result<&[ColumnInfo], ExecutionError> {
    if count > column_schema.len() {
        return Err(ExecutionError::InvalidOperation(format!(
            "Hive table has {count} partition columns but only {} columns",
            column_schema.len()
        )));
    }
    Ok(&column_schema[column_schema.len() - count..])
}

/// Raw partition values of one data file from its `key=value` directories
/// (keys matched case-insensitively), or `None` when a partition column
/// has no directory.
fn directory_partition_values(
    prefix: &ObjectPath,
    path: &ObjectPath,
    partition_columns: &[ColumnInfo],
) -> Option<Vec<String>> {
    if partition_columns.is_empty() {
        return Some(Vec::new());
    }
    let parts = relative_parts(prefix, path);
    let dirs = &parts[..parts.len().saturating_sub(1)];
    let kv: HashMap<String, String> = dirs
        .iter()
        .filter_map(|d| d.split_once('='))
        .map(|(k, v)| {
            (
                unescape_path_name(k).to_ascii_lowercase(),
                unescape_path_name(v),
            )
        })
        .collect();
    partition_columns
        .iter()
        .map(|col| kv.get(&col.name.to_ascii_lowercase()).cloned())
        .collect()
}

/// Convert raw partition values to one-row arrays in the columns' table
/// types. `__HIVE_DEFAULT_PARTITION__` is NULL. `source` names the file or
/// partition location in errors.
fn typed_partition_values(
    raw: &[impl AsRef<str>],
    partition_columns: &[ColumnInfo],
    source: &dyn fmt::Display,
) -> Result<Vec<ArrayRef>, ExecutionError> {
    raw.iter()
        .zip(partition_columns)
        .map(|(raw, col)| {
            let raw = raw.as_ref();
            let target: ArrowDataType = col.data_type.clone().into();
            if raw == HIVE_DEFAULT_PARTITION {
                return Ok(new_null_array(&target, 1));
            }
            let strict = CastOptions {
                safe: false,
                ..Default::default()
            };
            let text: ArrayRef = Arc::new(StringArray::from(vec![raw]));
            cast_with_options(&text, &target, &strict).map_err(|e| {
                ExecutionError::InvalidOperation(format!(
                    "partition value '{raw}' of column '{}' in '{source}' is not a valid {}: {e}",
                    col.name, col.data_type
                ))
            })
        })
        .collect()
}

/// Escape a partition key or value the way Hive's `FileUtils.escapePathName`
/// does when it lays out partition directories.
fn escape_path_name(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        let escape = matches!(
            c,
            '\u{01}'
                ..='\u{1F}'
                    | '"'
                    | '#'
                    | '%'
                    | '\''
                    | '*'
                    | '/'
                    | ':'
                    | '='
                    | '?'
                    | '\\'
                    | '\u{7F}'
                    | '{'
                    | '['
                    | ']'
                    | '^'
        );
        if escape {
            out.push_str(&format!("%{:02X}", c as u32));
        } else {
            out.push(c);
        }
    }
    out
}

/// The object key prefix of a partition location, taken literally. Hive
/// writes escaped partition directories (`ds=a%2Fb`) as literal keys and
/// records the location the same way, so the `%` must not be re-encoded
/// (as [`StorageUri::object_path`] does).
fn literal_object_path(uri: &StorageUri) -> ObjectPath {
    ObjectPath::parse(&uri.path).unwrap_or_else(|_| uri.object_path())
}

/// Hive's default location of a partition: `<table>/<k1>=<v1>/<k2>=<v2>`.
fn default_partition_location(
    table_location: &str,
    partition_columns: &[ColumnInfo],
    values: &[String],
) -> String {
    let mut location = table_location.trim_end_matches('/').to_string();
    for (col, value) in partition_columns.iter().zip(values) {
        location.push('/');
        location.push_str(&escape_path_name(&col.name.to_ascii_lowercase()));
        location.push('=');
        location.push_str(&escape_path_name(value));
    }
    location
}

/// Convert `ColumnInfo` slice to an Arrow schema.
fn column_info_to_arrow_schema(columns: &[ColumnInfo]) -> Arc<arrow::datatypes::Schema> {
    let fields: Vec<arrow::datatypes::Field> = columns.iter().map(|c| c.clone().into()).collect();
    Arc::new(arrow::datatypes::Schema::new(fields))
}

// ---------------------------------------------------------------------------
// HiveConnectorFactory
// ---------------------------------------------------------------------------

/// Connector factory for Hive tables.
///
/// Creates [`HiveDataSource`] instances from the HMS `location` that
/// `HiveTableProvider::properties()` attaches to each `TableScan` (and that
/// travels to workers with the serialized plan), listing the data files at
/// that location. The storage format, partitioning and ACID flag come from
/// the scan's properties too.
///
/// Nothing is cached per table: the scan's own properties are the source of
/// truth. A former cache keyed by bare table name made same-named tables in
/// different schemas read each other's files (#111).
pub struct HiveConnectorFactory {
    /// Storage registry for resolving object stores.
    storage_registry: Arc<StorageRegistry>,
}

impl HiveConnectorFactory {
    /// Create a new Hive connector factory.
    pub fn new(storage_registry: Arc<StorageRegistry>) -> Self {
        Self { storage_registry }
    }
}

impl fmt::Debug for HiveConnectorFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HiveConnectorFactory")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ConnectorFactory for HiveConnectorFactory {
    fn name(&self) -> &str {
        "hive"
    }

    async fn create_data_source(
        &self,
        table: &TableReference,
        schema: &[ColumnInfo],
        properties: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn DataSource>, ConnectorError> {
        // Iceberg tables redirected by the Hive catalog: read the pinned
        // snapshot's live files, never a listing of the table location.
        if arneb_iceberg::is_iceberg_table(properties) {
            return arneb_iceberg::create_data_source(&self.storage_registry, table, properties)
                .await;
        }

        let prop = |key: &str| properties.get(key).map(String::as_str).unwrap_or_default();
        if prop(props::TRANSACTIONAL).eq_ignore_ascii_case("true") {
            return Err(ConnectorError::UnsupportedOperation(format!(
                "Hive table '{table}' is transactional (ACID); reading Hive ACID tables \
                 is not supported"
            )));
        }
        let format = HiveFileFormat::detect(prop(props::INPUT_FORMAT), prop(props::SERDE_LIB))?;
        let partition_column_count: usize = match prop(props::PARTITION_COLUMNS) {
            "" => 0,
            n => n.parse().map_err(|_| {
                ConnectorError::ReadError(format!(
                    "Hive table '{table}': invalid partition column count '{n}'"
                ))
            })?,
        };
        let location = properties.get(props::LOCATION).ok_or_else(|| {
            ConnectorError::TableNotFound(format!(
                "Hive table '{table}' has no location in its properties"
            ))
        })?;

        let uri = StorageUri::parse(location)?;
        let store = self.storage_registry.get_store(&uri)?;
        let prefix = uri.object_path();

        // A partitioned table from HMS reads its registered partitions.
        if partition_column_count > 0 {
            if let Some(json) = properties.get(props::PARTITIONS) {
                let partitions = decode_partitions(json)?;
                let ds = HiveDataSource::open_partitions(
                    &self.storage_registry,
                    location,
                    schema.to_vec(),
                    format,
                    partition_column_count,
                    &partitions,
                )
                .await
                .map_err(|e| ConnectorError::ReadError(format!("hive table '{table}': {e}")))?;
                return Ok(Arc::new(ds));
            }
        }

        // Pre-list the data files under this prefix so the resulting
        // HiveDataSource exposes `files × splits_per_file` partitions.
        let ds = HiveDataSource::open(
            store,
            prefix,
            schema.to_vec(),
            format,
            partition_column_count,
        )
        .await
        .map_err(|e| ConnectorError::ReadError(format!("hive table '{table}': {e}")))?;
        Ok(Arc::new(ds))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::stream::collect_stream;
    use arneb_common::types::DataType;
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};
    use parquet::arrow::arrow_writer::ArrowWriter;

    /// Write a Parquet file to bytes with the given rows.
    fn write_parquet_bytes(ids: Vec<i32>, names: Vec<&str>) -> Vec<u8> {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    fn test_column_schema() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            },
        ]
    }

    #[tokio::test]
    async fn scan_single_parquet_file() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parquet_bytes = write_parquet_bytes(vec![1, 2, 3], vec!["a", "b", "c"]);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/part-0.parquet"),
                PutPayload::from_bytes(parquet_bytes.into()),
            )
            .await
            .unwrap();

        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();

        // splits_per_file may be > 1 on this machine; sum across all
        // partitions for a stable row-count assertion.
        let mut total_rows = 0;
        let mut all_batches = Vec::new();
        for p in 0..ds.partition_count() {
            let stream = ds.scan(&ScanContext::default(), p).await.unwrap();
            let batches = collect_stream(stream).await.unwrap();
            total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
            all_batches.extend(batches);
        }
        assert_eq!(total_rows, 3);

        let id_col = all_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        // Partition 0 holds the FIRST slice of the file. With 1 file +
        // many splits, that slice starts at row 0 → value(0) == 1.
        assert_eq!(id_col.value(0), 1);
    }

    #[tokio::test]
    async fn scan_multiple_parquet_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let bytes1 = write_parquet_bytes(vec![1, 2], vec!["a", "b"]);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/part-0.parquet"),
                PutPayload::from_bytes(bytes1.into()),
            )
            .await
            .unwrap();

        let bytes2 = write_parquet_bytes(vec![3, 4], vec!["c", "d"]);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/part-1.parquet"),
                PutPayload::from_bytes(bytes2.into()),
            )
            .await
            .unwrap();

        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();

        // 2 files × splits_per_file partitions; sum across all = 4 rows.
        assert_eq!(ds.partition_count() % 2, 0);
        let mut total_rows = 0;
        for p in 0..ds.partition_count() {
            let stream = ds.scan(&ScanContext::default(), p).await.unwrap();
            let batches = collect_stream(stream).await.unwrap();
            total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        assert_eq!(total_rows, 4);
    }

    #[tokio::test]
    async fn scan_skips_hidden_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let bytes = write_parquet_bytes(vec![10], vec!["x"]);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/data.parquet"),
                PutPayload::from_bytes(bytes.into()),
            )
            .await
            .unwrap();

        // Hadoop/Hive hidden-file markers — should be skipped.
        for hidden in ["_SUCCESS", "_committed_abc", ".hidden"] {
            store
                .put(
                    &ObjectPath::from(format!("warehouse/db/table/{hidden}")),
                    PutPayload::from_static(b""),
                )
                .await
                .unwrap();
        }

        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();

        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn scan_empty_directory() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/empty_table"),
            test_column_schema(),
        )
        .await
        .unwrap();

        // Empty directory → 0 file partitions → partition_count clamps to 1
        // but scan returns immediately with the empty-batches stream below.
        assert!(ds.partition_count() == 1);
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn scan_with_projection_pushdown() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let bytes = write_parquet_bytes(vec![1, 2], vec!["a", "b"]);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/data.parquet"),
                PutPayload::from_bytes(bytes.into()),
            )
            .await
            .unwrap();

        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();

        // Project only the "name" column (index 1).
        let ctx = ScanContext::default().with_projection(vec![1]);
        let mut all_batches = Vec::new();
        let mut total_rows = 0;
        let mut all_names: Vec<String> = Vec::new();
        for p in 0..ds.partition_count() {
            let stream = ds.scan(&ctx, p).await.unwrap();
            let batches = collect_stream(stream).await.unwrap();
            for b in &batches {
                total_rows += b.num_rows();
                let name_col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                for r in 0..b.num_rows() {
                    all_names.push(name_col.value(r).to_string());
                }
            }
            all_batches.extend(batches);
        }
        assert_eq!(total_rows, 2);
        assert_eq!(all_batches[0].num_columns(), 1);
        all_names.sort();
        assert_eq!(all_names, vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test]
    async fn hive_data_source_debug() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();
        let debug_str = format!("{ds:?}");
        assert!(debug_str.contains("HiveDataSource"));
        // Debug now reports file count instead of prefix.
        assert!(debug_str.contains("files"));
    }

    #[tokio::test]
    async fn hive_data_source_schema() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();
        let schema = ds.schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[1].name, "name");
    }

    #[test]
    fn hive_data_source_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<HiveDataSource>();
        assert_send_sync::<HiveConnectorFactory>();
    }

    // -- HiveConnectorFactory tests --

    /// Table properties as `HiveTableProvider::properties()` produces them.
    fn location_props(location: &str) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::from([("location".to_string(), location.to_string())])
    }

    #[tokio::test]
    async fn factory_creates_data_source() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let bytes = write_parquet_bytes(vec![10, 20], vec!["x", "y"]);
        store
            .put(
                &ObjectPath::from("data/table/part.parquet"),
                PutPayload::from_bytes(bytes.into()),
            )
            .await
            .unwrap();

        let registry = Arc::new(StorageRegistry::new());
        registry.register_store("s3://test-bucket", store);

        let factory = HiveConnectorFactory::new(registry);
        let props = location_props("s3://test-bucket/data/table");

        let table_ref = TableReference::table("my_table");
        let ds = factory
            .create_data_source(&table_ref, &test_column_schema(), &props)
            .await
            .unwrap();

        let mut total_rows = 0;
        for p in 0..ds.partition_count() {
            let stream = ds.scan(&ScanContext::default(), p).await.unwrap();
            let batches = collect_stream(stream).await.unwrap();
            total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        assert_eq!(total_rows, 2);
    }

    /// Regression for #111: same-named tables in different schemas must each
    /// read their own location, whichever is created first.
    #[tokio::test]
    async fn factory_same_table_name_in_two_schemas() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for (dir, ids) in [
            ("tpch/orders", vec![1, 2]),
            ("tpch_decimal/orders", vec![7]),
        ] {
            let names = vec!["x"; ids.len()];
            store
                .put(
                    &ObjectPath::from(format!("wh/{dir}/part.parquet")),
                    PutPayload::from_bytes(write_parquet_bytes(ids, names).into()),
                )
                .await
                .unwrap();
        }
        let registry = Arc::new(StorageRegistry::new());
        registry.register_store("s3://lake", store);

        async fn ids(factory: &HiveConnectorFactory, schema: &str) -> Vec<i32> {
            let table = TableReference {
                catalog: Some("datalake".to_string()),
                schema: Some(schema.to_string()),
                table: "orders".to_string(),
            };
            let props = location_props(&format!("s3://lake/wh/{schema}/orders"));
            let ds = factory
                .create_data_source(&table, &test_column_schema(), &props)
                .await
                .unwrap();
            let mut out = Vec::new();
            for p in 0..ds.partition_count() {
                let stream = ds.scan(&ScanContext::default(), p).await.unwrap();
                for b in collect_stream(stream).await.unwrap() {
                    let col = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                    out.extend(col.values().iter().copied());
                }
            }
            out.sort();
            out
        }

        for order in [["tpch", "tpch_decimal"], ["tpch_decimal", "tpch"]] {
            let factory = HiveConnectorFactory::new(registry.clone());
            for schema in order {
                let want = if schema == "tpch" {
                    vec![1, 2]
                } else {
                    vec![7]
                };
                assert_eq!(ids(&factory, schema).await, want, "schema {schema}");
            }
        }
    }

    #[tokio::test]
    async fn factory_redirects_iceberg_tables() {
        // An Iceberg table must be planned from its metadata, not by
        // listing `location`: with no metadata_location it fails instead
        // of scanning the directory.
        let registry = Arc::new(StorageRegistry::new());
        let factory = HiveConnectorFactory::new(registry);
        let mut props = std::collections::HashMap::new();
        props.insert("location".to_string(), "/tmp/ice".to_string());
        props.insert("table_type".to_string(), "ICEBERG".to_string());
        let err = factory
            .create_data_source(&TableReference::table("ice"), &[], &props)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("metadata_location"), "{err}");
    }

    #[tokio::test]
    async fn factory_unregistered_table() {
        let registry = Arc::new(StorageRegistry::new());
        let factory = HiveConnectorFactory::new(registry);

        let table_ref = TableReference::table("nonexistent");
        let result = factory
            .create_data_source(&table_ref, &[], &Default::default())
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nonexistent"));
    }

    #[test]
    fn factory_name() {
        let registry = Arc::new(StorageRegistry::new());
        let factory = HiveConnectorFactory::new(registry);
        assert_eq!(factory.name(), "hive");
    }

    #[test]
    fn factory_debug() {
        let registry = Arc::new(StorageRegistry::new());
        let factory = HiveConnectorFactory::new(registry);
        let debug_str = format!("{factory:?}");
        assert!(debug_str.contains("HiveConnectorFactory"));
    }

    // -- Compression codec tests --

    fn write_parquet_bytes_compressed(
        ids: Vec<i32>,
        names: Vec<&str>,
        compression: parquet::basic::Compression,
    ) -> Vec<u8> {
        use parquet::file::properties::WriterProperties;

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();

        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    async fn assert_hive_scan_reads_compressed(compression: parquet::basic::Compression) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let bytes = write_parquet_bytes_compressed(vec![1, 2, 3], vec!["a", "b", "c"], compression);
        store
            .put(
                &ObjectPath::from("warehouse/db/table/data.parquet"),
                PutPayload::from_bytes(bytes.into()),
            )
            .await
            .unwrap();

        let ds = HiveDataSource::from_prefix(
            store,
            ObjectPath::from("warehouse/db/table"),
            test_column_schema(),
        )
        .await
        .unwrap();

        let mut total_rows = 0;
        for p in 0..ds.partition_count() {
            let stream = ds.scan(&ScanContext::default(), p).await.unwrap();
            let batches = collect_stream(stream).await.unwrap();
            total_rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn scan_gzip_compressed_parquet() {
        assert_hive_scan_reads_compressed(parquet::basic::Compression::GZIP(
            parquet::basic::GzipLevel::default(),
        ))
        .await;
    }

    #[tokio::test]
    async fn scan_zstd_compressed_parquet() {
        assert_hive_scan_reads_compressed(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::default(),
        ))
        .await;
    }

    #[tokio::test]
    async fn scan_lz4_compressed_parquet() {
        assert_hive_scan_reads_compressed(parquet::basic::Compression::LZ4_RAW).await;
    }

    #[tokio::test]
    async fn scan_brotli_compressed_parquet() {
        assert_hive_scan_reads_compressed(parquet::basic::Compression::BROTLI(
            parquet::basic::BrotliLevel::default(),
        ))
        .await;
    }

    #[tokio::test]
    async fn factory_with_local_filesystem() {
        let registry = Arc::new(StorageRegistry::new());
        let factory = HiveConnectorFactory::new(registry);
        let props = location_props("/data/warehouse/db/tbl");

        let table_ref = TableReference::table("local_table");
        let ds = factory
            .create_data_source(&table_ref, &test_column_schema(), &props)
            .await
            .unwrap();

        // Scan will find no files (directory doesn't exist), returning empty stream.
        let stream = ds.scan(&ScanContext::default(), 0).await.unwrap();
        let batches = collect_stream(stream).await.unwrap();
        assert!(batches.is_empty());
    }
}
