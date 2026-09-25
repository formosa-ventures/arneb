//! Iceberg scan planning, data source, and connector factory.
//!
//! Scan planning resolves the pinned snapshot's manifest list and
//! manifests into the set of live Parquet data files. Each file is then
//! read through the same Parquet machinery the Hive connector uses
//! (row-group pruning, row-filter pushdown, projection pushdown,
//! intra-file splits), with one Iceberg-specific layer on top: columns are
//! matched to the file **by field ID**, not by name or position, so
//! renamed, reordered, added, and type-promoted columns read correctly.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use arrow::array::{new_null_array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::async_reader::{
    ParquetObjectReader, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use tracing::debug;

use arneb_common::error::{ArnebError, ConnectorError, ExecutionError};
use arneb_common::stream::{stream_from_batches, RecordBatchStream, SendableRecordBatchStream};
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_connectors::storage::{StorageRegistry, StorageUri};
use arneb_connectors::ConnectorFactory;
use arneb_execution::{DataSource, ScanContext};
use arneb_planner::PlanExpr;

use crate::manifest::{
    read_manifest, read_manifest_list, DataFile, DataFileContent, EntryStatus, ManifestContent,
    ManifestFile,
};
use crate::metadata::{IcebergType, PartitionSpec, TableMetadata};
use crate::pruning::file_can_be_skipped;

/// Table property keys used to hand a resolved Iceberg table from the
/// catalog to the connector factory (they travel inside the logical plan,
/// so workers see the same pinned snapshot as the coordinator).
pub mod props {
    /// Marks a table as Iceberg (value `ICEBERG`).
    pub const TABLE_TYPE: &str = "table_type";
    /// Location of the metadata JSON the catalog resolved.
    pub const METADATA_LOCATION: &str = "metadata_location";
    /// Snapshot pinned at planning time (absent for an empty table).
    pub const SNAPSHOT_ID: &str = "snapshot_id";
    /// Set when the HMS entry is not an Iceberg table; the factory fails
    /// the query with this message instead of reading anything.
    pub const ERROR: &str = "iceberg_error";
}

/// Manifests fetched concurrently during scan planning.
const MANIFEST_FETCH_CONCURRENCY: usize = 8;

// ---------------------------------------------------------------------------
// Object store helpers
// ---------------------------------------------------------------------------

/// Resolve a URI to its object store and path.
pub(crate) fn resolve(
    storage: &StorageRegistry,
    uri: &str,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath), ConnectorError> {
    let parsed = StorageUri::parse(uri)?;
    let store = storage.get_store(&parsed)?;
    Ok((store, parsed.object_path()))
}

/// Read a whole object.
pub(crate) async fn read_object(
    storage: &StorageRegistry,
    uri: &str,
) -> Result<Bytes, ConnectorError> {
    let (store, path) = resolve(storage, uri)?;
    let get = store
        .get(&path)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("failed to read '{uri}': {e}")))?;
    get.bytes()
        .await
        .map_err(|e| ConnectorError::ReadError(format!("failed to read '{uri}': {e}")))
}

/// Load and parse a table metadata file.
pub async fn load_metadata(
    storage: &StorageRegistry,
    metadata_location: &str,
) -> Result<TableMetadata, ConnectorError> {
    let bytes = read_object(storage, metadata_location).await?;
    TableMetadata::parse(&bytes).map_err(|e| match e {
        ConnectorError::ReadError(m) => {
            ConnectorError::ReadError(format!("{m} (file '{metadata_location}')"))
        }
        other => other,
    })
}

// ---------------------------------------------------------------------------
// Scan planning
// ---------------------------------------------------------------------------

/// A live data file selected for the scan.
#[derive(Debug, Clone)]
pub struct ScanFile {
    /// Partition spec the file was written with.
    pub spec_id: i32,
    /// Manifest metadata (path, bounds, partition tuple).
    pub data_file: DataFile,
}

/// Resolve the live data files of `snapshot_id`.
///
/// Fails with [`ConnectorError::UnsupportedOperation`] when the snapshot
/// carries live delete files (merge-on-read row-level deletes) or
/// non-Parquet data files — reading those without applying them would
/// silently return wrong results.
pub async fn plan_files(
    storage: &StorageRegistry,
    metadata: &TableMetadata,
    snapshot_id: i64,
    table_name: &str,
) -> Result<Vec<ScanFile>, ConnectorError> {
    let snapshot = metadata.snapshot(snapshot_id).ok_or_else(|| {
        ConnectorError::ReadError(format!(
            "Iceberg table {table_name}: snapshot {snapshot_id} not found in metadata"
        ))
    })?;

    let manifests: Vec<ManifestFile> = match &snapshot.manifest_list {
        Some(list) => read_manifest_list(&read_object(storage, list).await?)?,
        None => snapshot
            .manifests
            .iter()
            .map(|p| ManifestFile {
                manifest_path: p.clone(),
                partition_spec_id: 0,
                content: ManifestContent::Data,
                added_files_count: None,
                existing_files_count: None,
            })
            .collect(),
    };

    let to_read: Vec<ManifestFile> = manifests
        .into_iter()
        .filter(|m| !m.provably_empty())
        .collect();

    let per_manifest: Vec<(ManifestFile, Vec<crate::manifest::ManifestEntry>)> =
        futures::stream::iter(to_read.into_iter().map(|m| async move {
            let bytes = read_object(storage, &m.manifest_path).await?;
            let entries = read_manifest(&bytes)?;
            Ok::<_, ConnectorError>((m, entries))
        }))
        .buffered(MANIFEST_FETCH_CONCURRENCY)
        .try_collect()
        .await?;

    let mut files = Vec::new();
    for (manifest, entries) in per_manifest {
        for entry in entries {
            if entry.status == EntryStatus::Deleted {
                continue;
            }
            let df = entry.data_file;
            if manifest.content == ManifestContent::Deletes || df.content != DataFileContent::Data {
                let kind = match df.content {
                    DataFileContent::EqualityDeletes => "equality",
                    _ => "position",
                };
                return Err(ConnectorError::UnsupportedOperation(format!(
                    "Iceberg table {table_name} has {kind} delete files in snapshot \
                     {snapshot_id} (row-level deletes / merge-on-read, e.g. '{}'); reading \
                     tables with delete files is not supported yet. Compact the table \
                     (e.g. Trino `ALTER TABLE ... EXECUTE optimize`) or rewrite it with \
                     copy-on-write to make it readable",
                    df.file_path
                )));
            }
            if df.file_format != "PARQUET" {
                return Err(ConnectorError::UnsupportedOperation(format!(
                    "Iceberg table {table_name}: data file format {} is not supported \
                     (only PARQUET): '{}'",
                    df.file_format, df.file_path
                )));
            }
            files.push(ScanFile {
                spec_id: manifest.partition_spec_id,
                data_file: df,
            });
        }
    }
    debug!(
        table = table_name,
        snapshot_id,
        files = files.len(),
        "planned Iceberg scan"
    );
    Ok(files)
}

// ---------------------------------------------------------------------------
// IcebergDataSource
// ---------------------------------------------------------------------------

/// Data source over the live data files of one Iceberg snapshot.
pub struct IcebergDataSource {
    storage: Arc<StorageRegistry>,
    /// Exposed schema (current Iceberg schema, supported columns only).
    columns: Vec<ColumnInfo>,
    /// Field ID and Iceberg type of each exposed column (parallel to
    /// `columns`).
    column_fields: Vec<(i32, IcebergType)>,
    /// Name → field ID fallback for files written without field IDs.
    name_mapping: HashMap<String, i32>,
    partition_specs: HashMap<i32, PartitionSpec>,
    files: Vec<ScanFile>,
    splits_per_file: usize,
}

impl IcebergDataSource {
    /// Build a data source from planned files.
    pub fn new(
        storage: Arc<StorageRegistry>,
        metadata: &TableMetadata,
        files: Vec<ScanFile>,
    ) -> Self {
        let (cols, _) = metadata.current_schema.to_columns();
        let column_fields = cols
            .iter()
            .map(|(id, _)| {
                let ty = metadata
                    .current_schema
                    .field_by_id(*id)
                    .map(|f| f.field_type.clone())
                    .unwrap_or(IcebergType::Unsupported(String::new()));
                (*id, ty)
            })
            .collect();
        let columns = cols.into_iter().map(|(_, c)| c).collect();

        // Same heuristic as the Hive connector: split files into row-range
        // slices until there are enough partitions to occupy every core.
        let n_files = files.len().max(1);
        let target = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);
        let splits_per_file = if n_files >= target {
            1
        } else {
            target.div_ceil(n_files)
        };

        let mut name_mapping = metadata.name_mapping();
        if name_mapping.is_empty() {
            // No explicit mapping: fall back to current column names.
            for f in &metadata.current_schema.fields {
                name_mapping.insert(f.name.clone(), f.id);
            }
        }

        Self {
            storage,
            columns,
            column_fields,
            name_mapping,
            partition_specs: metadata.partition_specs.clone(),
            files,
            splits_per_file,
        }
    }

    /// Number of live data files.
    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    fn output_schema(&self, projection: Option<&[usize]>) -> SchemaRef {
        let fields: Vec<Field> = match projection {
            Some(p) => p.iter().map(|&i| self.columns[i].clone().into()).collect(),
            None => self.columns.iter().map(|c| c.clone().into()).collect(),
        };
        Arc::new(Schema::new(fields))
    }
}

impl fmt::Debug for IcebergDataSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergDataSource")
            .field("files", &self.files.len())
            .field("columns", &self.columns.len())
            .field("splits_per_file", &self.splits_per_file)
            .finish()
    }
}

/// How one output column is produced from a file batch.
#[derive(Debug, Clone)]
enum ColumnSource {
    /// Column at this position of the (projected) file batch, cast to the
    /// table type when the file type differs (e.g. `int` → `long`).
    File(usize),
    /// Column absent from the file (added after the file was written).
    Null,
}

/// Per-file mapping from the table schema to the physical file schema.
struct FileColumnMap {
    /// Field ID → root column index in the file.
    root_by_field: HashMap<i32, usize>,
    /// Root index → leaf index, for primitive roots only.
    leaf_by_root: HashMap<usize, usize>,
}

impl FileColumnMap {
    fn build(schema: &SchemaDescriptor, name_mapping: &HashMap<String, i32>) -> Self {
        let roots = schema.root_schema().get_fields();
        let mut root_by_field = HashMap::new();
        let has_ids = roots.iter().any(|f| f.get_basic_info().has_id());
        for (idx, f) in roots.iter().enumerate() {
            let info = f.get_basic_info();
            let id = if has_ids {
                info.has_id().then(|| info.id())
            } else {
                name_mapping.get(info.name()).copied()
            };
            if let Some(id) = id {
                root_by_field.entry(id).or_insert(idx);
            }
        }
        let mut leaf_by_root = HashMap::new();
        for leaf in 0..schema.num_columns() {
            let root = schema.get_column_root_idx(leaf);
            if roots[root].is_primitive() {
                leaf_by_root.insert(root, leaf);
            }
        }
        Self {
            root_by_field,
            leaf_by_root,
        }
    }
}

/// Rewrite column references in a pushed-down filter from table column
/// indices to file leaf indices. Returns `None` when the filter touches a
/// column that cannot be pushed into this file (missing, nested, or with a
/// different physical type) or uses an expression shape the Parquet
/// pushdown does not understand; the filter still runs above the scan.
fn remap_filter(e: &PlanExpr, map: &dyn Fn(usize) -> Option<usize>) -> Option<PlanExpr> {
    Some(match e {
        PlanExpr::Column { index, name, span } => PlanExpr::Column {
            index: map(*index)?,
            name: name.clone(),
            span: *span,
        },
        PlanExpr::Literal { .. } => e.clone(),
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => PlanExpr::BinaryOp {
            left: Box::new(remap_filter(left, map)?),
            op: *op,
            right: Box::new(remap_filter(right, map)?),
            span: *span,
        },
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => PlanExpr::InList {
            expr: Box::new(remap_filter(expr, map)?),
            list: list
                .iter()
                .map(|x| remap_filter(x, map))
                .collect::<Option<Vec<_>>>()?,
            negated: *negated,
            span: *span,
        },
        _ => return None,
    })
}

/// `true` when every `column <op> literal` comparison in `e` compares
/// against a literal of exactly the column's type. The shared Parquet
/// predicate kernels compare Arrow arrays directly and error on mixed
/// types (e.g. `Int64` column vs `Int32` scalar), so anything else stays
/// above the scan.
fn literal_types_match(e: &PlanExpr, columns: &[ColumnInfo]) -> bool {
    let col_type = |c: &PlanExpr| match c {
        PlanExpr::Column { index, .. } => columns.get(*index).map(|c| &c.data_type),
        _ => None,
    };
    let lit_ok = |c: &PlanExpr, l: &PlanExpr| match (col_type(c), l) {
        (Some(ct), PlanExpr::Literal { value, .. }) => &value.data_type() == ct,
        _ => true,
    };
    match e {
        PlanExpr::BinaryOp { left, right, .. } => {
            lit_ok(left, right)
                && lit_ok(right, left)
                && literal_types_match(left, columns)
                && literal_types_match(right, columns)
        }
        PlanExpr::InList { expr, list, .. } => list.iter().all(|l| lit_ok(expr, l)),
        _ => true,
    }
}

#[async_trait]
impl DataSource for IcebergDataSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.columns.clone()
    }

    fn partition_count(&self) -> usize {
        (self.files.len() * self.splits_per_file).max(1)
    }

    async fn scan(
        &self,
        ctx: &ScanContext,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, ExecutionError> {
        let projection: Vec<usize> = match &ctx.projection {
            Some(p) => p.clone(),
            None => (0..self.columns.len()).collect(),
        };
        if let Some(bad) = projection.iter().find(|&&i| i >= self.columns.len()) {
            return Err(ExecutionError::InvalidOperation(format!(
                "IcebergDataSource: projection index {bad} out of range"
            )));
        }
        let output_schema = self.output_schema(Some(&projection));

        if self.files.is_empty() {
            return Ok(stream_from_batches(output_schema, vec![]));
        }
        let total = self.files.len() * self.splits_per_file;
        if partition >= total {
            return Err(ExecutionError::InvalidOperation(format!(
                "IcebergDataSource: partition {partition} out of range (have {total})"
            )));
        }
        let file = &self.files[partition / self.splits_per_file];
        let split_idx = partition % self.splits_per_file;

        // File-level pruning from manifest bounds / partition values.
        if file_can_be_skipped(
            &file.data_file,
            self.partition_specs.get(&file.spec_id),
            &self.column_fields,
            &ctx.filters,
        ) {
            debug!(file = %file.data_file.file_path, "Iceberg file pruned by manifest stats");
            return Ok(stream_from_batches(output_schema, vec![]));
        }

        let uri = &file.data_file.file_path;
        let (store, path) = resolve(&self.storage, uri)
            .map_err(|e| ExecutionError::InvalidOperation(e.to_string()))?;
        let meta = store.head(&path).await.map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "failed to stat Iceberg data file '{uri}': {e}"
            ))
        })?;
        let reader = ParquetObjectReader::new(store, meta.location).with_file_size(meta.size);
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| {
                ExecutionError::InvalidOperation(format!("Parquet reader error for '{uri}': {e}"))
            })?;

        let fmap = FileColumnMap::build(builder.parquet_schema(), &self.name_mapping);
        let file_arrow = builder.schema().clone();

        // Which file roots to read, and how to assemble output columns.
        let file_root_of = |table_idx: usize| -> Option<usize> {
            let (field_id, _) = self.column_fields.get(table_idx)?;
            fmap.root_by_field.get(field_id).copied()
        };
        let roots: BTreeSet<usize> = projection.iter().filter_map(|&i| file_root_of(i)).collect();
        let root_pos: HashMap<usize, usize> =
            roots.iter().enumerate().map(|(pos, &r)| (r, pos)).collect();
        let sources: Vec<ColumnSource> = projection
            .iter()
            .map(|&i| match file_root_of(i) {
                Some(root) => ColumnSource::File(root_pos[&root]),
                None => ColumnSource::Null,
            })
            .collect();

        // Filter pushdown: only for columns whose physical type equals the
        // table type, so literal comparisons in the Parquet predicates and
        // row-group statistics are exact.
        let leaf_for = |table_idx: usize| -> Option<usize> {
            let root = file_root_of(table_idx)?;
            let target: ArrowDataType = self.columns.get(table_idx)?.data_type.clone().into();
            if file_arrow.field(root).data_type() != &target {
                return None;
            }
            fmap.leaf_by_root.get(&root).copied()
        };
        let file_filters: Vec<PlanExpr> = ctx
            .filters
            .iter()
            .filter(|f| literal_types_match(f, &self.columns))
            .filter_map(|f| remap_filter(f, &leaf_for))
            .collect();

        let total_rows: usize = builder
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .sum();
        let mut selection = None;
        if !file_filters.is_empty() {
            let rgs = builder.metadata().row_groups().to_vec();
            let selected =
                arneb_connectors::parquet_pushdown::prune_row_groups(&rgs, &file_filters, &[]);
            if selected.len() < rgs.len() {
                selection = Some(parquet::arrow::arrow_reader::RowSelection::from(
                    arneb_hive::datasource::build_row_selection(&rgs, &selected),
                ));
            }
        }
        if self.splits_per_file > 1 {
            let slice = arneb_hive::datasource::compute_split_selection(
                total_rows,
                split_idx,
                self.splits_per_file,
            );
            selection = Some(match selection {
                Some(s) => s.intersection(&slice),
                None => slice,
            });
        }
        if let Some(sel) = selection {
            builder = builder.with_row_selection(sel);
        }
        if !file_filters.is_empty() {
            if let Some(rf) = arneb_connectors::parquet_pushdown::build_row_filter(
                &file_filters,
                builder.parquet_schema(),
            ) {
                builder = builder.with_row_filter(rf);
            }
        }
        let mask = ProjectionMask::roots(builder.parquet_schema(), roots.iter().copied());
        builder = builder.with_projection(mask);
        let batch_size = ctx
            .batch_size
            .unwrap_or_else(arneb_connectors::file::scan_default_batch_size);
        builder = builder.with_batch_size(batch_size);
        let inner = builder.build().map_err(|e| {
            ExecutionError::InvalidOperation(format!("Parquet reader build error for '{uri}': {e}"))
        })?;

        Ok(Box::pin(IcebergBatchStream {
            schema: output_schema,
            inner: Box::pin(inner),
            sources,
            file_path: uri.clone(),
        }))
    }
}

/// Adapts the Parquet stream of one file to the table schema.
struct IcebergBatchStream {
    schema: SchemaRef,
    inner: Pin<Box<ParquetRecordBatchStream<ParquetObjectReader>>>,
    sources: Vec<ColumnSource>,
    file_path: String,
}

impl IcebergBatchStream {
    fn adapt(&self, batch: RecordBatch) -> Result<RecordBatch, ArnebError> {
        let n = batch.num_rows();
        let columns: Vec<ArrayRef> = self
            .sources
            .iter()
            .zip(self.schema.fields())
            .map(|(src, field)| -> Result<ArrayRef, ArnebError> {
                match src {
                    ColumnSource::Null => Ok(new_null_array(field.data_type(), n)),
                    ColumnSource::File(pos) => {
                        let col = batch.column(*pos);
                        if col.data_type() == field.data_type() {
                            Ok(col.clone())
                        } else {
                            arrow::compute::cast(col, field.data_type()).map_err(|e| {
                                ExecutionError::InvalidOperation(format!(
                                    "Iceberg column '{}' in '{}': cannot read {} as {}: {e}",
                                    field.name(),
                                    self.file_path,
                                    col.data_type(),
                                    field.data_type()
                                ))
                                .into()
                            })
                        }
                    }
                }
            })
            .collect::<Result<_, _>>()?;
        RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "Iceberg batch assembly for '{}': {e}",
                self.file_path
            ))
            .into()
        })
    }
}

impl Stream for IcebergBatchStream {
    type Item = Result<RecordBatch, ArnebError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(b))) => Poll::Ready(Some(self.adapt(b))),
            Poll::Ready(Some(Err(e))) => {
                let msg = format!("Parquet read error for '{}': {e}", self.file_path);
                Poll::Ready(Some(Err(ExecutionError::InvalidOperation(msg).into())))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for IcebergBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// IcebergConnectorFactory
// ---------------------------------------------------------------------------

/// Connector factory for Iceberg tables.
///
/// Reads the metadata location and pinned snapshot from the table
/// properties produced by [`crate::catalog::IcebergTableProvider`], plans
/// the scan, and returns an [`IcebergDataSource`]. Parsed metadata is
/// cached by location (metadata files are immutable once written).
pub struct IcebergConnectorFactory {
    storage: Arc<StorageRegistry>,
    metadata_cache: RwLock<HashMap<String, Arc<TableMetadata>>>,
}

/// Upper bound on cached metadata documents before the cache is reset.
const METADATA_CACHE_CAPACITY: usize = 256;

impl IcebergConnectorFactory {
    /// Create a factory that reads through `storage`.
    pub fn new(storage: Arc<StorageRegistry>) -> Self {
        Self {
            storage,
            metadata_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Load metadata, consulting the cache first.
    pub async fn metadata(&self, location: &str) -> Result<Arc<TableMetadata>, ConnectorError> {
        if let Some(md) = self.metadata_cache.read().unwrap().get(location) {
            return Ok(md.clone());
        }
        let md = Arc::new(load_metadata(&self.storage, location).await?);
        let mut cache = self.metadata_cache.write().unwrap();
        if cache.len() >= METADATA_CACHE_CAPACITY {
            cache.clear();
        }
        cache.insert(location.to_string(), md.clone());
        Ok(md)
    }
}

impl fmt::Debug for IcebergConnectorFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergConnectorFactory")
            .field(
                "cached_metadata",
                &self.metadata_cache.read().unwrap().len(),
            )
            .finish()
    }
}

#[async_trait]
impl ConnectorFactory for IcebergConnectorFactory {
    fn name(&self) -> &str {
        "iceberg"
    }

    async fn create_data_source(
        &self,
        table: &TableReference,
        _schema: &[ColumnInfo],
        properties: &HashMap<String, String>,
    ) -> Result<Arc<dyn DataSource>, ConnectorError> {
        if let Some(err) = properties.get(props::ERROR) {
            return Err(ConnectorError::UnsupportedOperation(err.clone()));
        }
        let location = properties.get(props::METADATA_LOCATION).ok_or_else(|| {
            ConnectorError::TableNotFound(format!(
                "Iceberg table '{table}' has no metadata_location property"
            ))
        })?;
        let metadata = self.metadata(location).await?;
        let snapshot_id = match properties.get(props::SNAPSHOT_ID) {
            Some(s) => Some(s.parse::<i64>().map_err(|_| {
                ConnectorError::ReadError(format!("invalid snapshot_id property '{s}'"))
            })?),
            None => metadata.current_snapshot_id,
        };
        let files = match snapshot_id {
            Some(id) => plan_files(&self.storage, &metadata, id, &table.to_string()).await?,
            None => Vec::new(),
        };
        Ok(Arc::new(IcebergDataSource::new(
            self.storage.clone(),
            &metadata,
            files,
        )))
    }
}
