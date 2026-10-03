//! Iceberg scan planning and data source.
//!
//! Scan planning resolves the pinned snapshot's manifest list and
//! manifests into the set of live Parquet data files. Each file is then
//! read through the shared split-based Parquet scan
//! ([`arneb_connectors::parquet_scan`], also used by the Hive connector),
//! with one Iceberg-specific layer on top: columns are matched to the file
//! **by field ID**, not by name or position, so renamed, reordered, added,
//! and type-promoted columns read correctly.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use arrow::array::{new_null_array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::schema::types::SchemaDescriptor;
use tracing::debug;

use arneb_common::error::{ArnebError, ConnectorError, ExecutionError};
use arneb_common::stream::{stream_from_batches, SendableRecordBatchStream};
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_connectors::parquet_scan::{self, ParquetBatchStream};
use arneb_connectors::storage::{StorageRegistry, StorageUri};
use arneb_execution::{DataSource, ScanContext};
use arneb_planner::PlanExpr;

use crate::catalog::{METADATA_LOCATION_PARAM, SNAPSHOT_ID_PROP};
use crate::manifest::{
    read_manifest, read_manifest_list, DataFile, DataFileContent, EntryStatus, ManifestContent,
};
use crate::metadata::TableMetadata;

/// Manifests fetched concurrently during scan planning.
const MANIFEST_FETCH_CONCURRENCY: usize = 8;

// ---------------------------------------------------------------------------
// Object store helpers
// ---------------------------------------------------------------------------

/// Resolve a URI to its object store and path.
fn resolve(
    storage: &StorageRegistry,
    uri: &str,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath), ConnectorError> {
    let parsed = StorageUri::parse(uri)?;
    let store = storage.get_store(&parsed)?;
    Ok((store, parsed.object_path()))
}

/// Read a whole object.
async fn read_object(storage: &StorageRegistry, uri: &str) -> Result<Bytes, ConnectorError> {
    let (store, path) = resolve(storage, uri)?;
    let read_err = |e| ConnectorError::ReadError(format!("failed to read '{uri}': {e}"));
    store
        .get(&path)
        .await
        .map_err(read_err)?
        .bytes()
        .await
        .map_err(read_err)
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
) -> Result<Vec<DataFile>, ConnectorError> {
    let snapshot = metadata.snapshot(snapshot_id).ok_or_else(|| {
        ConnectorError::ReadError(format!(
            "Iceberg table {table_name}: snapshot {snapshot_id} not found in metadata"
        ))
    })?;
    let list = snapshot.manifest_list.as_deref().ok_or_else(|| {
        ConnectorError::UnsupportedOperation(format!(
            "Iceberg table {table_name}: snapshot {snapshot_id} has no manifest list \
             (legacy v1 inline manifests are not supported)"
        ))
    })?;
    let to_read: Vec<_> = read_manifest_list(&read_object(storage, list).await?)?
        .into_iter()
        .filter(|m| !m.provably_empty())
        .collect();

    let per_manifest: Vec<_> = futures::stream::iter(to_read.into_iter().map(|m| async move {
        let entries = read_manifest(&read_object(storage, &m.manifest_path).await?)?;
        Ok::<_, ConnectorError>((m.content, entries))
    }))
    .buffered(MANIFEST_FETCH_CONCURRENCY)
    .try_collect()
    .await?;

    let mut files = Vec::new();
    for (manifest_content, entries) in per_manifest {
        for entry in entries {
            if entry.status == EntryStatus::Deleted {
                continue;
            }
            let df = entry.data_file;
            if manifest_content == ManifestContent::Deletes || df.content != DataFileContent::Data {
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
            files.push(df);
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

/// Build the data source for an Iceberg table from the properties
/// produced by [`crate::IcebergTableProvider`]: loads the metadata, plans
/// the pinned snapshot, and returns an [`IcebergDataSource`].
pub async fn create_data_source(
    storage: &Arc<StorageRegistry>,
    table: &TableReference,
    properties: &HashMap<String, String>,
) -> Result<Arc<dyn DataSource>, ConnectorError> {
    let location = properties.get(METADATA_LOCATION_PARAM).ok_or_else(|| {
        ConnectorError::TableNotFound(format!(
            "Iceberg table '{table}' has no metadata_location property"
        ))
    })?;
    let metadata = load_metadata(storage, location).await?;
    let snapshot_id = match properties.get(SNAPSHOT_ID_PROP) {
        Some(s) => Some(s.parse::<i64>().map_err(|_| {
            ConnectorError::ReadError(format!("invalid snapshot_id property '{s}'"))
        })?),
        None => metadata.current_snapshot_id,
    };
    let files = match snapshot_id {
        Some(id) => plan_files(storage, &metadata, id, &table.to_string()).await?,
        None => Vec::new(),
    };
    Ok(Arc::new(IcebergDataSource::new(
        storage.clone(),
        &metadata,
        files,
    )))
}

// ---------------------------------------------------------------------------
// IcebergDataSource
// ---------------------------------------------------------------------------

/// Data source over the live data files of one Iceberg snapshot.
pub struct IcebergDataSource {
    storage: Arc<StorageRegistry>,
    /// Exposed schema (current Iceberg schema, supported columns only).
    columns: Vec<ColumnInfo>,
    /// Field ID of each exposed column (parallel to `columns`).
    field_ids: Vec<i32>,
    /// Name → field ID fallback for files written without field IDs.
    name_mapping: HashMap<String, i32>,
    files: Vec<DataFile>,
    splits_per_file: usize,
}

impl IcebergDataSource {
    /// Build a data source from planned files.
    pub fn new(
        storage: Arc<StorageRegistry>,
        metadata: &TableMetadata,
        files: Vec<DataFile>,
    ) -> Self {
        let (field_ids, columns) = metadata.current_schema.to_columns().0.into_iter().unzip();
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
            field_ids,
            name_mapping,
            splits_per_file: parquet_scan::splits_per_file(files.len()),
            files,
        }
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

/// Assemble a table-schema batch from a projected file batch: cast
/// promoted columns, null-fill columns the file does not have.
fn adapt_batch(
    schema: &SchemaRef,
    sources: &[ColumnSource],
    file_path: &str,
    batch: RecordBatch,
) -> Result<RecordBatch, ArnebError> {
    let n = batch.num_rows();
    let columns: Vec<ArrayRef> = sources
        .iter()
        .zip(schema.fields())
        .map(|(src, field)| -> Result<ArrayRef, ArnebError> {
            match src {
                ColumnSource::Null => Ok(new_null_array(field.data_type(), n)),
                ColumnSource::File(pos) => {
                    let col = batch.column(*pos);
                    if col.data_type() == field.data_type() {
                        return Ok(col.clone());
                    }
                    arrow::compute::cast(col, field.data_type()).map_err(|e| {
                        ExecutionError::InvalidOperation(format!(
                            "Iceberg column '{}' in '{file_path}': cannot read {} as {}: {e}",
                            field.name(),
                            col.data_type(),
                            field.data_type()
                        ))
                        .into()
                    })
                }
            }
        })
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map_err(|e| {
        ExecutionError::InvalidOperation(format!("Iceberg batch assembly for '{file_path}': {e}"))
            .into()
    })
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
        let output_schema: SchemaRef = Arc::new(Schema::new(
            projection
                .iter()
                .map(|&i| Field::from(self.columns[i].clone()))
                .collect::<Vec<_>>(),
        ));

        if self.files.is_empty() {
            return Ok(stream_from_batches(output_schema, vec![]));
        }
        let (file_idx, split_idx) = parquet_scan::split_index(
            "IcebergDataSource",
            partition,
            self.files.len(),
            self.splits_per_file,
        )?;
        let uri = &self.files[file_idx].file_path;
        let (store, path) = resolve(&self.storage, uri)
            .map_err(|e| ExecutionError::InvalidOperation(e.to_string()))?;
        let builder = parquet_scan::open_parquet_builder(&store, &path).await?;

        let fmap = FileColumnMap::build(builder.parquet_schema(), &self.name_mapping);
        let file_arrow = builder.schema().clone();

        // Which file roots to read, and how to assemble output columns.
        let file_root_of = |table_idx: usize| -> Option<usize> {
            let field_id = self.field_ids.get(table_idx)?;
            fmap.root_by_field.get(field_id).copied()
        };
        let roots: Vec<usize> = projection
            .iter()
            .filter_map(|&i| file_root_of(i))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let sources: Vec<ColumnSource> = projection
            .iter()
            .map(|&i| match file_root_of(i) {
                Some(root) => ColumnSource::File(roots.binary_search(&root).unwrap_or_default()),
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

        let stream = parquet_scan::build_split_stream(
            builder,
            uri,
            &file_filters,
            Some(&roots),
            ctx.batch_size,
            split_idx,
            self.splits_per_file,
        )?;
        let schema = output_schema.clone();
        let file_path = uri.clone();
        Ok(Box::pin(
            ParquetBatchStream::new(output_schema, stream, uri.clone()).with_adapter(Box::new(
                move |batch| adapt_batch(&schema, &sources, &file_path, batch),
            )),
        ))
    }
}
