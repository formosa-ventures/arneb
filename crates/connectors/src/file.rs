//! File-based connector: reads CSV and Parquet files from the local filesystem.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use trino_catalog::{CatalogProvider, SchemaProvider, TableProvider};
use trino_common::error::{ConnectorError, ExecutionError};
use trino_common::types::{ColumnInfo, TableReference};
use trino_execution::DataSource;

use crate::ConnectorFactory;

// ---------------------------------------------------------------------------
// FileFormat
// ---------------------------------------------------------------------------

/// Supported file formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    /// Comma-separated values.
    Csv,
    /// Apache Parquet columnar format.
    Parquet,
}

// ---------------------------------------------------------------------------
// CsvDataSource
// ---------------------------------------------------------------------------

/// Reads a CSV file and produces Arrow RecordBatches.
#[derive(Debug)]
pub struct CsvDataSource {
    path: PathBuf,
    column_schema: Vec<ColumnInfo>,
    arrow_schema: Arc<Schema>,
}

impl CsvDataSource {
    /// Creates a new CSV data source with an explicit schema.
    pub fn new(path: impl Into<PathBuf>, schema: Vec<ColumnInfo>) -> Self {
        let arrow_schema = column_info_to_arrow_schema(&schema);
        Self {
            path: path.into(),
            column_schema: schema,
            arrow_schema,
        }
    }
}

impl DataSource for CsvDataSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.column_schema.clone()
    }

    fn scan(&self) -> Result<Vec<RecordBatch>, ExecutionError> {
        let file = std::fs::File::open(&self.path).map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "failed to open CSV file '{}': {}",
                self.path.display(),
                e
            ))
        })?;

        let reader = arrow_csv::ReaderBuilder::new(self.arrow_schema.clone())
            .with_header(true)
            .build(file)
            .map_err(|e| ExecutionError::InvalidOperation(format!("CSV reader error: {e}")))?;

        let mut batches = Vec::new();
        for result in reader {
            batches.push(result?);
        }
        Ok(batches)
    }
}

// ---------------------------------------------------------------------------
// ParquetDataSource
// ---------------------------------------------------------------------------

/// Reads a Parquet file and produces Arrow RecordBatches.
pub struct ParquetDataSource {
    path: PathBuf,
    column_schema: Vec<ColumnInfo>,
}

impl ParquetDataSource {
    /// Creates a new Parquet data source, reading schema from file metadata.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, ConnectorError> {
        let path = path.into();
        let file = std::fs::File::open(&path).map_err(|e| {
            ConnectorError::ReadError(format!(
                "failed to open Parquet file '{}': {}",
                path.display(),
                e
            ))
        })?;

        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| ConnectorError::ReadError(format!("Parquet metadata error: {e}")))?;

        let arrow_schema = reader.schema();
        let column_schema = arrow_schema_to_column_info(arrow_schema)?;

        Ok(Self {
            path,
            column_schema,
        })
    }
}

impl fmt::Debug for ParquetDataSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetDataSource")
            .field("path", &self.path)
            .finish()
    }
}

impl DataSource for ParquetDataSource {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.column_schema.clone()
    }

    fn scan(&self) -> Result<Vec<RecordBatch>, ExecutionError> {
        let file = std::fs::File::open(&self.path).map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "failed to open Parquet file '{}': {}",
                self.path.display(),
                e
            ))
        })?;

        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| ExecutionError::InvalidOperation(format!("Parquet reader error: {e}")))?
            .build()
            .map_err(|e| {
                ExecutionError::InvalidOperation(format!("Parquet reader build error: {e}"))
            })?;

        let mut batches = Vec::new();
        for result in reader {
            batches.push(result?);
        }
        Ok(batches)
    }
}

// ---------------------------------------------------------------------------
// FileTable
// ---------------------------------------------------------------------------

/// Table metadata for a registered file.
#[derive(Debug)]
struct FileTableEntry {
    path: PathBuf,
    format: FileFormat,
    schema: Vec<ColumnInfo>,
}

/// A file-backed table exposing schema metadata via [`TableProvider`].
#[derive(Debug)]
pub struct FileTable {
    schema: Vec<ColumnInfo>,
}

impl FileTable {
    /// Creates a new file table with the given schema.
    pub fn new(schema: Vec<ColumnInfo>) -> Self {
        Self { schema }
    }
}

impl TableProvider for FileTable {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// FileConnectorFactory
// ---------------------------------------------------------------------------

/// Factory that creates data sources from registered file tables.
pub struct FileConnectorFactory {
    tables: RwLock<HashMap<String, FileTableEntry>>,
}

impl FileConnectorFactory {
    /// Creates a new file connector factory.
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a file as a named table.
    ///
    /// For CSV files, `schema` must be provided. For Parquet files, `schema`
    /// can be `None` and will be read from the file metadata.
    pub fn register_table(
        &self,
        name: impl Into<String>,
        path: impl Into<PathBuf>,
        format: FileFormat,
        schema: Option<Vec<ColumnInfo>>,
    ) -> Result<(), ConnectorError> {
        let path = path.into();
        let schema = match (format, schema) {
            (_, Some(s)) => s,
            (FileFormat::Parquet, None) => {
                // Read schema from Parquet metadata.
                let ds = ParquetDataSource::new(&path)?;
                ds.column_schema.clone()
            }
            (FileFormat::Csv, None) => {
                return Err(ConnectorError::UnsupportedOperation(
                    "CSV tables require an explicit schema".to_string(),
                ));
            }
        };
        self.tables.write().unwrap().insert(
            name.into(),
            FileTableEntry {
                path,
                format,
                schema,
            },
        );
        Ok(())
    }
}

impl Default for FileConnectorFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for FileConnectorFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tables = self.tables.read().unwrap();
        f.debug_struct("FileConnectorFactory")
            .field("tables", &tables.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ConnectorFactory for FileConnectorFactory {
    fn name(&self) -> &str {
        "file"
    }

    fn create_data_source(
        &self,
        table: &TableReference,
        _schema: &[ColumnInfo],
    ) -> Result<Arc<dyn DataSource>, ConnectorError> {
        let tables = self.tables.read().unwrap();
        let entry = tables.get(&table.table).ok_or_else(|| {
            ConnectorError::TableNotFound(format!("file table '{}' not registered", table.table))
        })?;

        match entry.format {
            FileFormat::Csv => {
                let ds = CsvDataSource::new(&entry.path, entry.schema.clone());
                Ok(Arc::new(ds))
            }
            FileFormat::Parquet => {
                let ds = ParquetDataSource::new(&entry.path)?;
                Ok(Arc::new(ds))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FileSchema / FileCatalog
// ---------------------------------------------------------------------------

/// A schema backed by the file connector's registered tables.
pub struct FileSchema {
    factory: Arc<FileConnectorFactory>,
}

impl FileSchema {
    /// Creates a schema view over a file connector factory.
    pub fn new(factory: Arc<FileConnectorFactory>) -> Self {
        Self { factory }
    }
}

impl fmt::Debug for FileSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileSchema").finish()
    }
}

impl SchemaProvider for FileSchema {
    fn table_names(&self) -> Vec<String> {
        self.factory
            .tables
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let tables = self.factory.tables.read().unwrap();
        tables
            .get(name)
            .map(|e| Arc::new(FileTable::new(e.schema.clone())) as Arc<dyn TableProvider>)
    }
}

/// A catalog backed by a single file schema.
pub struct FileCatalog {
    schemas: HashMap<String, Arc<FileSchema>>,
}

impl FileCatalog {
    /// Creates a catalog with a single schema.
    pub fn new(schema_name: impl Into<String>, schema: Arc<FileSchema>) -> Self {
        let mut schemas = HashMap::new();
        schemas.insert(schema_name.into(), schema);
        Self { schemas }
    }
}

impl fmt::Debug for FileCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileCatalog")
            .field("schemas", &self.schemas.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CatalogProvider for FileCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .map(|s| Arc::clone(s) as Arc<dyn SchemaProvider>)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn column_info_to_arrow_schema(columns: &[ColumnInfo]) -> Arc<Schema> {
    let fields: Vec<arrow::datatypes::Field> = columns.iter().map(|c| c.clone().into()).collect();
    Arc::new(Schema::new(fields))
}

fn arrow_schema_to_column_info(schema: &Schema) -> Result<Vec<ColumnInfo>, ConnectorError> {
    schema
        .fields()
        .iter()
        .map(|f| {
            let data_type = trino_common::types::DataType::try_from(f.data_type().clone())
                .map_err(|e| {
                    ConnectorError::UnsupportedOperation(format!(
                        "unsupported Arrow type in Parquet file: {e}"
                    ))
                })?;
            Ok(ColumnInfo {
                name: f.name().clone(),
                data_type,
                nullable: f.is_nullable(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field};
    use std::io::Write;
    use std::path::Path;
    use trino_common::types::DataType;

    fn csv_schema() -> Vec<ColumnInfo> {
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

    fn write_test_csv(dir: &Path) -> PathBuf {
        let path = dir.join("test.csv");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "id,name").unwrap();
        writeln!(f, "1,alice").unwrap();
        writeln!(f, "2,bob").unwrap();
        writeln!(f, "3,carol").unwrap();
        path
    }

    fn write_test_parquet(dir: &Path) -> PathBuf {
        use parquet::arrow::arrow_writer::ArrowWriter;

        let path = dir.join("test.parquet");
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    // -- CSV tests --

    #[test]
    fn csv_data_source_reads_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_csv(dir.path());
        let ds = CsvDataSource::new(path, csv_schema());
        let batches = ds.scan().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn csv_data_source_file_not_found() {
        let ds = CsvDataSource::new("/nonexistent/path.csv", csv_schema());
        assert!(ds.scan().is_err());
    }

    // -- Parquet tests --

    #[test]
    fn parquet_data_source_reads_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(dir.path());
        let ds = ParquetDataSource::new(&path).unwrap();
        let batches = ds.scan().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn parquet_data_source_schema_from_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(dir.path());
        let ds = ParquetDataSource::new(&path).unwrap();
        let schema = ds.schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[1].name, "name");
    }

    #[test]
    fn parquet_data_source_file_not_found() {
        let result = ParquetDataSource::new("/nonexistent/path.parquet");
        assert!(result.is_err());
    }

    // -- FileConnectorFactory tests --

    #[test]
    fn file_factory_csv() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_csv(dir.path());
        let factory = FileConnectorFactory::new();
        factory
            .register_table("sales", path, FileFormat::Csv, Some(csv_schema()))
            .unwrap();

        let table_ref = TableReference::table("sales");
        let ds = factory.create_data_source(&table_ref, &[]).unwrap();
        let batches = ds.scan().unwrap();
        assert!(!batches.is_empty());
    }

    #[test]
    fn file_factory_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(dir.path());
        let factory = FileConnectorFactory::new();
        factory
            .register_table("events", path, FileFormat::Parquet, None)
            .unwrap();

        let table_ref = TableReference::table("events");
        let ds = factory.create_data_source(&table_ref, &[]).unwrap();
        let batches = ds.scan().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn file_factory_table_not_found() {
        let factory = FileConnectorFactory::new();
        let table_ref = TableReference::table("nope");
        let result = factory.create_data_source(&table_ref, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not registered"));
    }

    // -- FileSchema / FileCatalog tests --

    #[test]
    fn file_schema_and_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_csv(dir.path());
        let factory = Arc::new(FileConnectorFactory::new());
        factory
            .register_table("sales", path, FileFormat::Csv, Some(csv_schema()))
            .unwrap();

        let schema = Arc::new(FileSchema::new(factory));
        assert_eq!(schema.table_names().len(), 1);
        let tp = schema.table("sales").unwrap();
        assert_eq!(tp.schema().len(), 2);

        let catalog = FileCatalog::new("default", schema);
        assert_eq!(catalog.schema_names().len(), 1);
        assert!(catalog.schema("default").is_some());
    }

    // -- Integration tests --

    #[test]
    fn integration_memory_connector() {
        let catalog = Arc::new(MemoryCatalog::new());
        let mem_schema = Arc::new(MemorySchema::new());

        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "val",
            ArrowDataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(Int32Array::from(vec![10, 20]))])
                .unwrap();
        let table = Arc::new(MemoryTable::new(
            vec![ColumnInfo {
                name: "val".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            vec![batch],
        ));
        mem_schema.register_table("t", table);
        catalog.register_schema("default", mem_schema);

        let factory = MemoryConnectorFactory::new(catalog, "default");
        let ds = factory
            .create_data_source(&TableReference::table("t"), &[])
            .unwrap();
        let batches = ds.scan().unwrap();
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn integration_csv_connector() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_csv(dir.path());
        let ds = CsvDataSource::new(path, csv_schema());
        let batches = ds.scan().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // Verify column values.
        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
    }

    #[test]
    fn integration_parquet_connector() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_test_parquet(dir.path());
        let ds = ParquetDataSource::new(&path).unwrap();
        let batches = ds.scan().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        // Verify column values.
        let name_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "x");
    }

    // Need to re-import MemoryCatalog etc for integration tests
    use super::super::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
}
