//! In-memory connector: catalog + data source backed by pre-built RecordBatches.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use arneb_catalog::{CatalogProvider, SchemaProvider, TableProvider};
use arneb_common::error::ConnectorError;
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_execution::{DataSource, InMemoryDataSource};
use arrow::array::RecordBatch;
use async_trait::async_trait;

use arrow::datatypes::DataType as ArrowDataType;

use crate::{ConnectorFactory, DDLProvider};

fn arrow_type_to_data_type(dt: &ArrowDataType) -> arneb_common::types::DataType {
    use arneb_common::types::DataType;
    match dt {
        ArrowDataType::Boolean => DataType::Boolean,
        ArrowDataType::Int8 => DataType::Int8,
        ArrowDataType::Int16 => DataType::Int16,
        ArrowDataType::Int32 => DataType::Int32,
        ArrowDataType::Int64 => DataType::Int64,
        ArrowDataType::Float32 => DataType::Float32,
        ArrowDataType::Float64 => DataType::Float64,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::Utf8,
        ArrowDataType::Date32 => DataType::Date32,
        _ => DataType::Utf8, // fallback
    }
}

// ---------------------------------------------------------------------------
// MemoryTable
// ---------------------------------------------------------------------------

/// A table backed by in-memory RecordBatches.
#[derive(Debug, Clone)]
pub struct MemoryTable {
    schema: Vec<ColumnInfo>,
    batches: Vec<RecordBatch>,
}

impl MemoryTable {
    /// Creates a new memory table with the given schema and data.
    pub fn new(schema: Vec<ColumnInfo>, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }

    /// Returns the stored batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
}

impl TableProvider for MemoryTable {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// MemorySchema
// ---------------------------------------------------------------------------

/// A schema namespace containing named memory tables.
pub struct MemorySchema {
    tables: RwLock<HashMap<String, Arc<MemoryTable>>>,
}

impl MemorySchema {
    /// Creates an empty schema.
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a table in this schema.
    pub fn register_table(&self, name: impl Into<String>, table: Arc<MemoryTable>) {
        self.tables.write().unwrap().insert(name.into(), table);
    }

    /// Returns the underlying `MemoryTable` by name (for connector factory use).
    pub fn get_memory_table(&self, name: &str) -> Option<Arc<MemoryTable>> {
        self.tables.read().unwrap().get(name).cloned()
    }
}

impl Default for MemorySchema {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MemorySchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tables = self.tables.read().unwrap();
        f.debug_struct("MemorySchema")
            .field("tables", &tables.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for MemorySchema {
    async fn table_names(&self) -> Vec<String> {
        self.tables.read().unwrap().keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables
            .read()
            .unwrap()
            .get(name)
            .map(|t| Arc::clone(t) as Arc<dyn TableProvider>)
    }
}

// ---------------------------------------------------------------------------
// MemoryCatalog
// ---------------------------------------------------------------------------

/// A catalog containing named memory schemas.
pub struct MemoryCatalog {
    schemas: RwLock<HashMap<String, Arc<MemorySchema>>>,
}

impl MemoryCatalog {
    /// Creates an empty catalog.
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a schema in this catalog.
    pub fn register_schema(&self, name: impl Into<String>, schema: Arc<MemorySchema>) {
        self.schemas.write().unwrap().insert(name.into(), schema);
    }

    /// Returns the underlying `MemorySchema` by name (for connector factory use).
    pub fn get_memory_schema(&self, name: &str) -> Option<Arc<MemorySchema>> {
        self.schemas.read().unwrap().get(name).cloned()
    }
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MemoryCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let schemas = self.schemas.read().unwrap();
        f.debug_struct("MemoryCatalog")
            .field("schemas", &schemas.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[async_trait]
impl CatalogProvider for MemoryCatalog {
    async fn schema_names(&self) -> Vec<String> {
        self.schemas.read().unwrap().keys().cloned().collect()
    }

    async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .read()
            .unwrap()
            .get(name)
            .map(|s| Arc::clone(s) as Arc<dyn SchemaProvider>)
    }
}

// ---------------------------------------------------------------------------
// MemoryConnectorFactory
// ---------------------------------------------------------------------------

/// Factory that creates data sources from in-memory tables.
pub struct MemoryConnectorFactory {
    catalog: Arc<MemoryCatalog>,
    default_schema: String,
}

impl MemoryConnectorFactory {
    /// Creates a new factory backed by the given catalog.
    pub fn new(catalog: Arc<MemoryCatalog>, default_schema: impl Into<String>) -> Self {
        Self {
            catalog,
            default_schema: default_schema.into(),
        }
    }
}

impl fmt::Debug for MemoryConnectorFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryConnectorFactory")
            .field("default_schema", &self.default_schema)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// MemoryDDLProvider
// ---------------------------------------------------------------------------

/// DDL provider for in-memory tables.
#[derive(Debug)]
pub struct MemoryDDLProvider {
    catalog: Arc<MemoryCatalog>,
    default_schema: String,
}

impl MemoryDDLProvider {
    fn get_schema(&self) -> Result<Arc<MemorySchema>, ConnectorError> {
        self.catalog
            .get_memory_schema(&self.default_schema)
            .ok_or_else(|| {
                ConnectorError::TableNotFound(format!("schema '{}' not found", self.default_schema))
            })
    }
}

impl DDLProvider for MemoryDDLProvider {
    fn create_table(&self, name: &str, schema: &[ColumnInfo]) -> Result<(), ConnectorError> {
        let mem_schema = self.get_schema()?;
        {
            let tables = mem_schema.tables.read().unwrap();
            if tables.contains_key(name) {
                return Err(ConnectorError::TableNotFound(format!(
                    "table '{name}' already exists"
                )));
            }
        }
        let table = Arc::new(MemoryTable::new(schema.to_vec(), vec![]));
        mem_schema.register_table(name, table);
        Ok(())
    }

    fn drop_table(&self, name: &str) -> Result<(), ConnectorError> {
        let mem_schema = self.get_schema()?;
        let mut tables = mem_schema.tables.write().unwrap();
        if tables.remove(name).is_none() {
            return Err(ConnectorError::TableNotFound(format!(
                "table '{name}' not found"
            )));
        }
        Ok(())
    }

    fn insert_into(&self, name: &str, batches: Vec<RecordBatch>) -> Result<u64, ConnectorError> {
        let mem_schema = self.get_schema()?;
        let mut tables = mem_schema.tables.write().unwrap();
        let table = tables
            .get_mut(name)
            .ok_or_else(|| ConnectorError::TableNotFound(format!("table '{name}' not found")))?;

        let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let mut existing = table.batches.clone();
        existing.extend(batches);
        *table = Arc::new(MemoryTable::new(table.schema.clone(), existing));
        Ok(row_count)
    }

    fn delete_from(&self, name: &str, _predicate: Option<&str>) -> Result<u64, ConnectorError> {
        let mem_schema = self.get_schema()?;
        let mut tables = mem_schema.tables.write().unwrap();
        let table = tables
            .get_mut(name)
            .ok_or_else(|| ConnectorError::TableNotFound(format!("table '{name}' not found")))?;

        // Simple implementation: if no predicate, delete all rows
        let row_count: u64 = table.batches.iter().map(|b| b.num_rows() as u64).sum();
        *table = Arc::new(MemoryTable::new(table.schema.clone(), vec![]));
        Ok(row_count)
    }

    fn create_table_as_select(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), ConnectorError> {
        let mem_schema = self.get_schema()?;
        {
            let tables = mem_schema.tables.read().unwrap();
            if tables.contains_key(name) {
                return Err(ConnectorError::TableNotFound(format!(
                    "table '{name}' already exists"
                )));
            }
        }
        let schema = if batches.is_empty() {
            vec![]
        } else {
            batches[0]
                .schema()
                .fields()
                .iter()
                .map(|f| ColumnInfo {
                    name: f.name().clone(),
                    data_type: arrow_type_to_data_type(f.data_type()),
                    nullable: f.is_nullable(),
                })
                .collect()
        };
        let table = Arc::new(MemoryTable::new(schema, batches));
        mem_schema.register_table(name, table);
        Ok(())
    }
}

impl ConnectorFactory for MemoryConnectorFactory {
    fn name(&self) -> &str {
        "memory"
    }

    fn ddl_provider(&self) -> Option<Arc<dyn DDLProvider>> {
        Some(Arc::new(MemoryDDLProvider {
            catalog: Arc::clone(&self.catalog),
            default_schema: self.default_schema.clone(),
        }))
    }

    fn create_data_source(
        &self,
        table: &TableReference,
        _schema: &[ColumnInfo],
        _properties: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn DataSource>, ConnectorError> {
        let schema_name = table.schema.as_deref().unwrap_or(&self.default_schema);

        let mem_schema = self.catalog.get_memory_schema(schema_name).ok_or_else(|| {
            ConnectorError::TableNotFound(format!(
                "schema '{}' not found in memory catalog",
                schema_name
            ))
        })?;

        let mem_table = mem_schema.get_memory_table(&table.table).ok_or_else(|| {
            ConnectorError::TableNotFound(format!(
                "table '{}' not found in schema '{}'",
                table.table, schema_name
            ))
        })?;

        let ds = InMemoryDataSource::new(mem_table.schema.clone(), mem_table.batches.clone());
        Ok(Arc::new(ds))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::DataType;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};

    fn test_table() -> Arc<MemoryTable> {
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        Arc::new(MemoryTable::new(
            vec![ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            vec![batch],
        ))
    }

    #[test]
    fn memory_table_schema() {
        let table = test_table();
        let schema = TableProvider::schema(table.as_ref());
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].name, "id");
    }

    #[tokio::test]
    async fn memory_schema_register_and_lookup() {
        let schema = MemorySchema::new();
        schema.register_table("users", test_table());
        assert_eq!(schema.table_names().await.len(), 1);
        assert!(schema.table("users").await.is_some());
        assert!(schema.table("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn memory_catalog_register_and_lookup() {
        let catalog = MemoryCatalog::new();
        let schema = Arc::new(MemorySchema::new());
        catalog.register_schema("default", schema);
        assert_eq!(catalog.schema_names().await.len(), 1);
        assert!(catalog.schema("default").await.is_some());
        assert!(catalog.schema("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn memory_factory_create_data_source() {
        let catalog = Arc::new(MemoryCatalog::new());
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("users", test_table());
        catalog.register_schema("default", schema);

        let factory = MemoryConnectorFactory::new(catalog, "default");
        let table_ref = TableReference::table("users");
        let ds = factory
            .create_data_source(&table_ref, &[], &Default::default())
            .unwrap();
        let stream = ds
            .scan(&arneb_execution::ScanContext::default())
            .await
            .unwrap();
        let batches = arneb_common::stream::collect_stream(stream).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn memory_factory_table_not_found() {
        let catalog = Arc::new(MemoryCatalog::new());
        let schema = Arc::new(MemorySchema::new());
        catalog.register_schema("default", schema);

        let factory = MemoryConnectorFactory::new(catalog, "default");
        let table_ref = TableReference::table("nonexistent");
        let result = factory.create_data_source(&table_ref, &[], &Default::default());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }
}
