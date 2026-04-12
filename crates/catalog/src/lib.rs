//! Catalog system for the arneb query engine.
//!
//! Provides trait-based abstractions for catalog metadata access and
//! in-memory implementations for MVP usage. The catalog system resolves
//! table references (`catalog.schema.table`) to column schema metadata.

#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![deny(unsafe_code)]

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use arneb_common::error::CatalogError;
use arneb_common::types::{ColumnInfo, TableReference};
use async_trait::async_trait;

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

/// A catalog is a top-level namespace containing schemas.
///
/// Connectors implement this trait to expose their schema hierarchy.
#[async_trait]
pub trait CatalogProvider: fmt::Debug + Send + Sync {
    /// Returns the names of all schemas in this catalog.
    async fn schema_names(&self) -> Vec<String>;

    /// Returns the schema with the given name, or `None` if it does not exist.
    async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;
}

/// A schema is a namespace within a catalog containing tables.
#[async_trait]
pub trait SchemaProvider: fmt::Debug + Send + Sync {
    /// Returns the names of all tables in this schema.
    async fn table_names(&self) -> Vec<String>;

    /// Returns the table with the given name, or `None` if it does not exist.
    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>>;
}

/// A table exposes its column schema metadata.
///
/// In the MVP, this only provides column information. Data access methods
/// (e.g., `scan()`) will be added when the execution engine is built.
pub trait TableProvider: fmt::Debug + Send + Sync {
    /// Returns the column schema for this table.
    fn schema(&self) -> Vec<ColumnInfo>;

    /// Returns connector-specific properties (e.g., storage location, format).
    ///
    /// These properties are carried through the logical plan and passed to
    /// the connector factory at data source creation time. Default: empty.
    fn properties(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

// ---------------------------------------------------------------------------
// In-memory implementations
// ---------------------------------------------------------------------------

/// An in-memory table storing a fixed column schema.
#[derive(Debug, Clone)]
pub struct MemoryTable {
    columns: Vec<ColumnInfo>,
}

impl MemoryTable {
    /// Creates a new in-memory table with the given column schema.
    pub fn new(columns: Vec<ColumnInfo>) -> Self {
        Self { columns }
    }
}

impl TableProvider for MemoryTable {
    fn schema(&self) -> Vec<ColumnInfo> {
        self.columns.clone()
    }
}

/// An in-memory schema storing tables by name.
#[derive(Debug)]
pub struct MemorySchema {
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl MemorySchema {
    /// Creates a new empty in-memory schema.
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a table in this schema.
    pub fn register_table(&self, name: impl Into<String>, table: Arc<dyn TableProvider>) {
        self.tables.write().unwrap().insert(name.into(), table);
    }

    /// Removes a table from this schema, returning the removed table if it existed.
    pub fn deregister_table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.write().unwrap().remove(name)
    }
}

impl Default for MemorySchema {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchemaProvider for MemorySchema {
    async fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        let mut names: Vec<String> = tables.keys().cloned().collect();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.read().unwrap().get(name).cloned()
    }
}

/// An in-memory catalog storing schemas by name.
#[derive(Debug)]
pub struct MemoryCatalog {
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl MemoryCatalog {
    /// Creates a new empty in-memory catalog.
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a schema in this catalog.
    pub fn register_schema(&self, name: impl Into<String>, schema: Arc<dyn SchemaProvider>) {
        self.schemas.write().unwrap().insert(name.into(), schema);
    }

    /// Removes a schema from this catalog, returning the removed schema if it existed.
    pub fn deregister_schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.write().unwrap().remove(name)
    }
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CatalogProvider for MemoryCatalog {
    async fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read().unwrap();
        let mut names: Vec<String> = schemas.keys().cloned().collect();
        names.sort();
        names
    }

    async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.read().unwrap().get(name).cloned()
    }
}

// ---------------------------------------------------------------------------
// CatalogManager
// ---------------------------------------------------------------------------

/// Top-level catalog manager that holds registered catalogs and resolves
/// table references using configurable default catalog and schema names.
#[derive(Debug)]
pub struct CatalogManager {
    catalogs: RwLock<HashMap<String, Arc<dyn CatalogProvider>>>,
    default_catalog: String,
    default_schema: String,
}

impl CatalogManager {
    /// Creates a new `CatalogManager` with the given default catalog and schema names.
    pub fn new(default_catalog: impl Into<String>, default_schema: impl Into<String>) -> Self {
        Self {
            catalogs: RwLock::new(HashMap::new()),
            default_catalog: default_catalog.into(),
            default_schema: default_schema.into(),
        }
    }

    /// Returns the default catalog name.
    pub fn default_catalog(&self) -> &str {
        &self.default_catalog
    }

    /// Returns the default schema name.
    pub fn default_schema(&self) -> &str {
        &self.default_schema
    }

    /// Registers a catalog with the given name.
    pub fn register_catalog(&self, name: impl Into<String>, catalog: Arc<dyn CatalogProvider>) {
        self.catalogs.write().unwrap().insert(name.into(), catalog);
    }

    /// Removes a catalog, returning the removed catalog if it existed.
    pub fn deregister_catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.write().unwrap().remove(name)
    }

    /// Returns the catalog with the given name, or `None` if not registered.
    pub fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.read().unwrap().get(name).cloned()
    }

    /// Returns the names of all registered catalogs.
    pub fn catalog_names(&self) -> Vec<String> {
        let catalogs = self.catalogs.read().unwrap();
        let mut names: Vec<String> = catalogs.keys().cloned().collect();
        names.sort();
        names
    }

    /// Resolves a [`TableReference`] to a [`TableProvider`].
    ///
    /// Resolution logic:
    /// - Three-part (`catalog.schema.table`): use all parts as-is
    /// - Two-part (`schema.table`): use default catalog + provided schema
    /// - One-part (`table`): use default catalog + default schema
    pub async fn resolve_table(
        &self,
        reference: &TableReference,
    ) -> Result<Arc<dyn TableProvider>, CatalogError> {
        let catalog_name = reference
            .catalog
            .as_deref()
            .unwrap_or(&self.default_catalog);
        let schema_name = reference.schema.as_deref().unwrap_or(&self.default_schema);
        let table_name = &reference.table;

        let catalog = {
            let catalogs = self.catalogs.read().unwrap();
            catalogs
                .get(catalog_name)
                .cloned()
                .ok_or_else(|| CatalogError::CatalogNotFound(catalog_name.to_string()))?
        };

        let schema = catalog
            .schema(schema_name)
            .await
            .ok_or_else(|| CatalogError::SchemaNotFound(format!("{catalog_name}.{schema_name}")))?;

        schema.table(table_name).await.ok_or_else(|| {
            CatalogError::SchemaNotFound(format!("{catalog_name}.{schema_name}.{table_name}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::DataType;

    // -- Helper --

    fn test_columns() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ]
    }

    // -- MemoryTable tests --

    #[test]
    fn memory_table_schema() {
        let table = MemoryTable::new(test_columns());
        let schema = table.schema();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[0].data_type, DataType::Int64);
        assert!(!schema[0].nullable);
        assert_eq!(schema[1].name, "name");
        assert_eq!(schema[1].data_type, DataType::Utf8);
        assert!(schema[1].nullable);
    }

    #[test]
    fn memory_table_empty() {
        let table = MemoryTable::new(vec![]);
        assert!(table.schema().is_empty());
    }

    #[test]
    fn memory_table_as_trait_object() {
        let table: Arc<dyn TableProvider> = Arc::new(MemoryTable::new(test_columns()));
        assert_eq!(table.schema().len(), 2);
    }

    // -- MemorySchema tests --

    #[tokio::test]
    async fn memory_schema_empty() {
        let schema = MemorySchema::new();
        assert!(schema.table_names().await.is_empty());
    }

    #[tokio::test]
    async fn memory_schema_register_table() {
        let schema = MemorySchema::new();
        let table = Arc::new(MemoryTable::new(test_columns()));
        schema.register_table("users", table);

        assert_eq!(schema.table_names().await, vec!["users".to_string()]);
        assert!(schema.table("users").await.is_some());
    }

    #[tokio::test]
    async fn memory_schema_deregister_table() {
        let schema = MemorySchema::new();
        let table = Arc::new(MemoryTable::new(test_columns()));
        schema.register_table("users", table);

        let removed = schema.deregister_table("users");
        assert!(removed.is_some());
        assert!(schema.table("users").await.is_none());
        assert!(schema.table_names().await.is_empty());
    }

    #[test]
    fn memory_schema_deregister_nonexistent() {
        let schema = MemorySchema::new();
        assert!(schema.deregister_table("nope").is_none());
    }

    #[tokio::test]
    async fn memory_schema_lookup_missing() {
        let schema = MemorySchema::new();
        assert!(schema.table("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn memory_schema_multiple_tables() {
        let schema = MemorySchema::new();
        schema.register_table("users", Arc::new(MemoryTable::new(test_columns())));
        schema.register_table("orders", Arc::new(MemoryTable::new(vec![])));

        let mut names = schema.table_names().await;
        names.sort();
        assert_eq!(names, vec!["orders".to_string(), "users".to_string()]);
    }

    // -- MemoryCatalog tests --

    #[tokio::test]
    async fn memory_catalog_empty() {
        let catalog = MemoryCatalog::new();
        assert!(catalog.schema_names().await.is_empty());
    }

    #[tokio::test]
    async fn memory_catalog_register_schema() {
        let catalog = MemoryCatalog::new();
        let schema = Arc::new(MemorySchema::new());
        catalog.register_schema("default", schema);

        assert_eq!(catalog.schema_names().await, vec!["default".to_string()]);
        assert!(catalog.schema("default").await.is_some());
    }

    #[tokio::test]
    async fn memory_catalog_deregister_schema() {
        let catalog = MemoryCatalog::new();
        let schema = Arc::new(MemorySchema::new());
        catalog.register_schema("default", schema);

        let removed = catalog.deregister_schema("default");
        assert!(removed.is_some());
        assert!(catalog.schema("default").await.is_none());
    }

    #[test]
    fn memory_catalog_deregister_nonexistent() {
        let catalog = MemoryCatalog::new();
        assert!(catalog.deregister_schema("nope").is_none());
    }

    #[tokio::test]
    async fn memory_catalog_lookup_missing() {
        let catalog = MemoryCatalog::new();
        assert!(catalog.schema("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn memory_catalog_multiple_schemas() {
        let catalog = MemoryCatalog::new();
        catalog.register_schema("default", Arc::new(MemorySchema::new()));
        catalog.register_schema("analytics", Arc::new(MemorySchema::new()));

        let mut names = catalog.schema_names().await;
        names.sort();
        assert_eq!(names, vec!["analytics".to_string(), "default".to_string()]);
    }

    // -- CatalogManager tests --

    fn setup_manager() -> CatalogManager {
        let manager = CatalogManager::new("memory", "default");

        let schema = Arc::new(MemorySchema::new());
        schema.register_table("users", Arc::new(MemoryTable::new(test_columns())));
        schema.register_table("orders", Arc::new(MemoryTable::new(vec![])));

        let catalog = Arc::new(MemoryCatalog::new());
        catalog.register_schema("default", schema);

        let analytics_schema = Arc::new(MemorySchema::new());
        analytics_schema.register_table(
            "events",
            Arc::new(MemoryTable::new(vec![ColumnInfo {
                name: "event_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            }])),
        );
        catalog.register_schema("analytics", analytics_schema);

        manager.register_catalog("memory", catalog);
        manager
    }

    #[test]
    fn catalog_manager_defaults() {
        let manager = CatalogManager::new("memory", "default");
        assert_eq!(manager.default_catalog(), "memory");
        assert_eq!(manager.default_schema(), "default");
    }

    #[test]
    fn catalog_manager_register_catalog() {
        let manager = CatalogManager::new("memory", "default");
        let catalog = Arc::new(MemoryCatalog::new());
        manager.register_catalog("memory", catalog);

        assert_eq!(manager.catalog_names(), vec!["memory".to_string()]);
        assert!(manager.catalog("memory").is_some());
    }

    #[test]
    fn catalog_manager_deregister_catalog() {
        let manager = CatalogManager::new("memory", "default");
        let catalog = Arc::new(MemoryCatalog::new());
        manager.register_catalog("memory", catalog);

        let removed = manager.deregister_catalog("memory");
        assert!(removed.is_some());
        assert!(manager.catalog("memory").is_none());
    }

    #[test]
    fn catalog_manager_catalog_names() {
        let manager = setup_manager();
        assert_eq!(manager.catalog_names(), vec!["memory".to_string()]);
    }

    #[tokio::test]
    async fn resolve_table_fully_qualified() {
        let manager = setup_manager();
        let reference = TableReference {
            catalog: Some("memory".to_string()),
            schema: Some("default".to_string()),
            table: "users".to_string(),
        };
        let table = manager.resolve_table(&reference).await.unwrap();
        assert_eq!(table.schema().len(), 2);
    }

    #[tokio::test]
    async fn resolve_table_two_part() {
        let manager = setup_manager();
        let reference = TableReference {
            catalog: None,
            schema: Some("analytics".to_string()),
            table: "events".to_string(),
        };
        let table = manager.resolve_table(&reference).await.unwrap();
        assert_eq!(table.schema().len(), 1);
        assert_eq!(table.schema()[0].name, "event_id");
    }

    #[tokio::test]
    async fn resolve_table_one_part() {
        let manager = setup_manager();
        let reference = TableReference::table("users");
        let table = manager.resolve_table(&reference).await.unwrap();
        assert_eq!(table.schema().len(), 2);
    }

    #[tokio::test]
    async fn resolve_table_catalog_not_found() {
        let manager = setup_manager();
        let reference = TableReference {
            catalog: Some("nonexistent".to_string()),
            schema: Some("default".to_string()),
            table: "users".to_string(),
        };
        let err = manager.resolve_table(&reference).await.unwrap_err();
        assert!(matches!(err, CatalogError::CatalogNotFound(_)));
    }

    #[tokio::test]
    async fn resolve_table_schema_not_found() {
        let manager = setup_manager();
        let reference = TableReference {
            catalog: Some("memory".to_string()),
            schema: Some("nonexistent".to_string()),
            table: "users".to_string(),
        };
        let err = manager.resolve_table(&reference).await.unwrap_err();
        assert!(matches!(err, CatalogError::SchemaNotFound(_)));
    }

    #[tokio::test]
    async fn resolve_table_table_not_found() {
        let manager = setup_manager();
        let reference = TableReference {
            catalog: Some("memory".to_string()),
            schema: Some("default".to_string()),
            table: "nonexistent".to_string(),
        };
        let err = manager.resolve_table(&reference).await.unwrap_err();
        // Table not found is reported as SchemaNotFound with the full path
        assert!(matches!(err, CatalogError::SchemaNotFound(_)));
        assert!(err.to_string().contains("nonexistent"));
    }

    // -- Thread safety tests --

    #[test]
    fn catalog_provider_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Arc<dyn CatalogProvider>>();
        assert_send_sync::<Arc<dyn SchemaProvider>>();
        assert_send_sync::<Arc<dyn TableProvider>>();
        assert_send_sync::<MemoryCatalog>();
        assert_send_sync::<MemorySchema>();
        assert_send_sync::<MemoryTable>();
        assert_send_sync::<CatalogManager>();
    }
}
