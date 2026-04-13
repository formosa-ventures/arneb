//! Connector abstraction traits.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arneb_common::error::ConnectorError;
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_execution::DataSource;

/// A provider for DDL/DML operations (create, drop, insert, delete).
///
/// Connectors that support write operations implement this trait.
/// Read-only connectors do not need to implement it.
pub trait DDLProvider: Send + Sync + Debug {
    /// Create a new table with the given schema.
    fn create_table(&self, name: &str, schema: &[ColumnInfo]) -> Result<(), ConnectorError>;

    /// Drop an existing table.
    fn drop_table(&self, name: &str) -> Result<(), ConnectorError>;

    /// Insert record batches into an existing table. Returns the number of rows inserted.
    fn insert_into(
        &self,
        name: &str,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Result<u64, ConnectorError>;

    /// Delete rows matching a predicate. None means delete all. Returns the number of rows deleted.
    fn delete_from(&self, name: &str, predicate: Option<&str>) -> Result<u64, ConnectorError>;

    /// Create a table and populate it with the given record batches.
    fn create_table_as_select(
        &self,
        name: &str,
        batches: Vec<arrow::record_batch::RecordBatch>,
    ) -> Result<(), ConnectorError>;
}

/// Factory that creates [`DataSource`] instances for a connector type.
pub trait ConnectorFactory: Send + Sync + Debug {
    /// Returns the connector type name (e.g., "memory", "file").
    fn name(&self) -> &str;

    /// Creates a data source for the given table.
    ///
    /// `properties` contains connector-specific metadata from the catalog
    /// (e.g., storage location for Hive tables). Connectors that don't
    /// need extra properties can ignore this parameter.
    fn create_data_source(
        &self,
        table: &TableReference,
        schema: &[ColumnInfo],
        properties: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn DataSource>, ConnectorError>;

    /// Returns the DDL provider for this connector, if it supports write operations.
    fn ddl_provider(&self) -> Option<Arc<dyn DDLProvider>> {
        None
    }
}

/// Registry mapping connector names to their factories.
#[derive(Debug, Default)]
pub struct ConnectorRegistry {
    factories: HashMap<String, Arc<dyn ConnectorFactory>>,
}

impl ConnectorRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a connector factory under the given name.
    pub fn register(&mut self, name: impl Into<String>, factory: Arc<dyn ConnectorFactory>) {
        self.factories.insert(name.into(), factory);
    }

    /// Returns the factory for the given connector name, if registered.
    pub fn get(&self, name: &str) -> Option<Arc<dyn ConnectorFactory>> {
        self.factories.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct DummyFactory;

    impl ConnectorFactory for DummyFactory {
        fn name(&self) -> &str {
            "dummy"
        }

        fn create_data_source(
            &self,
            _table: &TableReference,
            _schema: &[ColumnInfo],
            _properties: &std::collections::HashMap<String, String>,
        ) -> Result<Arc<dyn DataSource>, ConnectorError> {
            Err(ConnectorError::UnsupportedOperation("dummy".to_string()))
        }
    }

    #[test]
    fn registry_register_and_get() {
        let mut registry = ConnectorRegistry::new();
        let factory: Arc<dyn ConnectorFactory> = Arc::new(DummyFactory);
        registry.register("dummy", factory);
        let retrieved = registry.get("dummy");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name(), "dummy");
    }

    #[test]
    fn registry_get_unregistered() {
        let registry = ConnectorRegistry::new();
        assert!(registry.get("nonexistent").is_none());
    }
}
