//! Connector abstraction traits.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use trino_common::error::ConnectorError;
use trino_common::types::{ColumnInfo, TableReference};
use trino_execution::DataSource;

/// Factory that creates [`DataSource`] instances for a connector type.
pub trait ConnectorFactory: Send + Sync + Debug {
    /// Returns the connector type name (e.g., "memory", "file").
    fn name(&self) -> &str;

    /// Creates a data source for the given table.
    fn create_data_source(
        &self,
        table: &TableReference,
        schema: &[ColumnInfo],
    ) -> Result<Arc<dyn DataSource>, ConnectorError>;
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
