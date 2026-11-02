//! Connector abstraction traits.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use arneb_common::error::ConnectorError;
use arneb_common::types::{ColumnInfo, TableReference};
use arneb_execution::DataSource;
use async_trait::async_trait;

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
#[async_trait]
pub trait ConnectorFactory: Send + Sync + Debug {
    /// Returns the connector type name (e.g., "memory", "file").
    fn name(&self) -> &str;

    /// Creates a data source for the given table.
    ///
    /// `properties` contains connector-specific metadata from the catalog
    /// (e.g., storage location for Hive tables). Connectors that don't
    /// need extra properties can ignore this parameter.
    ///
    /// `async` since multi-partition sources (Hive) must list their
    /// underlying files before reporting their partition count to the
    /// physical planner.
    async fn create_data_source(
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

/// Registry mapping connector names to their factories. Shared as
/// `Arc<ConnectorRegistry>`; factories can be registered and removed at
/// runtime through a shared reference. The lock is held only to clone or
/// swap an `Arc`, so lookups stay cheap.
///
/// A catalog swap changes a connector and the matching catalog provider
/// (held elsewhere, in a `CatalogManager`) together; [`swap`](Self::swap)
/// and [`pinned`](Self::pinned) exclude each other, so a statement that
/// pins both never sees one half-swapped.
#[derive(Debug, Default)]
pub struct ConnectorRegistry {
    factories: RwLock<HashMap<String, Arc<dyn ConnectorFactory>>>,
    /// Written across a [`swap`](Self::swap), read across a
    /// [`pinned`](Self::pinned) snapshot.
    swaps: RwLock<()>,
    /// The `(epoch, generation)` of the catalog set these factories belong
    /// to, as the runtime catalog manager tagged them; `None` until it
    /// does. A [`pinned`](Self::pinned) copy keeps the tag it was taken
    /// with.
    version: RwLock<Option<(u64, u64)>>,
}

impl ConnectorRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a connector factory under the given name, replacing any
    /// factory already registered under it.
    pub fn register(&self, name: impl Into<String>, factory: Arc<dyn ConnectorFactory>) {
        self.factories
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(name.into(), factory);
    }

    /// Removes the factory registered under `name`, returning it.
    pub fn deregister(&self, name: &str) -> Option<Arc<dyn ConnectorFactory>> {
        self.factories
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(name)
    }

    /// Returns the factory for the given connector name, if registered.
    pub fn get(&self, name: &str) -> Option<Arc<dyn ConnectorFactory>> {
        self.factories
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(name)
            .cloned()
    }

    /// Tags the registry with the `(epoch, generation)` of the catalog set
    /// it now holds. Call it inside the [`swap`](Self::swap) that installs
    /// the set, so a pinned copy never pairs one set's factories with
    /// another's tag.
    pub fn set_version(&self, version: Option<(u64, u64)>) {
        *self.version.write().unwrap_or_else(|p| p.into_inner()) = version;
    }

    /// The `(epoch, generation)` tag ([`set_version`](Self::set_version)).
    pub fn version(&self) -> Option<(u64, u64)> {
        *self.version.read().unwrap_or_else(|p| p.into_inner())
    }

    /// Runs `f`, which registers or removes connectors together with their
    /// catalog providers, with every [`pinned`](Self::pinned) snapshot
    /// excluded, so none sees the change half done.
    pub fn swap<R>(&self, f: impl FnOnce() -> R) -> R {
        let _swap = self.swaps.write().unwrap_or_else(|p| p.into_inner());
        f()
    }

    /// A frozen copy of the registered factories, taken together with what
    /// `f` snapshots (such as the catalog providers) and never in the
    /// middle of a [`swap`](Self::swap). A statement plans and executes
    /// against the pair, so a catalog change while it runs does not give
    /// it another connector than the provider it was planned with.
    pub fn pinned<R>(&self, f: impl FnOnce() -> R) -> (ConnectorRegistry, R) {
        let _swap = self.swaps.read().unwrap_or_else(|p| p.into_inner());
        let pinned = f();
        let factories = self
            .factories
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        (
            ConnectorRegistry {
                factories: RwLock::new(factories),
                swaps: RwLock::new(()),
                version: RwLock::new(self.version()),
            },
            pinned,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct DummyFactory;

    #[async_trait]
    impl ConnectorFactory for DummyFactory {
        fn name(&self) -> &str {
            "dummy"
        }

        async fn create_data_source(
            &self,
            _table: &TableReference,
            _schema: &[ColumnInfo],
            _properties: &std::collections::HashMap<String, String>,
        ) -> Result<Arc<dyn DataSource>, ConnectorError> {
            Err(ConnectorError::UnsupportedOperation("dummy".to_string()))
        }
    }

    #[test]
    fn pinned_copy_keeps_the_factories_it_was_taken_with() {
        let registry = ConnectorRegistry::new();
        let old: Arc<dyn ConnectorFactory> = Arc::new(DummyFactory);
        registry.register("c", Arc::clone(&old));
        registry.set_version(Some((1, 4)));
        let (pinned, seen) = registry.pinned(|| 7);
        assert_eq!(seen, 7);
        registry.swap(|| {
            registry.register("c", Arc::new(DummyFactory));
            registry.register("d", Arc::new(DummyFactory));
            registry.set_version(Some((1, 5)));
        });
        assert_eq!(pinned.version(), Some((1, 4)));
        assert_eq!(registry.version(), Some((1, 5)));
        assert!(Arc::ptr_eq(&pinned.get("c").unwrap(), &old));
        assert!(pinned.get("d").is_none());
        assert!(!Arc::ptr_eq(&registry.get("c").unwrap(), &old));
    }

    #[test]
    fn registry_register_and_get() {
        let registry = ConnectorRegistry::new();
        let factory: Arc<dyn ConnectorFactory> = Arc::new(DummyFactory);
        registry.register("dummy", factory);
        let retrieved = registry.get("dummy");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name(), "dummy");
    }

    #[test]
    fn registry_register_and_deregister_through_shared_ref() {
        let registry = Arc::new(ConnectorRegistry::new());
        let shared = Arc::clone(&registry);
        shared.register("dummy", Arc::new(DummyFactory));
        assert_eq!(registry.get("dummy").unwrap().name(), "dummy");
        let removed = shared.deregister("dummy");
        assert_eq!(removed.unwrap().name(), "dummy");
        assert!(registry.get("dummy").is_none());
        assert!(shared.deregister("dummy").is_none());
    }

    #[test]
    fn registry_get_unregistered() {
        let registry = ConnectorRegistry::new();
        assert!(registry.get("nonexistent").is_none());
    }
}
