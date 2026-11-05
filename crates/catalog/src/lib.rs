//! Catalog system for the arneb query engine.
//!
//! Provides trait-based abstractions for catalog metadata access and
//! in-memory implementations for MVP usage. The catalog system resolves
//! table references (`catalog.schema.table`) to column schema metadata.

#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![deny(unsafe_code)]

pub mod access;
pub mod conformance;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

pub use access::{AccessRules, CatalogAccess, RulesError};

use arneb_common::error::CatalogError;
use arneb_common::types::{ColumnInfo, ScalarValue, TableReference};
use async_trait::async_trait;

// ---------------------------------------------------------------------------
// Statistics carriers
// ---------------------------------------------------------------------------

/// Per-table statistics used by the cost-based planner and join reorderer.
///
/// Every field is nullable. Connectors that cannot provide statistics return
/// `TableStatistics::default()` (all `None`). The cost model and selectivity
/// estimator fall back to conservative defaults when fields are missing.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableStatistics {
    /// Approximate total row count.
    pub row_count: Option<u64>,
    /// On-disk byte size (compressed for Parquet/ORC).
    pub size_bytes: Option<u64>,
    /// Per-column distributional summaries, keyed by column name.
    pub columns: HashMap<String, ColumnStatistics>,
}

/// Per-column distributional summary.
///
/// All fields are best-effort approximations populated by the connector.
/// Missing fields make the selectivity estimator use conservative defaults.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnStatistics {
    /// Approximate number of distinct values (NDV).
    pub ndv: Option<u64>,
    /// Fraction of NULL values in `[0.0, 1.0]`.
    pub null_fraction: Option<f64>,
    /// Smallest observed value (used by range selectivity).
    pub min_value: Option<ScalarValue>,
    /// Largest observed value (used by range selectivity).
    pub max_value: Option<ScalarValue>,
}

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

    /// Like [`schema`](Self::schema), but tells "does not exist"
    /// (`Ok(None)`) apart from "could not ask the source" (`Err`, e.g. an
    /// unreachable metastore), so a caller choosing between schemas (such
    /// as `SET search_path`) fails instead of silently picking another.
    /// Default: `schema`, which never fails.
    async fn try_schema(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn SchemaProvider>>, CatalogError> {
        Ok(self.schema(name).await)
    }

    /// Like [`schema_names`](Self::schema_names), but `Err` when the source
    /// could not be asked, so a listing that failed is not mistaken for an
    /// empty catalog. Default: `schema_names`, which never fails.
    async fn try_schema_names(&self) -> Result<Vec<String>, CatalogError> {
        Ok(self.schema_names().await)
    }
}

/// What [`SchemaProvider::table_if_allowed`] found under a name.
#[derive(Debug, Clone)]
pub enum TableLookup {
    /// The table, whose stored name the gate allowed.
    Found(Arc<dyn TableProvider>),
    /// A table answers to the name, but the gate refused its stored
    /// `(schema, table)`. It was not loaded. Not the same as
    /// [`Missing`](Self::Missing): a caller choosing between names (the
    /// planner's case fold) must not move past a denied one.
    Denied,
    /// No table answers to the name.
    Missing,
}

impl TableLookup {
    /// The table when found; `None` when denied or missing.
    pub fn found(self) -> Option<Arc<dyn TableProvider>> {
        match self {
            TableLookup::Found(table) => Some(table),
            TableLookup::Denied | TableLookup::Missing => None,
        }
    }
}

/// Asked by [`SchemaProvider::table_if_allowed`] whether the session user
/// may read a table, given its stored `(schema, table)`.
pub type TableGate<'a> = &'a (dyn Fn(&str, &str) -> bool + Send + Sync);

/// A schema is a namespace within a catalog containing tables.
#[async_trait]
pub trait SchemaProvider: fmt::Debug + Send + Sync {
    /// Returns the names of all tables in this schema.
    async fn table_names(&self) -> Vec<String>;

    /// Like [`table_names`](Self::table_names), but `Err` when the source
    /// could not be asked, so a listing that failed is not mistaken for an
    /// empty schema. Default: `table_names`, which never fails.
    async fn try_table_names(&self) -> Result<Vec<String>, CatalogError> {
        Ok(self.table_names().await)
    }

    /// Returns the table with the given name, or `None` if it does not exist.
    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>>;

    /// Like [`table`](Self::table), but tells a table `allowed` refuses
    /// under its stored `(schema, table)` ([`TableLookup::Denied`]) apart
    /// from a missing one, and returns `Err` (rather than "missing") when
    /// the source could not be asked, as [`CatalogProvider::try_schema`]
    /// does. A connector whose lookup is expensive (partitions,
    /// table-format metadata) overrides it to ask `allowed` as soon as it
    /// knows the stored name, so a table the user may not read costs no
    /// more to refuse than a missing one. The caller checks the identity
    /// again afterwards. Default: `table`, then `allowed` on its
    /// [`TableProvider::canonical_name`]; never fails.
    async fn table_if_allowed(
        &self,
        name: &str,
        allowed: TableGate<'_>,
    ) -> Result<TableLookup, CatalogError> {
        let Some(table) = self.table(name).await else {
            return Ok(TableLookup::Missing);
        };
        Ok(match table.canonical_name() {
            Some((schema, stored)) if !allowed(&schema, &stored) => TableLookup::Denied,
            _ => TableLookup::Found(table),
        })
    }

    /// The schema's name as the source stores it. Access rules are checked
    /// on this name as well as on the name as written.
    ///
    /// **A connector whose [`CatalogProvider::schema`] accepts any name
    /// other than the exact stored one (case-insensitive matching, or any
    /// other normalization) MUST return `Some`**, or access rules on the
    /// stored name could be bypassed by writing it differently. `None`
    /// claims the provider answers only to its exact stored name; the
    /// catalog manager holds it to that by finding the name as written in
    /// [`CatalogProvider::schema_names`], and treats the schema as missing
    /// otherwise. Required, so no connector gets `None` by omission.
    /// [`conformance::check_name_resolution`] tests this.
    fn canonical_name(&self) -> Option<String>;
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

    /// Returns optional per-table statistics (row count, column NDV, min/max, etc.).
    ///
    /// Connectors that cannot provide statistics return `None`. The cost-based
    /// planner falls back to conservative defaults in that case.
    fn statistics(&self) -> Option<TableStatistics> {
        None
    }

    /// The `(schema, table)` this provider reads, as the source stores
    /// them (after any case-insensitive matching or redirection). Access
    /// rules are checked on this identity as well as on the name as
    /// written.
    ///
    /// **A connector whose [`SchemaProvider::table`] accepts any name
    /// other than the exact stored one MUST return `Some`** (see
    /// [`SchemaProvider::canonical_name`]). `None` claims the table was
    /// found by its exact stored name; the catalog manager holds it to
    /// that by finding the name in [`SchemaProvider::table_names`], and
    /// treats the table as missing otherwise. Required, so no connector
    /// gets `None` by omission.
    fn canonical_name(&self) -> Option<(String, String)>;
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

    /// Found by its exact registered name only.
    fn canonical_name(&self) -> Option<(String, String)> {
        None
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
    /// Found by its exact registered name only.
    fn canonical_name(&self) -> Option<String> {
        None
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
    /// Shared so that session views created by
    /// [`CatalogManager::with_session_defaults`] observe catalogs registered
    /// (or deregistered) on the root manager after the view was created.
    catalogs: Arc<RwLock<HashMap<String, Arc<dyn CatalogProvider>>>>,
    default_catalog: String,
    default_schema: String,
    /// When `true`, [`CatalogManager::qualify_table_reference`] fills in the
    /// missing catalog/schema parts from this manager's defaults. Only set on
    /// session views: the defaults of a session view differ from the ones the
    /// rest of the system (connector lookup, workers) assumes, so a table
    /// reference planned against a view must carry its resolved catalog and
    /// schema explicitly.
    qualify_references: bool,
    /// The session user and the rules they are checked against, on a view
    /// made by [`CatalogManager::with_access`]. `None` checks nothing.
    access: Option<Arc<SessionAccess>>,
}

/// A session user and the access rules that apply to them.
#[derive(Debug)]
struct SessionAccess {
    user: String,
    rules: Arc<AccessRules>,
}

impl CatalogManager {
    /// Creates a new `CatalogManager` with the given default catalog and schema names.
    pub fn new(default_catalog: impl Into<String>, default_schema: impl Into<String>) -> Self {
        Self {
            catalogs: Arc::new(RwLock::new(HashMap::new())),
            default_catalog: default_catalog.into(),
            default_schema: default_schema.into(),
            qualify_references: false,
            access: None,
        }
    }

    /// Returns a session view of this manager that shares every registered
    /// catalog but resolves unqualified table references against
    /// `default_catalog` / `default_schema` instead of the server defaults.
    ///
    /// Table references planned against a view are fully qualified by
    /// [`CatalogManager::qualify_table_reference`], so downstream consumers
    /// that only know the server defaults (connector lookup, distributed
    /// workers) still resolve the table the session meant. Used by client
    /// protocols with per-session catalog/schema (e.g. the Trino
    /// `X-Trino-Catalog` / `X-Trino-Schema` headers).
    pub fn with_session_defaults(
        &self,
        default_catalog: impl Into<String>,
        default_schema: impl Into<String>,
    ) -> Self {
        Self {
            catalogs: Arc::clone(&self.catalogs),
            default_catalog: default_catalog.into(),
            default_schema: default_schema.into(),
            qualify_references: true,
            access: self.access.clone(),
        }
    }

    /// Returns a view of this manager, keeping its defaults, that checks
    /// every table read and filters every listing against `rules` for
    /// `user` (file-based-access-control).
    pub fn with_access(&self, user: impl Into<String>, rules: Arc<AccessRules>) -> Self {
        Self {
            catalogs: Arc::clone(&self.catalogs),
            default_catalog: self.default_catalog.clone(),
            default_schema: self.default_schema.clone(),
            qualify_references: self.qualify_references,
            access: Some(Arc::new(SessionAccess {
                user: user.into(),
                rules,
            })),
        }
    }

    /// Fails with [`CatalogError::AccessDenied`] when the session user may
    /// not run queries (`queries` rules); always passes without access rules.
    pub fn check_execute(&self) -> Result<(), CatalogError> {
        if self
            .access
            .as_ref()
            .is_none_or(|a| a.rules.can_execute(&a.user))
        {
            Ok(())
        } else {
            Err(CatalogError::AccessDenied("Cannot execute query".into()))
        }
    }

    /// Whether the session user may see a query run by `owner`; always true
    /// without access rules.
    pub fn can_view_query(&self, owner: &str) -> bool {
        self.access
            .as_ref()
            .is_none_or(|a| a.rules.can_view_query(&a.user, owner))
    }

    /// Whether the session user may see `catalog` (true without rules,
    /// whether or not it is registered).
    pub fn catalog_visible(&self, catalog: &str) -> bool {
        self.access
            .as_ref()
            .is_none_or(|a| a.rules.catalog_visible(&a.user, catalog))
    }

    /// Whether the session user may see `catalog.schema` (true without
    /// rules, whether or not it exists).
    pub fn schema_visible(&self, catalog: &str, schema: &str) -> bool {
        self.access
            .as_ref()
            .is_none_or(|a| a.rules.schema_visible(&a.user, catalog, schema))
    }

    /// Whether the session user may see `catalog.schema.table` (true
    /// without rules, whether or not it exists).
    pub fn table_visible(&self, catalog: &str, schema: &str, table: &str) -> bool {
        self.access
            .as_ref()
            .is_none_or(|a| a.rules.table_visible(&a.user, catalog, schema, table))
    }

    /// Whether the session user may read `catalog.schema.table` (true
    /// without rules, whether or not it exists).
    fn can_select(&self, catalog: &str, schema: &str, table: &str) -> bool {
        self.access
            .as_ref()
            .is_none_or(|a| a.rules.can_select(&a.user, catalog, schema, table))
    }

    /// Catalog names the session user may see (all of them without rules).
    pub fn visible_catalog_names(&self) -> Vec<String> {
        let mut names = self.catalog_names();
        if let Some(a) = &self.access {
            names.retain(|c| a.rules.catalog_visible(&a.user, c));
        }
        names
    }

    /// Schema names of `catalog` the session user may see; empty for an
    /// unknown or hidden catalog, or (logged) when the source cannot be
    /// reached. See [`try_visible_schema_names`](Self::try_visible_schema_names).
    pub async fn visible_schema_names(&self, catalog: &str) -> Vec<String> {
        self.try_visible_schema_names(catalog)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("listing schemas of {catalog}: {e}");
                Vec::new()
            })
    }

    /// Like [`visible_schema_names`](Self::visible_schema_names), but `Err`
    /// when the source could not be asked, so an outage is not mistaken
    /// for an empty catalog.
    pub async fn try_visible_schema_names(
        &self,
        catalog: &str,
    ) -> Result<Vec<String>, CatalogError> {
        let Some(provider) = self.catalog(catalog) else {
            return Ok(Vec::new());
        };
        let mut names = provider.try_schema_names().await?;
        if let Some(a) = &self.access {
            names.retain(|s| a.rules.schema_visible(&a.user, catalog, s));
        }
        Ok(names)
    }

    /// Table names of `catalog.schema` the session user may see; empty for
    /// an unknown or hidden schema, or (logged) when the source cannot be
    /// reached. See [`try_visible_table_names`](Self::try_visible_table_names).
    pub async fn visible_table_names(&self, catalog: &str, schema: &str) -> Vec<String> {
        self.try_visible_table_names(catalog, schema)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("listing tables of {catalog}.{schema}: {e}");
                Vec::new()
            })
    }

    /// Like [`visible_table_names`](Self::visible_table_names), but `Err`
    /// when the source could not be asked, either for the schema or for
    /// its tables, so an outage is not mistaken for an empty schema. A
    /// schema the connector resolves to a differently-written stored name
    /// is checked under both names.
    pub async fn try_visible_table_names(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<Vec<String>, CatalogError> {
        if !self.schema_visible(catalog, schema) {
            return Ok(Vec::new());
        }
        let Some((schema_provider, stored)) = self.lookup_schema(catalog, schema).await? else {
            return Ok(Vec::new());
        };
        let mut names = schema_provider.try_table_names().await?;
        if let Some(a) = &self.access {
            names.retain(|t| {
                a.rules.table_visible(&a.user, catalog, schema, t)
                    && a.rules.table_visible(&a.user, catalog, &stored, t)
            });
        }
        Ok(names)
    }

    /// Whether `catalog.schema` exists and the session user may see it,
    /// under the name as written and the name the connector resolved it
    /// to. Without rules, whether it exists. `Err` when the source could
    /// not be asked (see [`CatalogProvider::try_schema`]).
    pub async fn visible_schema_exists(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<bool, CatalogError> {
        if !self.schema_visible(catalog, schema) {
            return Ok(false);
        }
        Ok(self.lookup_schema(catalog, schema).await?.is_some())
    }

    /// The provider for `catalog.schema` and the schema's stored name.
    /// `None` when the catalog or schema does not exist, or when the
    /// connector resolved `schema` to a stored name the session user may
    /// not see, so a hidden schema reads as a missing one whatever case
    /// it is written in. The written name is not checked here. `Err` only
    /// when the source could not be asked.
    async fn lookup_schema(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<Option<(Arc<dyn SchemaProvider>, String)>, CatalogError> {
        let Some(provider) = self.catalog(catalog) else {
            return Ok(None);
        };
        let Some(found) = provider.try_schema(schema).await? else {
            return Ok(None);
        };
        let Some(a) = &self.access else {
            let stored = found.canonical_name().unwrap_or_else(|| schema.to_string());
            return Ok(Some((found, stored)));
        };
        let stored = match found.canonical_name() {
            Some(stored) => stored,
            // The provider claims it answers only to its stored name: hold
            // it to that, so a loose connector that reports no stored name
            // fails closed.
            None if provider
                .try_schema_names()
                .await?
                .iter()
                .any(|s| s == schema) =>
            {
                schema.to_string()
            }
            None => return Ok(None),
        };
        Ok(a.rules
            .schema_visible(&a.user, catalog, &stored)
            .then_some((found, stored)))
    }

    /// Returns the reference the planner should record for a scanned table.
    ///
    /// On the root manager this is `reference` unchanged (planning output is
    /// byte-identical to before session views existed). On a session view
    /// (see [`CatalogManager::with_session_defaults`]) the missing catalog
    /// and schema parts are filled in from the view's defaults.
    pub fn qualify_table_reference(&self, reference: &TableReference) -> TableReference {
        if !self.qualify_references {
            return reference.clone();
        }
        TableReference {
            catalog: Some(
                reference
                    .catalog
                    .clone()
                    .unwrap_or_else(|| self.default_catalog.clone()),
            ),
            schema: Some(
                reference
                    .schema
                    .clone()
                    .unwrap_or_else(|| self.default_schema.clone()),
            ),
            table: reference.table.clone(),
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

    /// `reference` with each part that names nothing as written, but names
    /// something in lowercase, replaced by the lowercase form. PostgreSQL
    /// and Trino fold unquoted identifiers to lowercase, and the parser
    /// keeps no quoting, so `FROM NATION` and `INFORMATION_SCHEMA.TABLES`
    /// find `nation` and `information_schema.tables`. A part that names
    /// something as written is kept, so mixed-case names still resolve.
    /// A lowercase form the session user may not see is never probed, so
    /// the folded name does not reveal whether a hidden object exists.
    ///
    /// A part is folded only when the source answers that nothing goes by
    /// it as written, so neither access rules nor an outage choose which
    /// object is read: `Err` when the source could not be asked. Both
    /// spellings are looked up before the fold is decided, so how many
    /// lookups a reference costs never depends on whether a name the user
    /// may not read exists.
    pub async fn fold_reference_case(
        &self,
        reference: &TableReference,
    ) -> Result<TableReference, CatalogError> {
        // All-lowercase references (the common case) need no lookups.
        let folds = |name: &str| name.to_lowercase() != name;
        if !reference.catalog.as_deref().is_some_and(folds)
            && !reference.schema.as_deref().is_some_and(folds)
            && !folds(&reference.table)
        {
            return Ok(reference.clone());
        }
        let mut folded = reference.clone();
        let catalog_name = match &reference.catalog {
            Some(name) => {
                let catalogs = self.catalogs.read().unwrap();
                let lower = name.to_lowercase();
                if !catalogs.contains_key(name)
                    && self.catalog_visible(&lower)
                    && catalogs.contains_key(&lower)
                {
                    folded.catalog = Some(lower);
                }
                folded.catalog.clone().unwrap_or_default()
            }
            None => self.default_catalog.clone(),
        };
        let Some(catalog) = self.catalog(&catalog_name) else {
            return Ok(folded);
        };
        // Both spellings are always asked about, and only then is the fold
        // decided, so the number of source lookups never depends on whether
        // an object exists (one the user may not see included). `try_schema`,
        // not `schema`: a transient failure looking up `"Shop"` must not
        // fold the reference onto `shop`.
        let schema = match &reference.schema {
            Some(name)
                if name.to_lowercase() != *name
                    && self.schema_visible(&catalog_name, &name.to_lowercase()) =>
            {
                let lower = name.to_lowercase();
                let as_written = catalog.try_schema(name).await?;
                let lowered = catalog.try_schema(&lower).await?;
                match (as_written, lowered) {
                    (None, Some(lowered)) => {
                        folded.schema = Some(lower);
                        Some(lowered)
                    }
                    (as_written, _) => as_written,
                }
            }
            Some(name) => catalog.try_schema(name).await?,
            None => catalog.try_schema(&self.default_schema).await?,
        };
        let schema_name = folded
            .schema
            .clone()
            .unwrap_or_else(|| self.default_schema.clone());
        let Some(schema) = schema else {
            return Ok(folded);
        };
        // A schema hidden under its stored name reads as missing in
        // `resolve_table`, so probing its tables would only reveal, by the
        // lookups it costs, that it exists.
        let stored_schema = schema
            .canonical_name()
            .unwrap_or_else(|| schema_name.clone());
        if !self.schema_visible(&catalog_name, &stored_schema) {
            return Ok(folded);
        }
        // Access rules must never choose which table is read: fold only
        // when the source answers that no table goes by the name as
        // written (`Missing`); one that exists, readable or not, stops the
        // fold, and `resolve_table` then reads or refuses it as written.
        // Both names are always probed before deciding, so a denied name
        // costs the source the same lookups whether or not it exists.
        // Neither probe loads a table: the gate refuses every stored name,
        // so a connector stops at its name lookup (`Denied` meaning "it
        // exists"), and the second probe only records whether the user may
        // read the lowercase table. So a denied table is never loaded here,
        // and an allowed one is loaded once, by `resolve_table`.
        let lower = reference.table.to_lowercase();
        if lower != reference.table && self.table_visible(&catalog_name, &schema_name, &lower) {
            let as_written = schema
                .table_if_allowed(&reference.table, &|_, _| false)
                .await?;
            let readable = std::sync::atomic::AtomicBool::new(false);
            let record = |s: &str, t: &str| {
                readable.store(
                    self.can_select(&catalog_name, s, t),
                    std::sync::atomic::Ordering::Relaxed,
                );
                false
            };
            let lowered = schema.table_if_allowed(&lower, &record).await?;
            let fold = matches!(as_written, TableLookup::Missing)
                && match lowered {
                    // A connector that reports no stored name (memory,
                    // file) finds by the exact name only, and its lookup is
                    // cheap.
                    TableLookup::Found(_) => true,
                    TableLookup::Denied => readable.load(std::sync::atomic::Ordering::Relaxed),
                    TableLookup::Missing => false,
                };
            if fold {
                folded.table = lower;
            }
        }
        Ok(folded)
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
        // Checked before the lookup, so a denied name reads the same
        // whether or not the table exists.
        if let Some(a) = &self.access {
            if !a
                .rules
                .can_select(&a.user, catalog_name, schema_name, table_name)
            {
                return Err(CatalogError::AccessDenied(format!(
                    "Cannot select from table {catalog_name}.{schema_name}.{table_name}"
                )));
            }
        }

        if self.catalog(catalog_name).is_none() {
            return Err(CatalogError::CatalogNotFound(catalog_name.to_string()));
        }
        let (schema, stored_schema) = self
            .lookup_schema(catalog_name, schema_name)
            .await?
            .ok_or_else(|| CatalogError::SchemaNotFound(format!("{catalog_name}.{schema_name}")))?;
        let not_found =
            || CatalogError::SchemaNotFound(format!("{catalog_name}.{schema_name}.{table_name}"));
        let Some(a) = &self.access else {
            return schema
                .table_if_allowed(table_name, &|_, _| true)
                .await?
                .found()
                .ok_or_else(not_found);
        };

        // The connector may have resolved the name to a stored one that
        // differs from it (case-insensitive matching, redirection). The
        // rules apply to what is actually read, so check that identity
        // too, before the connector loads the table where it can and once
        // more here. A denial reads as a missing table: the name as
        // written was allowed, and the stored one must not be revealed.
        let allowed = |s: &str, t: &str| a.rules.can_select(&a.user, catalog_name, s, t);
        let table = schema
            .table_if_allowed(table_name, &allowed)
            .await?
            .found()
            .ok_or_else(not_found)?;
        let (stored_schema, stored_table) = match table.canonical_name() {
            Some(identity) => identity,
            // Found by its exact name, the provider claims: hold it to that.
            None if schema
                .try_table_names()
                .await?
                .iter()
                .any(|t| t == table_name) =>
            {
                (stored_schema, table_name.clone())
            }
            None => return Err(not_found()),
        };
        if !allowed(&stored_schema, &stored_table) {
            return Err(not_found());
        }
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::DataType;
    use async_trait::async_trait;

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

    // -- Session view tests --

    #[tokio::test]
    async fn session_view_resolves_against_its_own_defaults() {
        let root = CatalogManager::new("memory", "default");
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("t", Arc::new(MemoryTable::new(test_columns())));
        let catalog = Arc::new(MemoryCatalog::new());
        catalog.register_schema("sales", schema);
        root.register_catalog("lake", catalog);

        let unqualified = TableReference {
            catalog: None,
            schema: None,
            table: "t".to_string(),
        };
        assert!(root.resolve_table(&unqualified).await.is_err());

        let view = root.with_session_defaults("lake", "sales");
        assert!(view.resolve_table(&unqualified).await.is_ok());
        let qualified = view.qualify_table_reference(&unqualified);
        assert_eq!(qualified.catalog.as_deref(), Some("lake"));
        assert_eq!(qualified.schema.as_deref(), Some("sales"));
        // Root manager never rewrites references.
        assert_eq!(root.qualify_table_reference(&unqualified), unqualified);

        // Catalogs registered on the root after the view exists are shared.
        root.register_catalog("late", Arc::new(MemoryCatalog::new()));
        assert!(view.catalog("late").is_some());
    }

    /// file-based-access-control: a view checks reads before the lookup,
    /// keeps the session defaults it wraps, and filters every listing.
    #[tokio::test]
    async fn access_view_checks_reads_and_filters_listings() {
        let root = CatalogManager::new("lake", "sales");
        for (catalog_name, schemas) in [("lake", ["sales", "hr"]), ("pg", ["public", "ops"])] {
            let catalog = Arc::new(MemoryCatalog::new());
            for schema_name in schemas {
                let schema = Arc::new(MemorySchema::new());
                for table in ["orders", "salaries"] {
                    schema.register_table(table, Arc::new(MemoryTable::new(test_columns())));
                }
                catalog.register_schema(schema_name, schema);
            }
            root.register_catalog(catalog_name, catalog);
        }
        let rules = Arc::new(
            AccessRules::from_json(
                r#"{
                    "catalogs": [{"catalog": "pg", "allow": "none"}, {"allow": "read-only"}],
                    "schemas": [{"owner": false}],
                    "tables": [{"user": "alice", "schema": "sales", "table": "orders",
                                "privileges": ["SELECT"]}]
                }"#,
            )
            .unwrap(),
        );
        let alice = root.with_access("alice", Arc::clone(&rules));
        assert!(
            read(&alice, "orders").await.is_ok(),
            "unqualified names use the defaults"
        );
        let err = read(&alice, "salaries").await.unwrap_err().to_string();
        assert_eq!(
            err,
            "Access Denied: Cannot select from table lake.sales.salaries"
        );
        let missing = read(&alice, "lake.sales.nope")
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            missing,
            "Access Denied: Cannot select from table lake.sales.nope"
        );
        assert!(read(&alice, "pg.public.orders").await.is_err());
        assert!(
            read(&root, "pg.public.salaries").await.is_ok(),
            "the root checks nothing"
        );

        assert_eq!(alice.visible_catalog_names(), vec!["lake"]);
        assert_eq!(alice.visible_schema_names("lake").await, vec!["sales"]);
        assert!(alice.visible_schema_names("pg").await.is_empty());
        assert_eq!(
            alice.visible_table_names("lake", "sales").await,
            vec!["orders"]
        );
        assert!(alice.visible_table_names("lake", "hr").await.is_empty());

        // A session-defaults view of an access view keeps the user.
        let session = alice.with_session_defaults("lake", "hr");
        assert!(read(&session, "orders").await.is_err());
        assert!(read(&session, "sales.orders").await.is_ok());
    }

    async fn read(
        manager: &CatalogManager,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>, CatalogError> {
        let mut parts: Vec<String> = name.split('.').map(String::from).collect();
        let table = parts.pop().unwrap();
        let schema = parts.pop();
        let catalog = parts.pop();
        manager
            .resolve_table(&TableReference {
                catalog,
                schema,
                table,
            })
            .await
    }

    // -- Access on the resolved name --

    /// PostgreSQL-style lookup: the exact name, else a unique
    /// ASCII-case-insensitive match.
    fn resolve_loosely(names: &[&str], wanted: &str) -> Option<String> {
        if names.contains(&wanted) {
            return Some(wanted.to_string());
        }
        let mut found = names.iter().filter(|n| n.eq_ignore_ascii_case(wanted));
        let first = found.next()?;
        found.next().is_none().then(|| first.to_string())
    }

    #[derive(Debug)]
    struct LooseTable {
        schema: String,
        name: String,
    }

    impl TableProvider for LooseTable {
        fn schema(&self) -> Vec<ColumnInfo> {
            test_columns()
        }
        fn canonical_name(&self) -> Option<(String, String)> {
            Some((self.schema.clone(), self.name.clone()))
        }
    }

    #[derive(Debug)]
    struct LooseSchema {
        name: String,
        tables: &'static [&'static str],
    }

    #[async_trait]
    impl SchemaProvider for LooseSchema {
        async fn table_names(&self) -> Vec<String> {
            self.tables.iter().map(|t| t.to_string()).collect()
        }
        async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
            let name = resolve_loosely(self.tables, name)?;
            Some(Arc::new(LooseTable {
                schema: self.name.clone(),
                name,
            }))
        }
        fn canonical_name(&self) -> Option<String> {
            Some(self.name.clone())
        }
    }

    /// A connector resolving names as PostgreSQL's does: `public` holds
    /// `secret`, `open` and the mixed-case `Orders`; `hr` holds `salaries`;
    /// `shop` holds both `"Orders"` and `orders`.
    #[derive(Debug)]
    struct LooseCatalog;

    #[async_trait]
    impl CatalogProvider for LooseCatalog {
        async fn schema_names(&self) -> Vec<String> {
            vec!["hr".into(), "public".into(), "shop".into()]
        }
        async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            let name = resolve_loosely(&["hr", "public", "shop"], name)?;
            let tables: &'static [&'static str] = match name.as_str() {
                "hr" => &["salaries"],
                "shop" => &["Orders", "orders"],
                _ => &["Orders", "open", "secret"],
            };
            Some(Arc::new(LooseSchema { name, tables }))
        }
    }

    fn loose_view(rules: &str) -> CatalogManager {
        let root = CatalogManager::new("pg", "public");
        root.register_catalog("pg", Arc::new(LooseCatalog));
        root.with_access("alice", Arc::new(AccessRules::from_json(rules).unwrap()))
    }

    /// The planner's path: fold the reference's case, then resolve it.
    async fn fold_read(
        manager: &CatalogManager,
        schema: &str,
        table: &str,
    ) -> Result<Arc<dyn TableProvider>, CatalogError> {
        let reference = TableReference {
            catalog: Some("pg".into()),
            schema: Some(schema.into()),
            table: table.into(),
        };
        let folded = manager.fold_reference_case(&reference).await?;
        manager.resolve_table(&folded).await
    }

    /// The error a table that does not exist gets under the same name.
    async fn missing_table_error(manager: &CatalogManager, schema: &str, table: &str) -> String {
        let nope = "nope_does_not_exist";
        let err = fold_read(manager, schema, nope).await.unwrap_err();
        assert!(matches!(err, CatalogError::SchemaNotFound(_)), "{err}");
        err.to_string().replace(nope, table)
    }

    /// A table rule on the stored name holds whatever case the name is
    /// written in, and the denial reads exactly as a missing table.
    #[tokio::test]
    async fn table_rule_applies_to_the_name_the_connector_resolved() {
        let alice = loose_view(
            r#"{"tables": [
                {"user": "alice", "schema": "public", "table": "secret", "privileges": []},
                {"privileges": ["SELECT"]}
            ]}"#,
        );
        assert!(matches!(
            fold_read(&alice, "public", "secret").await.unwrap_err(),
            CatalogError::AccessDenied(_)
        ));
        for (schema, table) in [
            ("public", "SECRET"),
            ("public", "Secret"),
            ("PUBLIC", "secret"),
            ("PUBLIC", "SECRET"),
        ] {
            let err = fold_read(&alice, schema, table).await.unwrap_err();
            assert_eq!(
                err.to_string(),
                missing_table_error(&alice, schema, table).await,
                "{schema}.{table} must read as a missing table"
            );
        }
        // Allowed tables still resolve in any case.
        for (schema, table) in [("public", "open"), ("PUBLIC", "OPEN"), ("Public", "Open")] {
            assert!(
                fold_read(&alice, schema, table).await.is_ok(),
                "{schema}.{table}"
            );
        }
        assert!(!alice
            .visible_table_names("pg", "PUBLIC")
            .await
            .contains(&"secret".to_string()));
    }

    /// A schema rule on the stored name holds whatever case the schema is
    /// written in, for reads and for listings.
    #[tokio::test]
    async fn schema_rule_applies_to_the_schema_the_connector_resolved() {
        let alice = loose_view(
            r#"{"tables": [
                {"user": "alice", "schema": "hr", "privileges": []},
                {"privileges": ["SELECT"]}
            ]}"#,
        );
        assert!(fold_read(&alice, "hr", "salaries").await.is_err());
        for (schema, table) in [("HR", "salaries"), ("Hr", "SALARIES")] {
            let err = fold_read(&alice, schema, table).await.unwrap_err();
            assert_eq!(
                err.to_string(),
                missing_table_error(&alice, schema, table).await,
                "{schema}.{table} must read as a missing table"
            );
        }
        assert!(alice.visible_table_names("pg", "HR").await.is_empty());

        // A schema hidden under its stored name reads as a missing schema
        // when written in another case the rules allow (here any
        // uppercase name), and lists nothing.
        let alice = loose_view(
            r#"{
                "schemas": [{"owner": false}],
                "tables": [{"user": "alice", "schema": "public|[A-Z]+", "privileges": ["SELECT"]}]
            }"#,
        );
        assert!(!alice.schema_visible("pg", "hr"));
        let err = fold_read(&alice, "HR", "salaries").await.unwrap_err();
        let missing = fold_read(&alice, "NOPE", "salaries").await.unwrap_err();
        assert!(
            matches!(missing, CatalogError::SchemaNotFound(_)),
            "{missing}"
        );
        assert_eq!(err.to_string(), missing.to_string().replace("NOPE", "HR"));
        assert!(alice.visible_table_names("pg", "HR").await.is_empty());
        assert!(!alice.visible_schema_exists("pg", "HR").await.unwrap());
        assert!(alice.visible_schema_exists("pg", "PUBLIC").await.unwrap());
    }

    /// A mixed-case stored name (`"Orders"`) still reads through a folded
    /// or differently-cased reference when the rules allow it, and both
    /// the written and the stored name must be allowed.
    #[tokio::test]
    async fn mixed_case_stored_names_need_both_names_allowed() {
        let alice = loose_view(
            r#"{"tables": [
                {"user": "alice", "table": "secret", "privileges": []},
                {"privileges": ["SELECT"]}
            ]}"#,
        );
        for table in ["Orders", "orders", "ORDERS"] {
            assert!(fold_read(&alice, "public", table).await.is_ok(), "{table}");
        }

        let alice = loose_view(
            r#"{"tables": [
                {"user": "alice", "table": "Orders", "privileges": []},
                {"privileges": ["SELECT"]}
            ]}"#,
        );
        assert!(fold_read(&alice, "public", "Orders").await.is_err());
        let err = fold_read(&alice, "public", "orders").await.unwrap_err();
        assert_eq!(
            err.to_string(),
            missing_table_error(&alice, "public", "orders").await
        );

        // Denying the written form denies it even though the stored one is
        // allowed.
        let alice = loose_view(
            r#"{"tables": [
                {"user": "alice", "table": "orders", "privileges": []},
                {"privileges": ["SELECT"]}
            ]}"#,
        );
        assert!(matches!(
            fold_read(&alice, "public", "orders").await.unwrap_err(),
            CatalogError::AccessDenied(_)
        ));
        assert!(fold_read(&alice, "public", "Orders").await.is_ok());
    }

    /// Access rules never choose which table a name reads. `shop` holds
    /// both `"Orders"` and `orders`: a rule denying alice `"Orders"`
    /// refuses `shop.Orders` instead of folding it onto `orders`, the
    /// table an unrestricted user reading the same name never gets.
    #[tokio::test]
    async fn a_denied_name_is_never_folded_onto_another_table() {
        let root = CatalogManager::new("pg", "public");
        root.register_catalog("pg", Arc::new(LooseCatalog));
        let stored = |t: Arc<dyn TableProvider>| t.canonical_name().unwrap();
        assert_eq!(
            stored(fold_read(&root, "shop", "Orders").await.unwrap()),
            ("shop".to_string(), "Orders".to_string())
        );

        // Denied as written: refused (Access Denied), never folded.
        let alice = loose_view(
            r#"{"tables": [
                {"user": "alice", "table": "Orders", "privileges": []},
                {"privileges": ["SELECT"]}
            ]}"#,
        );
        let err = fold_read(&alice, "shop", "Orders").await.unwrap_err();
        assert!(matches!(err, CatalogError::AccessDenied(_)), "{err}");

        // Denied only under its stored name (the schema is written in a
        // case the rule does not name): refused as a missing table, never
        // read as `orders`.
        let alice = loose_view(
            r#"{"tables": [
                {"user": "alice", "schema": "shop", "table": "Orders", "privileges": []},
                {"privileges": ["SELECT"]}
            ]}"#,
        );
        let err = fold_read(&alice, "SHOP", "Orders").await.unwrap_err();
        assert_eq!(
            err.to_string(),
            missing_table_error(&alice, "SHOP", "Orders").await
        );
        assert_eq!(
            stored(fold_read(&alice, "SHOP", "orders").await.unwrap()),
            ("shop".to_string(), "orders".to_string())
        );
    }

    /// An outage looking up the schema as written is reported, never
    /// taken for "missing": a transient failure on `"Shop"` must not fold
    /// the reference onto a different schema `shop`.
    #[tokio::test]
    async fn an_outage_never_folds_onto_another_schema() {
        use std::sync::atomic::{AtomicBool, Ordering};
        /// Schemas `Shop` and `shop`; the first lookup of `Shop` fails.
        #[derive(Debug, Default)]
        struct Blip(AtomicBool);
        #[async_trait]
        impl CatalogProvider for Blip {
            async fn schema_names(&self) -> Vec<String> {
                vec!["Shop".into(), "shop".into()]
            }
            async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
                self.try_schema(name).await.ok().flatten()
            }
            async fn try_schema(
                &self,
                name: &str,
            ) -> Result<Option<Arc<dyn SchemaProvider>>, CatalogError> {
                if name == "Shop" && !self.0.swap(true, Ordering::SeqCst) {
                    return Err(CatalogError::Unavailable("connection reset".into()));
                }
                Ok(["Shop", "shop"].contains(&name).then(|| {
                    Arc::new(LooseSchema {
                        name: name.to_string(),
                        tables: &["orders"],
                    }) as Arc<dyn SchemaProvider>
                }))
            }
        }
        let root = CatalogManager::new("pg", "public");
        root.register_catalog("pg", Arc::new(Blip::default()));
        let reference = TableReference {
            catalog: Some("pg".into()),
            schema: Some("Shop".into()),
            table: "orders".into(),
        };
        assert!(matches!(
            root.fold_reference_case(&reference).await,
            Err(CatalogError::Unavailable(_))
        ));
        // Once the source answers, `Shop` is kept as written.
        assert_eq!(
            root.fold_reference_case(&reference)
                .await
                .unwrap()
                .to_string(),
            "pg.Shop.orders"
        );
    }

    /// A PostgreSQL-like connector over a fixed layout that counts every
    /// question it is asked (lookups and listings), to show how many round
    /// trips a statement costs.
    #[derive(Debug)]
    struct Counted {
        layout: &'static [(&'static str, &'static [&'static str])],
        asked: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Counted {
        fn ask(&self) {
            self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct CountedSchema {
        inner: LooseSchema,
        asked: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl SchemaProvider for CountedSchema {
        async fn table_names(&self) -> Vec<String> {
            self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.table_names().await
        }
        async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
            self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.table(name).await
        }
        fn canonical_name(&self) -> Option<String> {
            self.inner.canonical_name()
        }
    }

    #[async_trait]
    impl CatalogProvider for Counted {
        async fn schema_names(&self) -> Vec<String> {
            self.ask();
            self.layout.iter().map(|(s, _)| s.to_string()).collect()
        }
        async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            self.ask();
            let names: Vec<&str> = self.layout.iter().map(|(s, _)| *s).collect();
            let name = resolve_loosely(&names, name)?;
            let tables = self.layout.iter().find(|(s, _)| *s == name)?.1;
            Some(Arc::new(CountedSchema {
                inner: LooseSchema { name, tables },
                asked: self.asked.clone(),
            }))
        }
    }

    /// Source questions and the outcome of reading `schema.table` as alice
    /// under `rules`, from `layout`.
    async fn counted_read(
        layout: &'static [(&'static str, &'static [&'static str])],
        rules: &str,
        schema: &str,
        table: &str,
    ) -> (usize, String) {
        let asked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let root = CatalogManager::new("pg", "public");
        root.register_catalog(
            "pg",
            Arc::new(Counted {
                layout,
                asked: asked.clone(),
            }),
        );
        let alice = root.with_access("alice", Arc::new(AccessRules::from_json(rules).unwrap()));
        let err = fold_read(&alice, schema, table).await.unwrap_err();
        (
            asked.load(std::sync::atomic::Ordering::SeqCst),
            err.to_string(),
        )
    }

    /// A denied name costs the source the same number of questions whether
    /// or not the table exists, so latency does not tell alice which. The
    /// case fold asks about both spellings before deciding, instead of
    /// stopping as soon as the name as written turns out to exist.
    #[tokio::test]
    async fn a_denied_name_costs_the_same_whether_or_not_it_exists() {
        // Denied as written, every other name readable.
        let rules = r#"{"tables": [
            {"user": "alice", "table": "Payroll", "privileges": []},
            {"privileges": ["SELECT"]}
        ]}"#;
        let present = counted_read(&[("public", &["Payroll", "t"])], rules, "public", "Payroll");
        let absent = counted_read(&[("public", &["t"])], rules, "public", "Payroll");
        let (present, absent) = (present.await, absent.await);
        assert!(present.1.contains("Access Denied"), "{}", present.1);
        assert_eq!(present, absent);

        // Denied only under its stored name (`shop`.`Orders`, written
        // `SHOP.Orders`): refused as missing either way, at the same cost.
        let rules = r#"{"tables": [
            {"user": "alice", "schema": "shop", "table": "Orders", "privileges": []},
            {"privileges": ["SELECT"]}
        ]}"#;
        let present = counted_read(&[("shop", &["Orders", "t"])], rules, "SHOP", "Orders");
        let absent = counted_read(&[("shop", &["t"])], rules, "SHOP", "Orders");
        let (present, absent) = (present.await, absent.await);
        assert!(present.1.contains("schema not found"), "{}", present.1);
        assert_eq!(present, absent);

        // A schema the user may not see costs the same too.
        let rules = r#"{"tables": [
            {"user": "alice", "schema": "Hidden", "privileges": []},
            {"privileges": ["SELECT"]}
        ]}"#;
        let present = counted_read(
            &[("HIDDEN", &["t"]), ("Hidden", &["t"])],
            rules,
            "Hidden",
            "t",
        );
        let absent = counted_read(&[("other", &["t"])], rules, "Hidden", "t");
        let (present, absent) = (present.await, absent.await);
        assert_eq!(present, absent);

        // So does a mixed-case table in a schema hidden under its stored
        // name (`hr`, written `HR`): its tables are not probed.
        let rules = r#"{
            "schemas": [{"owner": false}],
            "tables": [{"user": "alice", "schema": "public|[A-Z]+", "privileges": ["SELECT"]}]
        }"#;
        let present = counted_read(&[("hr", &["salaries"])], rules, "HR", "Salaries");
        let absent = counted_read(&[("other", &["t"])], rules, "HR", "Salaries");
        let (present, absent) = (present.await, absent.await);
        assert!(present.1.contains("schema not found"), "{}", present.1);
        assert_eq!(present, absent);
    }

    /// A connector normalizing names as HMS does: trimmed, lowercased,
    /// and a `@catalog#` prefix dropped. Databases `hr` and `sales`.
    #[derive(Debug)]
    struct HmsLikeCatalog;

    #[async_trait]
    impl CatalogProvider for HmsLikeCatalog {
        async fn schema_names(&self) -> Vec<String> {
            vec!["hr".into(), "sales".into()]
        }
        async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            let name = name.trim().to_lowercase();
            let name = name.rsplit_once('#').map_or(name.as_str(), |(_, db)| db);
            let tables: &'static [&'static str] = match name {
                "hr" => &["salaries"],
                "sales" => &["orders"],
                _ => return None,
            };
            Some(Arc::new(LooseSchema {
                name: name.to_string(),
                tables,
            }))
        }
    }

    /// Names HMS normalizes beyond case (`"hr "`, `@hive#hr`) are checked
    /// under the stored name too: a denied schema lists nothing, reads as
    /// missing, and cannot be a search-path target.
    #[tokio::test]
    async fn hms_normalized_schema_names_are_checked_under_the_stored_name() {
        let root = CatalogManager::new("datalake", "sales");
        root.register_catalog("datalake", Arc::new(HmsLikeCatalog));
        let rules = r#"{
            "schemas": [{"owner": false}],
            "tables": [
                {"user": "alice", "schema": "hr", "privileges": []},
                {"user": "alice", "schema": "hr .*| hr|.*#hr|HR|sales", "privileges": ["SELECT"]}
            ]
        }"#;
        let alice = root.with_access("alice", Arc::new(AccessRules::from_json(rules).unwrap()));
        assert!(!alice.schema_visible("datalake", "hr"));
        for written in ["hr ", "@hive#hr", "HR", " hr"] {
            assert!(alice.schema_visible("datalake", written), "{written:?}");
            assert!(
                alice
                    .visible_table_names("datalake", written)
                    .await
                    .is_empty(),
                "{written:?} lists the denied schema"
            );
            assert!(!alice
                .visible_schema_exists("datalake", written)
                .await
                .unwrap());
            let err = alice
                .resolve_table(&TableReference {
                    catalog: Some("datalake".into()),
                    schema: Some(written.into()),
                    table: "salaries".into(),
                })
                .await
                .unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("schema not found: datalake.{written}"),
                "{written:?} reads as a missing schema"
            );
        }
        assert_eq!(
            alice.visible_table_names("datalake", "sales").await,
            vec!["orders"]
        );
    }

    /// The conformance check passes exact and correctly reporting
    /// connectors, and catches a loose one that reports nothing.
    #[tokio::test]
    async fn conformance_check_catches_loose_connectors_without_canonical_names() {
        conformance::check_name_resolution(&LooseCatalog)
            .await
            .unwrap();
        conformance::check_name_resolution(&HmsLikeCatalog)
            .await
            .unwrap();
        let memory = MemoryCatalog::new();
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("Orders", Arc::new(MemoryTable::new(test_columns())));
        memory.register_schema("Sales", schema);
        conformance::check_name_resolution(&memory).await.unwrap();

        #[derive(Debug)]
        struct Forgetful;
        #[async_trait]
        impl SchemaProvider for Forgetful {
            async fn table_names(&self) -> Vec<String> {
                vec!["secret".into()]
            }
            async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
                name.eq_ignore_ascii_case("secret")
                    .then(|| Arc::new(MemoryTable::new(test_columns())) as Arc<dyn TableProvider>)
            }
            fn canonical_name(&self) -> Option<String> {
                None
            }
        }
        #[derive(Debug)]
        struct ForgetfulCatalog;
        #[async_trait]
        impl CatalogProvider for ForgetfulCatalog {
            async fn schema_names(&self) -> Vec<String> {
                vec!["public".into()]
            }
            async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
                (name == "public").then(|| Arc::new(Forgetful) as Arc<dyn SchemaProvider>)
            }
        }
        let err = conformance::check_name_resolution(&ForgetfulCatalog)
            .await
            .unwrap_err();
        assert!(err.contains("public.secret"), "{err}");

        // A connector reporting a listed, but wrong, stored schema fails.
        #[derive(Debug)]
        struct Misreporting;
        #[async_trait]
        impl CatalogProvider for Misreporting {
            async fn schema_names(&self) -> Vec<String> {
                vec!["hr".into(), "public".into()]
            }
            async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
                resolve_loosely(&["hr", "public"], name)?;
                Some(Arc::new(LooseSchema {
                    name: "hr".into(),
                    tables: &[],
                }))
            }
        }
        let err = conformance::check_name_resolution(&Misreporting)
            .await
            .unwrap_err();
        assert!(err.contains("\"public\""), "{err}");
    }

    /// Without rules, loose resolution is unchanged.
    #[tokio::test]
    async fn loose_resolution_without_rules_is_unchanged() {
        let root = CatalogManager::new("pg", "public");
        root.register_catalog("pg", Arc::new(LooseCatalog));
        for (schema, table) in [
            ("PUBLIC", "SECRET"),
            ("public", "orders"),
            ("Hr", "Salaries"),
        ] {
            assert!(
                fold_read(&root, schema, table).await.is_ok(),
                "{schema}.{table}"
            );
        }
        assert_eq!(
            root.visible_table_names("pg", "PUBLIC").await,
            vec!["Orders", "open", "secret"]
        );
    }

    /// A loose connector that reports no stored name (`None`) fails
    /// closed under rules: only names it lists exactly resolve, so a deny
    /// rule on the stored name cannot be bypassed by writing it in
    /// another case. Without rules its loose matching is unchanged.
    #[tokio::test]
    async fn a_loose_connector_reporting_no_stored_name_fails_closed() {
        #[derive(Debug)]
        struct Silent;
        #[async_trait]
        impl SchemaProvider for Silent {
            async fn table_names(&self) -> Vec<String> {
                vec!["open".into(), "secret".into()]
            }
            async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
                resolve_loosely(&["open", "secret"], name)
                    .map(|_| Arc::new(MemoryTable::new(test_columns())) as Arc<dyn TableProvider>)
            }
            fn canonical_name(&self) -> Option<String> {
                None
            }
        }
        #[derive(Debug)]
        struct SilentCatalog;
        #[async_trait]
        impl CatalogProvider for SilentCatalog {
            async fn schema_names(&self) -> Vec<String> {
                vec!["public".into()]
            }
            async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
                resolve_loosely(&["public"], name)
                    .map(|_| Arc::new(Silent) as Arc<dyn SchemaProvider>)
            }
        }
        let root = CatalogManager::new("pg", "public");
        root.register_catalog("pg", Arc::new(SilentCatalog));
        let rules = r#"{"tables": [
            {"user": "alice", "table": "secret", "privileges": []},
            {"privileges": ["SELECT"]}
        ]}"#;
        let alice = root.with_access("alice", Arc::new(AccessRules::from_json(rules).unwrap()));
        assert!(fold_read(&alice, "public", "open").await.is_ok());
        for (schema, table) in [("public", "SECRET"), ("public", "OPEN"), ("PUBLIC", "open")] {
            let err = fold_read(&alice, schema, table).await.unwrap_err();
            assert!(
                matches!(err, CatalogError::SchemaNotFound(_)),
                "{schema}.{table}: {err}"
            );
        }
        assert!(!alice.visible_schema_exists("pg", "PUBLIC").await.unwrap());
        assert!(fold_read(&root, "PUBLIC", "SECRET").await.is_ok());
    }

    /// An unreachable source is an error, not a missing schema: reads
    /// and `visible_schema_exists` report it, so `SET search_path` can
    /// fail instead of falling back to another schema.
    #[tokio::test]
    async fn an_unreachable_source_is_not_a_missing_schema() {
        let root = CatalogManager::new("lake", "sales");
        root.register_catalog("lake", Arc::new(conformance::Unreachable::default()));
        let rules = r#"{"tables": [{"privileges": ["SELECT"]}]}"#;
        let alice = root.with_access("alice", Arc::new(AccessRules::from_json(rules).unwrap()));
        for manager in [&root, &alice] {
            assert!(matches!(
                manager.visible_schema_exists("lake", "sales").await,
                Err(CatalogError::Unavailable(_))
            ));
            let err = manager
                .resolve_table(&TableReference {
                    catalog: Some("lake".into()),
                    schema: Some("sales".into()),
                    table: "orders".into(),
                })
                .await
                .unwrap_err();
            assert!(matches!(err, CatalogError::Unavailable(_)), "{err}");
            assert!(matches!(
                manager.try_visible_table_names("lake", "sales").await,
                Err(CatalogError::Unavailable(_))
            ));
            assert!(matches!(
                manager.try_visible_schema_names("lake").await,
                Err(CatalogError::Unavailable(_))
            ));
            // The lenient listings show nothing (and log it).
            assert!(manager
                .visible_table_names("lake", "sales")
                .await
                .is_empty());
        }
        let err = conformance::check_name_resolution(&conformance::Unreachable::default())
            .await
            .unwrap_err();
        assert!(err.contains("metastore down"), "{err}");
    }

    /// A schema that still resolves during an outage (as Hive's name cache
    /// serves it) reports the outage when its tables are listed or loaded,
    /// never an empty schema or a missing table, with or without rules;
    /// the conformance check fails rather than passing on it.
    #[tokio::test]
    async fn an_unreachable_source_behind_a_cached_schema_is_not_empty_or_missing() {
        let lake = conformance::Unreachable {
            cached: vec!["sales".into()],
        };
        let root = CatalogManager::new("lake", "sales");
        root.register_catalog(
            "lake",
            Arc::new(conformance::Unreachable {
                cached: vec!["sales".into()],
            }),
        );
        let rules = r#"{"tables": [{"privileges": ["SELECT"]}]}"#;
        let alice = root.with_access("alice", Arc::new(AccessRules::from_json(rules).unwrap()));
        for manager in [&root, &alice] {
            assert!(manager
                .visible_schema_exists("lake", "sales")
                .await
                .unwrap());
            assert!(matches!(
                manager.try_visible_table_names("lake", "sales").await,
                Err(CatalogError::Unavailable(_))
            ));
            let err = manager
                .resolve_table(&TableReference {
                    catalog: Some("lake".into()),
                    schema: Some("sales".into()),
                    table: "ORDERS".into(),
                })
                .await
                .unwrap_err();
            assert!(matches!(err, CatalogError::Unavailable(_)), "{err}");
        }
        let err = conformance::check_name_resolution(&lake).await.unwrap_err();
        assert!(err.contains("metastore down"), "{err}");
    }

    /// A catalog that is legitimately empty passes the conformance check;
    /// only one that can't be asked fails it.
    #[tokio::test]
    async fn the_conformance_check_passes_an_empty_catalog() {
        conformance::check_name_resolution(&MemoryCatalog::new())
            .await
            .unwrap();
    }

    /// The connector is asked, before it loads a table, whether the
    /// stored name may be read, so a denied table costs no more than a
    /// missing one.
    #[tokio::test]
    async fn a_denied_table_is_refused_before_it_is_loaded() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static LOADS: AtomicUsize = AtomicUsize::new(0);
        #[derive(Debug)]
        struct Gated;
        #[async_trait]
        impl SchemaProvider for Gated {
            async fn table_names(&self) -> Vec<String> {
                vec!["open".into(), "secret".into()]
            }
            async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
                self.table_if_allowed(name, &|_, _| true)
                    .await
                    .ok()?
                    .found()
            }
            async fn table_if_allowed(
                &self,
                name: &str,
                allowed: TableGate<'_>,
            ) -> Result<TableLookup, CatalogError> {
                let Some(stored) = resolve_loosely(&["open", "secret"], name) else {
                    return Ok(TableLookup::Missing);
                };
                if !allowed("public", &stored) {
                    return Ok(TableLookup::Denied);
                }
                LOADS.fetch_add(1, Ordering::SeqCst);
                Ok(TableLookup::Found(Arc::new(LooseTable {
                    schema: "public".into(),
                    name: stored,
                })))
            }
            fn canonical_name(&self) -> Option<String> {
                Some("public".into())
            }
        }
        #[derive(Debug)]
        struct GatedCatalog;
        #[async_trait]
        impl CatalogProvider for GatedCatalog {
            async fn schema_names(&self) -> Vec<String> {
                vec!["public".into()]
            }
            async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
                (name == "public").then(|| Arc::new(Gated) as Arc<dyn SchemaProvider>)
            }
        }
        let root = CatalogManager::new("pg", "public");
        root.register_catalog("pg", Arc::new(GatedCatalog));
        let rules = r#"{"tables": [
            {"user": "alice", "table": "secret", "privileges": []},
            {"privileges": ["SELECT"]}
        ]}"#;
        let alice = root.with_access("alice", Arc::new(AccessRules::from_json(rules).unwrap()));
        let read = |table: &str| TableReference {
            catalog: Some("pg".into()),
            schema: Some("public".into()),
            table: table.into(),
        };
        assert!(alice.resolve_table(&read("SECRET")).await.is_err());
        assert_eq!(LOADS.load(Ordering::SeqCst), 0, "denied table was loaded");
        assert!(alice.resolve_table(&read("OPEN")).await.is_ok());
        assert_eq!(LOADS.load(Ordering::SeqCst), 1);
    }

    /// The upgrade note's example: an allow rule written in another case
    /// than the stored schema name no longer grants the read.
    #[tokio::test]
    async fn an_allow_rule_must_match_the_stored_name() {
        let root = CatalogManager::new("datalake", "sales");
        root.register_catalog("datalake", Arc::new(HmsLikeCatalog));
        let read = |rule: &str| {
            let rules = format!(
                r#"{{"tables": [
                    {{"user": "alice", "schema": "{rule}", "privileges": ["SELECT"]}},
                    {{"privileges": []}}
                ]}}"#
            );
            let alice =
                root.with_access("alice", Arc::new(AccessRules::from_json(&rules).unwrap()));
            async move {
                alice
                    .resolve_table(&TableReference {
                        catalog: Some("datalake".into()),
                        schema: Some("SALES".into()),
                        table: "orders".into(),
                    })
                    .await
            }
        };
        assert!(matches!(
            read("SALES").await.unwrap_err(),
            CatalogError::SchemaNotFound(_)
        ));
        assert!(read("(?i)sales").await.is_ok());
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
    async fn unmatched_names_fold_to_lowercase() {
        let cm = CatalogManager::new("lake", "sales");
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("nation", Arc::new(MemoryTable::new(test_columns())));
        schema.register_table("MixedT", Arc::new(MemoryTable::new(test_columns())));
        let catalog = Arc::new(MemoryCatalog::new());
        catalog.register_schema("sales", schema);
        cm.register_catalog("lake", catalog);
        let fold = |catalog: Option<&str>, schema: Option<&str>, table: &str| {
            let reference = TableReference {
                catalog: catalog.map(str::to_string),
                schema: schema.map(str::to_string),
                table: table.to_string(),
            };
            let cm = &cm;
            async move {
                cm.fold_reference_case(&reference)
                    .await
                    .unwrap()
                    .to_string()
            }
        };
        assert_eq!(fold(None, None, "NATION").await, "nation");
        assert_eq!(
            fold(Some("LAKE"), Some("Sales"), "Nation").await,
            "lake.sales.nation"
        );
        // A name that matches as written is kept; one that matches nothing
        // either way is left for resolve_table to report.
        assert_eq!(fold(None, None, "MixedT").await, "MixedT");
        assert_eq!(fold(None, None, "MIXEDT").await, "MIXEDT");
        assert_eq!(fold(Some("NOPE"), None, "T").await, "NOPE.T");
    }

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

    // -- Statistics tests --

    #[test]
    fn table_statistics_default_is_all_none() {
        let stats = TableStatistics::default();
        assert_eq!(stats.row_count, None);
        assert_eq!(stats.size_bytes, None);
        assert!(stats.columns.is_empty());
    }

    #[test]
    fn column_statistics_default_is_all_none() {
        let stats = ColumnStatistics::default();
        assert_eq!(stats.ndv, None);
        assert_eq!(stats.null_fraction, None);
        assert_eq!(stats.min_value, None);
        assert_eq!(stats.max_value, None);
    }

    #[test]
    fn table_statistics_round_trips_through_clone() {
        let mut columns = HashMap::new();
        columns.insert(
            "id".to_string(),
            ColumnStatistics {
                ndv: Some(1_000_000),
                null_fraction: Some(0.0),
                min_value: Some(ScalarValue::Int64(1)),
                max_value: Some(ScalarValue::Int64(1_000_000)),
            },
        );
        let stats = TableStatistics {
            row_count: Some(1_000_000),
            size_bytes: Some(64 * 1024 * 1024),
            columns,
        };
        let cloned = stats.clone();
        assert_eq!(stats, cloned);
    }

    #[test]
    fn default_table_provider_returns_no_statistics() {
        // MemoryTable does not override `statistics()`, so it picks up the
        // default `None` implementation defined on `TableProvider`.
        let table = MemoryTable::new(test_columns());
        assert!(table.statistics().is_none());
    }

    #[test]
    fn table_statistics_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TableStatistics>();
        assert_send_sync::<ColumnStatistics>();
    }
}
