//! The external catalogs (`hive`, `postgresql`) a node serves, kept live so
//! they can change at runtime (runtime-config D6, D8). The `memory` and
//! `file` catalogs are not managed here: they are startup-only.
//!
//! [`LiveCatalogs`] builds a catalog's provider and connector from its spec,
//! installs both into the shared [`CatalogManager`] and [`ConnectorRegistry`],
//! and remembers the installed specs plus the catalog generation they belong
//! to. A query keeps the provider `Arc` it was planned with, so a change
//! never disturbs a running statement.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use arneb_catalog::{CatalogManager, CatalogProvider};
use arneb_connectors::{ConnectorFactory, ConnectorRegistry, StorageRegistry};

use crate::config::{CatalogConfig, RedactedString, StorageConfig};

/// How long [`LiveCatalogs::build`] waits for a metastore or database.
const BUILD_TIMEOUT: Duration = Duration::from_secs(10);

/// The value that replaces a secret in [`redacted_spec`].
// The catalog API (runtime-config D7) is the first caller outside tests.
#[allow(dead_code)]
pub const REDACTED: &str = "<redacted>";

/// Catalog names a spec may not take: the startup-only catalogs and the
/// metadata namespaces.
// The catalog API (runtime-config D7) is the first caller outside tests.
#[allow(dead_code)]
const RESERVED_NAMES: [&str; 5] = [
    "memory",
    "file",
    "system",
    "information_schema",
    "pg_catalog",
];

/// A catalog's provider and connector, built but not yet installed.
#[derive(Clone)]
pub struct BuiltCatalog {
    /// What planning resolves schemas and tables through.
    pub provider: Arc<dyn CatalogProvider>,
    /// What execution creates data sources through.
    pub connector: Arc<dyn ConnectorFactory>,
}

impl std::fmt::Debug for BuiltCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltCatalog")
            .field("connector", &self.connector.name())
            .finish_non_exhaustive()
    }
}

/// Why a catalog could not be built. Messages never carry a secret.
#[derive(Debug, thiserror::Error)]
pub enum CatalogBuildError {
    /// The spec is incomplete or has a bad value.
    #[error("invalid catalog: {0}")]
    Invalid(String),
    /// The metastore or database could not be reached.
    #[error("unreachable: {0}")]
    Unreachable(String),
    /// The metastore or database did not answer within 10 s.
    #[error("no answer within {} s", BUILD_TIMEOUT.as_secs())]
    Timeout,
}

/// What [`LiveCatalogs::apply_set`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyReport {
    /// False when the set's generation was not newer than the one held, so
    /// nothing changed.
    pub applied: bool,
    /// `(catalog, error)` for each new or changed catalog that failed to
    /// build and was skipped.
    pub failed: Vec<(String, String)>,
}

/// The node's live external catalogs. Share as `Arc<LiveCatalogs>`.
pub struct LiveCatalogs {
    manager: Arc<CatalogManager>,
    connectors: Arc<ConnectorRegistry>,
    global_storage: StorageConfig,
    /// The installed specs, by name.
    specs: RwLock<BTreeMap<String, CatalogConfig>>,
    generation: AtomicU64,
    /// Serializes [`apply_set`](Self::apply_set) calls, which build outside
    /// any other lock.
    applying: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for LiveCatalogs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveCatalogs")
            .field("generation", &self.generation())
            .field("catalogs", &self.read_specs().keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl LiveCatalogs {
    /// An empty set at generation 0, installing into `manager` and
    /// `connectors`. `global_storage` is the `[storage]` a catalog's own
    /// `storage` is merged over.
    pub fn new(
        manager: Arc<CatalogManager>,
        connectors: Arc<ConnectorRegistry>,
        global_storage: StorageConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            manager,
            connectors,
            global_storage,
            specs: RwLock::new(BTreeMap::new()),
            generation: AtomicU64::new(0),
            applying: tokio::sync::Mutex::new(()),
        })
    }

    /// The catalog generation the installed set belongs to.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Sets the catalog generation (after a change was persisted).
    pub fn set_generation(&self, g: u64) {
        self.generation.store(g, Ordering::SeqCst);
    }

    /// The installed specs, in name order (raw secrets included).
    pub fn specs(&self) -> Vec<CatalogConfig> {
        self.read_specs().values().cloned().collect()
    }

    fn read_specs(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, CatalogConfig>> {
        self.specs.read().unwrap_or_else(|p| p.into_inner())
    }

    fn write_specs(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<String, CatalogConfig>> {
        self.specs.write().unwrap_or_else(|p| p.into_inner())
    }

    /// Builds the catalog `spec` describes (connects to its metastore or
    /// database) without installing it. Gives up after 10 s.
    pub async fn build(&self, spec: &CatalogConfig) -> Result<BuiltCatalog, CatalogBuildError> {
        match tokio::time::timeout(BUILD_TIMEOUT, self.build_inner(spec)).await {
            Ok(r) => r,
            Err(_) => Err(CatalogBuildError::Timeout),
        }
    }

    async fn build_inner(&self, spec: &CatalogConfig) -> Result<BuiltCatalog, CatalogBuildError> {
        match spec.catalog_type.as_str() {
            "hive" => self.build_hive(spec).await,
            "postgresql" => build_postgresql(spec).await,
            other => Err(CatalogBuildError::Invalid(format!(
                "unsupported catalog type '{other}'"
            ))),
        }
    }

    /// Hive (Iceberg tables in the same metastore are redirected to the
    /// Iceberg reader by the Hive catalog).
    async fn build_hive(&self, spec: &CatalogConfig) -> Result<BuiltCatalog, CatalogBuildError> {
        let metastore_uri = spec.metastore_uri.as_deref().ok_or_else(|| {
            CatalogBuildError::Invalid("hive catalog requires metastore_uri".into())
        })?;
        // Per-catalog storage: merge the catalog override over [storage].
        let merged_storage = StorageConfig::merge(&self.global_storage, spec.storage.as_ref());
        let storage = Arc::new(StorageRegistry::with_config(
            merged_storage.to_cloud_config(),
        ));
        let hms_client = arneb_hive::catalog::HmsClient::new(metastore_uri)
            .await
            .map_err(|e| CatalogBuildError::Unreachable(format!("{metastore_uri}: {e}")))?;
        let provider = Arc::new(arneb_hive::catalog::HiveCatalogProvider::new(
            Arc::new(hms_client),
            storage.clone(),
        ));
        let connector = Arc::new(arneb_hive::datasource::HiveConnectorFactory::new(storage));
        tracing::info!(
            catalog = %spec.name,
            metastore = %metastore_uri,
            "connected to hive metastore"
        );
        Ok(BuiltCatalog {
            provider,
            connector,
        })
    }

    /// Installs a built catalog under `spec.name`, replacing one of the same
    /// name. The connector goes in first, so a statement planned against
    /// the new provider always finds it.
    pub fn install(&self, spec: CatalogConfig, built: BuiltCatalog) {
        let name = spec.name.clone();
        self.connectors.register(name.clone(), built.connector);
        self.manager.register_catalog(name.clone(), built.provider);
        self.write_specs().insert(name, spec);
    }

    /// Removes the installed catalog `name`. False when no such catalog is
    /// installed here (the startup-only catalogs are never touched).
    pub fn remove(&self, name: &str) -> bool {
        if self.write_specs().remove(name).is_none() {
            return false;
        }
        self.manager.deregister_catalog(name);
        self.connectors.deregister(name);
        true
    }

    /// Makes the installed set equal `specs` at `generation`, unless that
    /// generation is not newer than the one held (then nothing changes).
    /// Every new or changed catalog is built first; then they are swapped
    /// in and the catalogs not in the set are removed. A catalog that fails
    /// to build is skipped with a WARN and listed in the report (a changed
    /// one keeps its previous version); the rest still apply and the
    /// generation still advances.
    pub async fn apply_set(&self, generation: u64, specs: Vec<CatalogConfig>) -> ApplyReport {
        let _applying = self.applying.lock().await;
        if generation <= self.generation() {
            return ApplyReport::default();
        }
        let installed = self.read_specs().clone();
        let mut built = Vec::new();
        let mut failed = Vec::new();
        let mut wanted = std::collections::BTreeSet::new();
        for spec in specs {
            wanted.insert(spec.name.clone());
            if installed
                .get(&spec.name)
                .is_some_and(|current| same_spec(current, &spec))
            {
                continue;
            }
            match self.build(&spec).await {
                Ok(b) => built.push((spec, b)),
                Err(e) => {
                    tracing::warn!(
                        catalog = %spec.name,
                        catalog_type = %spec.catalog_type,
                        error = %e,
                        "failed to build catalog, skipping"
                    );
                    failed.push((spec.name.clone(), e.to_string()));
                }
            }
        }
        for (spec, b) in built {
            tracing::info!(
                catalog = %spec.name,
                catalog_type = %spec.catalog_type,
                generation,
                "registered catalog"
            );
            self.install(spec, b);
        }
        for name in installed.keys().filter(|n| !wanted.contains(*n)) {
            self.remove(name);
            tracing::info!(catalog = %name, generation, "removed catalog");
        }
        self.set_generation(generation);
        ApplyReport {
            applied: true,
            failed,
        }
    }
}

/// PostgreSQL. A bad config is `Invalid`; a failed connection
/// `Unreachable` (the connector's errors never carry the password).
async fn build_postgresql(spec: &CatalogConfig) -> Result<BuiltCatalog, CatalogBuildError> {
    let pg_config = spec
        .postgres_config()
        .map_err(|e| CatalogBuildError::Invalid(e.to_string()))?;
    let target = pg_config.display_target();
    let catalog = arneb_postgresql::PostgresCatalog::connect(&spec.name, pg_config)
        .await
        .map_err(|e| CatalogBuildError::Unreachable(e.to_string()))?;
    let c = catalog.config();
    tracing::info!(
        catalog = %spec.name,
        target = %target,
        max_connections = c.max_connections,
        split_min_rows = c.split_min_rows,
        max_splits = c.max_splits,
        unconstrained_numeric_scale = c.unconstrained_numeric_scale,
        connect_timeout_secs = c.connect_timeout_secs,
        pool_wait_timeout_secs = c.pool_wait_timeout_secs,
        domain_compaction_threshold = c.domain_compaction_threshold,
        "connected to postgresql"
    );
    let connector = Arc::new(catalog.connector_factory());
    Ok(BuiltCatalog {
        provider: Arc::new(catalog),
        connector,
    })
}

fn same_spec(a: &CatalogConfig, b: &CatalogConfig) -> bool {
    match (serde_json::to_value(a), serde_json::to_value(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// Checks a runtime catalog spec (D6): the name is 1–63 characters of
/// `[a-z0-9_]` starting with a letter and not reserved; the type is `hive`
/// (with `metastore_uri`) or `postgresql` (with a valid `connection_url`
/// and settings). The message never carries a secret.
// The catalog API (runtime-config D7) is the first caller outside tests.
#[allow(dead_code)]
pub fn validate_spec(spec: &CatalogConfig) -> Result<(), String> {
    let name = spec.name.as_str();
    let well_formed = (1..=63).contains(&name.len())
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if !well_formed {
        return Err(format!(
            "catalog name {name:?} must be 1-63 characters of [a-z0-9_], starting with a letter"
        ));
    }
    if RESERVED_NAMES.contains(&name) {
        return Err(format!("catalog name {name:?} is reserved"));
    }
    match spec.catalog_type.as_str() {
        "hive" => {
            if spec.metastore_uri.as_deref().is_none_or(str::is_empty) {
                return Err(format!("hive catalog '{name}' requires metastore_uri"));
            }
        }
        "postgresql" => {
            spec.postgres_config().map_err(|e| e.to_string())?;
        }
        other => {
            return Err(format!(
                "unsupported catalog type '{other}' (expected hive or postgresql)"
            ))
        }
    }
    Ok(())
}

/// The spec as JSON with every secret replaced by `"<redacted>"`: the
/// `password`, the password inside `connection_url`, and the storage
/// `secret_access_key`, `service_account_path` and Azure `access_key`.
/// Every API response, MCP result and audit entry uses this form.
// The catalog API (runtime-config D7) is the first caller outside tests.
#[allow(dead_code)]
pub fn redacted_spec(spec: &CatalogConfig) -> serde_json::Value {
    let mut spec = spec.clone();
    let redact = |v: &mut Option<String>| {
        if v.is_some() {
            *v = Some(REDACTED.to_owned());
        }
    };
    if spec.password.is_some() {
        spec.password = Some(RedactedString(REDACTED.to_owned()));
    }
    if let Some(url) = &mut spec.connection_url {
        let mut ranges: Vec<_> = url_query_passwords(&url.0)
            .into_iter()
            .map(|(_, value)| value)
            .chain(url_password_range(&url.0))
            .collect();
        // Replace from the end so the earlier ranges stay valid.
        ranges.sort_by_key(|r| std::cmp::Reverse(r.start));
        for range in ranges {
            url.0.replace_range(range, REDACTED);
        }
    }
    if let Some(storage) = &mut spec.storage {
        if let Some(s3) = &mut storage.s3 {
            redact(&mut s3.secret_access_key);
        }
        if let Some(gcs) = &mut storage.gcs {
            redact(&mut gcs.service_account_path);
        }
        if let Some(azure) = &mut storage.azure {
            redact(&mut azure.access_key);
        }
    }
    serde_json::to_value(&spec).unwrap_or(serde_json::Value::Null)
}

/// Fills every secret of `new` that is exactly `"<redacted>"` from
/// `stored`, so a spec read through the API can be sent back unchanged
/// (D6). A secret `stored` lacks is cleared.
// The catalog API (runtime-config D7) is the first caller outside tests.
#[allow(dead_code)]
pub fn fill_redacted(new: &mut CatalogConfig, stored: &CatalogConfig) {
    let is_redacted = |v: &Option<String>| v.as_deref() == Some(REDACTED);
    if new.password.as_ref().is_some_and(|p| p.0 == REDACTED) {
        new.password = stored.password.clone();
    }
    if let Some(url) = &mut new.connection_url {
        // The query comes after the userinfo, so fill it first: that keeps
        // the userinfo range valid.
        let stored_query_password = stored.connection_url.as_ref().and_then(|s| {
            url_query_passwords(&s.0)
                .first()
                .map(|(_, value)| s.0[value.clone()].to_owned())
        });
        for (segment, value) in url_query_passwords(&url.0).into_iter().rev() {
            if url.0[value.clone()] != *REDACTED {
                continue;
            }
            match &stored_query_password {
                Some(p) => url.0.replace_range(value, p),
                // No stored password: drop the parameter and one separator.
                None => {
                    let drop = if url.0[..segment.start].ends_with('&') {
                        segment.start - 1..segment.end
                    } else if url.0[segment.end..].starts_with('&') {
                        segment.start..segment.end + 1
                    } else {
                        segment.start - 1..segment.end
                    };
                    url.0.replace_range(drop, "");
                }
            }
        }
        if let Some(range) = url_password_range(&url.0) {
            if &url.0[range.clone()] == REDACTED {
                let stored_password = stored
                    .connection_url
                    .as_ref()
                    .and_then(|s| url_password_range(&s.0).map(|r| s.0[r].to_owned()));
                match stored_password {
                    Some(p) => url.0.replace_range(range, &p),
                    // No stored password: drop the `:<redacted>` part.
                    None => url.0.replace_range(range.start - 1..range.end, ""),
                }
            }
        }
    }
    let stored_storage = stored.storage.as_ref();
    if let Some(storage) = &mut new.storage {
        if let Some(s3) = &mut storage.s3 {
            if is_redacted(&s3.secret_access_key) {
                s3.secret_access_key = stored_storage
                    .and_then(|s| s.s3.as_ref())
                    .and_then(|s| s.secret_access_key.clone());
            }
        }
        if let Some(gcs) = &mut storage.gcs {
            if is_redacted(&gcs.service_account_path) {
                gcs.service_account_path = stored_storage
                    .and_then(|s| s.gcs.as_ref())
                    .and_then(|s| s.service_account_path.clone());
            }
        }
        if let Some(azure) = &mut storage.azure {
            if is_redacted(&azure.access_key) {
                azure.access_key = stored_storage
                    .and_then(|s| s.azure.as_ref())
                    .and_then(|s| s.access_key.clone());
            }
        }
    }
}

/// The byte range of the password in `scheme://user:password@host/...`,
/// if the URL's userinfo has one.
// The catalog API (runtime-config D7) is the first caller outside tests.
#[allow(dead_code)]
fn url_password_range(url: &str) -> Option<std::ops::Range<usize>> {
    let authority_start = url.find("://")? + 3;
    let rest = &url[authority_start..];
    let authority_len = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_len];
    let at = authority.rfind('@')?;
    let colon = authority[..at].find(':')?;
    Some(authority_start + colon + 1..authority_start + at)
}

/// `(parameter, value)` byte ranges of every `password=` query parameter
/// (libpq URIs accept the password there too).
// The catalog API (runtime-config D7) is the first caller outside tests.
#[allow(dead_code)]
fn url_query_passwords(url: &str) -> Vec<(std::ops::Range<usize>, std::ops::Range<usize>)> {
    let Some(scheme_end) = url.find("://") else {
        return Vec::new();
    };
    let Some(q) = url[scheme_end..].find('?').map(|i| scheme_end + i + 1) else {
        return Vec::new();
    };
    let end = url[q..].find('#').map_or(url.len(), |i| q + i);
    let mut out = Vec::new();
    let mut start = q;
    for param in url[q..end].split('&') {
        if let Some(value) = param.strip_prefix("password=") {
            let value_start = start + param.len() - value.len();
            out.push((start..start + param.len(), value_start..start + param.len()));
        }
        start += param.len() + 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory};

    fn spec(json: serde_json::Value) -> CatalogConfig {
        serde_json::from_value(json).unwrap()
    }

    fn hive(name: &str) -> CatalogConfig {
        spec(serde_json::json!({"name": name, "type": "hive", "metastore_uri": "127.0.0.1:9083"}))
    }

    fn live() -> (
        Arc<LiveCatalogs>,
        Arc<CatalogManager>,
        Arc<ConnectorRegistry>,
    ) {
        let manager = Arc::new(CatalogManager::new("memory", "default"));
        let connectors = Arc::new(ConnectorRegistry::new());
        let live = LiveCatalogs::new(
            manager.clone(),
            connectors.clone(),
            StorageConfig::default(),
        );
        (live, manager, connectors)
    }

    fn fake_built() -> BuiltCatalog {
        let catalog = Arc::new(MemoryCatalog::new());
        BuiltCatalog {
            provider: catalog.clone(),
            connector: Arc::new(MemoryConnectorFactory::new(catalog, "default")),
        }
    }

    /// A TCP port with nothing listening on it.
    fn closed_port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    }

    #[test]
    fn validate_spec_rejects_reserved_and_bad_names() {
        assert_eq!(validate_spec(&hive("lake_1")), Ok(()));
        for bad in ["memory", "File", "1x", &"a".repeat(64), "", "pg-prod"] {
            assert!(
                validate_spec(&hive(bad)).is_err(),
                "{bad:?} must be refused"
            );
        }
        assert!(validate_spec(&hive(&"a".repeat(63))).is_ok());
        for reserved in ["file", "system", "information_schema", "pg_catalog"] {
            let e = validate_spec(&hive(reserved)).unwrap_err();
            assert!(e.contains("reserved"), "{e}");
        }
        let mysql = spec(serde_json::json!({"name": "m", "type": "mysql"}));
        assert!(validate_spec(&mysql).unwrap_err().contains("mysql"));
        let no_uri = spec(serde_json::json!({"name": "h", "type": "hive"}));
        assert!(validate_spec(&no_uri)
            .unwrap_err()
            .contains("metastore_uri"));
        let no_url = spec(serde_json::json!({"name": "p", "type": "postgresql"}));
        assert!(validate_spec(&no_url)
            .unwrap_err()
            .contains("connection_url"));
        let pg = spec(serde_json::json!({
            "name": "p", "type": "postgresql", "connection_url": "postgresql://h:5432/db"
        }));
        assert_eq!(validate_spec(&pg), Ok(()));
    }

    fn secret_spec() -> CatalogConfig {
        spec(serde_json::json!({
            "name": "p",
            "type": "postgresql",
            "connection_url": "postgresql://bob:url-secret-1@db.internal:5432/sales",
            "user": "bob",
            "password": "pw-secret-2",
            "storage": {
                "s3": {"region": "us-east-1", "access_key_id": "AKIA", "secret_access_key": "s3-secret-3"},
                "gcs": {"service_account_path": "/keys/gcs-secret-4.json"},
                "azure": {"storage_account": "acct", "access_key": "az-secret-5"}
            }
        }))
    }

    #[test]
    fn redacted_spec_hides_every_secret() {
        let v = redacted_spec(&secret_spec());
        let text = v.to_string();
        for raw in [
            "url-secret-1",
            "pw-secret-2",
            "s3-secret-3",
            "gcs-secret-4",
            "az-secret-5",
        ] {
            assert!(
                !text.contains(raw),
                "a secret leaked into the redacted spec"
            );
        }
        assert_eq!(
            v["connection_url"],
            "postgresql://bob:<redacted>@db.internal:5432/sales"
        );
        assert_eq!(v["password"], REDACTED);
        assert_eq!(v["storage"]["s3"]["secret_access_key"], REDACTED);
        assert_eq!(v["storage"]["s3"]["access_key_id"], "AKIA");
        assert_eq!(v["storage"]["gcs"]["service_account_path"], REDACTED);
        assert_eq!(v["storage"]["azure"]["access_key"], REDACTED);
        assert_eq!(v["user"], "bob");
        // No secret: nothing to hide, and no placeholder appears.
        let plain = redacted_spec(&spec(serde_json::json!({
            "name": "p", "type": "postgresql", "connection_url": "postgresql://bob@h/db"
        })));
        assert_eq!(plain["connection_url"], "postgresql://bob@h/db");
        assert!(plain["password"].is_null());
        // libpq URIs also take the password as a query parameter.
        let query = redacted_spec(&spec(serde_json::json!({
            "name": "p", "type": "postgresql",
            "connection_url":
                "postgresql://bob:ui-secret-7@db/sales?sslmode=disable&password=q-secret-6&application_name=x"
        })));
        let text = query.to_string();
        assert!(!text.contains("q-secret-6") && !text.contains("ui-secret-7"));
        assert_eq!(
            query["connection_url"],
            "postgresql://bob:<redacted>@db/sales?sslmode=disable&password=<redacted>&application_name=x"
        );
        let only = redacted_spec(&spec(serde_json::json!({
            "name": "p", "type": "postgresql",
            "connection_url": "postgresql://bob@db/sales?password=q-secret-6"
        })));
        assert_eq!(
            only["connection_url"],
            "postgresql://bob@db/sales?password=<redacted>"
        );
    }

    #[test]
    fn catalog_config_debug_hides_every_secret() {
        let mut s = secret_spec();
        s.connection_url = None; // `RedactedString` already hides the URL
        let shown = format!("{s:?}");
        for raw in ["pw-secret-2", "s3-secret-3", "gcs-secret-4", "az-secret-5"] {
            assert!(!shown.contains(raw), "a secret leaked into Debug");
        }
        let shown = format!("{:?}", secret_spec());
        assert!(!shown.contains("url-secret-1"));
        assert!(shown.contains("AKIA"), "non-secret fields still show");
    }

    #[test]
    fn fill_redacted_restores_stored_secrets() {
        let stored = secret_spec();
        let mut round_trip: CatalogConfig = serde_json::from_value(redacted_spec(&stored)).unwrap();
        fill_redacted(&mut round_trip, &stored);
        assert_eq!(
            serde_json::to_value(&round_trip).unwrap(),
            serde_json::to_value(&stored).unwrap(),
            "a GET body sent back unchanged keeps every stored secret"
        );

        // A new secret wins; a redacted one the stored spec lacks is cleared.
        let mut changed: CatalogConfig = serde_json::from_value(serde_json::json!({
            "name": "p",
            "type": "postgresql",
            "connection_url": "postgresql://bob:<redacted>@other:5432/sales",
            "password": "new-password"
        }))
        .unwrap();
        fill_redacted(&mut changed, &stored);
        assert_eq!(
            changed.connection_url.as_ref().unwrap().0,
            "postgresql://bob:url-secret-1@other:5432/sales"
        );
        assert_eq!(changed.password.as_ref().unwrap().0, "new-password");
        let mut orphan: CatalogConfig = serde_json::from_value(serde_json::json!({
            "name": "p", "type": "postgresql",
            "connection_url": "postgresql://bob:<redacted>@h/db",
            "password": "<redacted>"
        }))
        .unwrap();
        let bare = spec(serde_json::json!({
            "name": "p", "type": "postgresql", "connection_url": "postgresql://bob@h/db"
        }));
        fill_redacted(&mut orphan, &bare);
        assert_eq!(
            orphan.connection_url.as_ref().unwrap().0,
            "postgresql://bob@h/db"
        );
        assert!(orphan.password.is_none());

        // A query-parameter password round-trips, and a redacted one the
        // stored spec lacks is dropped with its separator.
        let stored_q = spec(serde_json::json!({
            "name": "p", "type": "postgresql",
            "connection_url": "postgresql://bob:ui-secret-7@db/sales?sslmode=disable&password=q-secret-6"
        }));
        let mut back: CatalogConfig = serde_json::from_value(redacted_spec(&stored_q)).unwrap();
        fill_redacted(&mut back, &stored_q);
        assert_eq!(
            back.connection_url.as_ref().unwrap().0,
            stored_q.connection_url.as_ref().unwrap().0
        );
        for (sent, kept) in [
            (
                "postgresql://bob@h/db?password=<redacted>",
                "postgresql://bob@h/db",
            ),
            (
                "postgresql://bob@h/db?password=<redacted>&sslmode=disable",
                "postgresql://bob@h/db?sslmode=disable",
            ),
            (
                "postgresql://bob@h/db?sslmode=disable&password=<redacted>",
                "postgresql://bob@h/db?sslmode=disable",
            ),
        ] {
            let mut new = spec(serde_json::json!({
                "name": "p", "type": "postgresql", "connection_url": sent
            }));
            fill_redacted(&mut new, &bare);
            assert_eq!(new.connection_url.as_ref().unwrap().0, kept);
        }
    }

    #[tokio::test]
    async fn apply_set_ignores_an_older_generation() {
        let (live, manager, _) = live();
        live.install(hive("lake"), fake_built());
        live.set_generation(5);
        for older in [5, 4, 0] {
            let report = live.apply_set(older, vec![]).await;
            assert_eq!(report, ApplyReport::default());
        }
        assert_eq!(live.generation(), 5);
        assert!(manager.catalog("lake").is_some(), "nothing was removed");
        assert_eq!(live.specs().len(), 1);
    }

    #[tokio::test]
    async fn apply_set_removes_catalogs_not_in_the_set() {
        let (live, manager, connectors) = live();
        live.install(hive("stale"), fake_built());
        live.set_generation(1);
        let pg = spec(serde_json::json!({
            "name": "pg",
            "type": "postgresql",
            "connection_url": format!("postgresql://u@127.0.0.1:{}/db", closed_port()),
            "connect_timeout_secs": 2
        }));
        let report = live.apply_set(2, vec![pg]).await;
        assert!(report.applied);
        assert_eq!(report.failed.len(), 1, "{report:?}");
        assert_eq!(report.failed[0].0, "pg");
        assert_eq!(live.generation(), 2, "the generation still advances");
        assert!(manager.catalog("stale").is_none());
        assert!(connectors.get("stale").is_none());
        assert!(manager.catalog("pg").is_none());
        assert!(live.specs().is_empty());
        assert!(!live.remove("stale"), "already gone");
    }

    #[tokio::test]
    async fn apply_set_keeps_an_unchanged_catalog_without_rebuilding() {
        let (live, manager, _) = live();
        // Its metastore does not exist, so a rebuild would fail.
        live.install(hive("lake"), fake_built());
        let report = live.apply_set(1, vec![hive("lake")]).await;
        assert_eq!(
            report,
            ApplyReport {
                applied: true,
                failed: vec![]
            }
        );
        assert!(manager.catalog("lake").is_some());
    }

    #[tokio::test]
    async fn build_refuses_an_unsupported_type_and_a_hive_spec_without_uri() {
        let (live, _, _) = live();
        let mysql = spec(serde_json::json!({"name": "m", "type": "mysql"}));
        assert!(matches!(
            live.build(&mysql).await,
            Err(CatalogBuildError::Invalid(_))
        ));
        let no_uri = spec(serde_json::json!({"name": "h", "type": "hive"}));
        assert!(matches!(
            live.build(&no_uri).await,
            Err(CatalogBuildError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn install_and_remove_touch_only_managed_catalogs() {
        let (live, manager, connectors) = live();
        manager.register_catalog("memory", Arc::new(MemoryCatalog::new()));
        assert!(!live.remove("memory"));
        assert!(manager.catalog("memory").is_some());
        live.install(hive("lake"), fake_built());
        assert!(connectors.get("lake").is_some());
        assert!(live.remove("lake"));
        assert!(manager.catalog("lake").is_none());
        assert!(connectors.get("lake").is_none());
    }
}
