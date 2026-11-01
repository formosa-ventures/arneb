//! The external catalogs (`hive`, `postgresql`) a node serves, kept live so
//! they can change at runtime (runtime-config D6, D8). The `memory` and
//! `file` catalogs are not managed here: they are startup-only.
//!
//! [`LiveCatalogs`] builds a catalog's provider and connector from its spec,
//! installs both into the shared [`CatalogManager`] and [`ConnectorRegistry`],
//! and remembers the installed specs plus the catalog generation they belong
//! to. A statement pins the providers and connectors together when it
//! starts ([`ConnectorRegistry::pinned`]; every install and remove here is
//! one [`ConnectorRegistry::swap`]), so a change never disturbs a running
//! statement on the coordinator. A worker resolves each task's connectors
//! when the task starts.

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
pub const REDACTED: &str = "<redacted>";

/// Catalog names a spec may not take: the startup-only catalogs and the
/// metadata namespaces.
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
    /// On a coordinator, its own epoch (random per start); on a worker, the
    /// epoch of the coordinator its set came from (0 before the first).
    /// Ruling R6.
    epoch: AtomicU64,
    /// Catalogs of the applied set that failed to build (Ruling R8): a set
    /// with the same epoch and generation rebuilds them instead of being
    /// rejected as stale.
    failed: RwLock<std::collections::BTreeSet<String>>,
    /// On a worker, the coordinator's default catalog and schema from the
    /// last applied set (Ruling R12); `None` until a set that carries them
    /// arrives, and on a coordinator.
    pushed_defaults: RwLock<Option<(String, String)>>,
    /// The coordinator serves its set over Flight `get_catalogs` without a
    /// `[cluster] secret`, so no served spec may carry a secret
    /// ([`exposure_check`](Self::exposure_check)).
    unauthenticated_flight: std::sync::atomic::AtomicBool,
    /// Serializes the swap step of [`apply_set`](Self::apply_set) calls.
    /// Builds run before it is taken, so an unreachable metastore never
    /// holds up another push or pull.
    applying: tokio::sync::Mutex<()>,
    /// Tests replace [`build`](Self::build) with this.
    #[cfg(test)]
    test_builder: Option<TestBuilder>,
    /// How long a test build of a catalog takes, by name.
    #[cfg(test)]
    test_build_delay: std::sync::Mutex<BTreeMap<String, Duration>>,
}

#[cfg(test)]
pub(crate) type TestBuilder =
    Box<dyn Fn(&CatalogConfig) -> Result<BuiltCatalog, CatalogBuildError> + Send + Sync>;

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
            epoch: AtomicU64::new(0),
            failed: RwLock::new(Default::default()),
            pushed_defaults: RwLock::new(None),
            unauthenticated_flight: std::sync::atomic::AtomicBool::new(false),
            applying: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            test_builder: None,
            #[cfg(test)]
            test_build_delay: Default::default(),
        })
    }

    /// [`new`](Self::new) whose [`build`](Self::build) is `builder`.
    #[cfg(test)]
    pub(crate) fn with_test_builder(
        manager: Arc<CatalogManager>,
        connectors: Arc<ConnectorRegistry>,
        builder: TestBuilder,
    ) -> Arc<Self> {
        Self::with_test_builder_and_storage(manager, connectors, StorageConfig::default(), builder)
    }

    /// [`with_test_builder`](Self::with_test_builder) over `global_storage`.
    #[cfg(test)]
    pub(crate) fn with_test_builder_and_storage(
        manager: Arc<CatalogManager>,
        connectors: Arc<ConnectorRegistry>,
        global_storage: StorageConfig,
        builder: TestBuilder,
    ) -> Arc<Self> {
        let mut live =
            Arc::into_inner(Self::new(manager, connectors, global_storage)).expect("fresh Arc");
        live.test_builder = Some(builder);
        Arc::new(live)
    }

    /// Whether a catalog of the applied set failed to build (Ruling R8).
    pub fn has_failed_builds(&self) -> bool {
        !self
            .failed
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
    }

    /// The catalog generation the installed set belongs to.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// The coordinator epoch: this coordinator's own, or on a worker the one
    /// its set came from (Ruling R6).
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Sets this coordinator's epoch, once at startup before it serves.
    pub fn set_epoch(&self, epoch: u64) {
        self.epoch.store(epoch, Ordering::SeqCst);
    }

    /// Sets the catalog generation (after a change was persisted).
    pub fn set_generation(&self, g: u64) {
        self.generation.store(g, Ordering::SeqCst);
    }

    /// The default catalog's name (unqualified table names resolve in it).
    pub fn default_catalog(&self) -> String {
        self.manager.default_catalog().to_owned()
    }

    /// The installed specs, in name order (raw secrets included).
    pub fn specs(&self) -> Vec<CatalogConfig> {
        self.read_specs().values().cloned().collect()
    }

    /// The default catalog and schema a scan that names none resolves in:
    /// on a worker the coordinator's, once a set carrying them was applied
    /// (Ruling R12); otherwise this node's own.
    pub fn scan_defaults(&self) -> (String, String) {
        self.pushed_defaults
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .unwrap_or_else(|| {
                (
                    self.manager.default_catalog().to_owned(),
                    self.manager.default_schema().to_owned(),
                )
            })
    }

    /// Whether a set carrying the coordinator's defaults was applied here.
    pub fn has_pushed_defaults(&self) -> bool {
        self.pushed_defaults
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
    }

    /// The storage a hive catalog of `spec` reads through: its own
    /// `storage` merged over this node's global `[storage]`.
    fn storage_for(&self, spec: &CatalogConfig) -> StorageConfig {
        StorageConfig::merge(&self.global_storage, spec.storage.as_ref())
    }

    /// `spec` as a worker must build it (Ruling R12): a hive spec carries
    /// the global `[storage]` merged in, so the worker needs none of its
    /// own. Other types read no object storage and are unchanged.
    fn effective_spec(&self, spec: &CatalogConfig) -> CatalogConfig {
        let mut effective = spec.clone();
        if spec.catalog_type == "hive" {
            let storage = self.storage_for(spec);
            let empty = storage.s3.is_none() && storage.gcs.is_none() && storage.azure.is_none();
            effective.storage = (!empty).then_some(storage);
        }
        effective
    }

    /// Marks this coordinator as serving its set over Flight without a
    /// `[cluster] secret`: from then on [`exposure_check`](Self::exposure_check)
    /// refuses a spec that would serve a secret.
    pub fn set_unauthenticated_flight(&self, on: bool) {
        self.unauthenticated_flight.store(on, Ordering::SeqCst);
    }

    /// The secrets the served set carries, as `catalog 'x': field` (the
    /// effective specs: a hive spec with the global `[storage]` keys merged
    /// in, Ruling R12).
    pub fn served_secrets(&self) -> Vec<String> {
        self.specs()
            .iter()
            .flat_map(|spec| {
                secret_fields(&self.effective_spec(spec))
                    .into_iter()
                    .map(|field| format!("catalog '{}': {field}", spec.name))
            })
            .collect()
    }

    /// Whether `spec` may be served: always with a `[cluster] secret`;
    /// without one only when its effective spec carries no secret (Flight
    /// `get_catalogs` would hand it to any peer). `Err` names the secret
    /// fields.
    pub fn exposure_check(&self, spec: &CatalogConfig) -> Result<(), Vec<&'static str>> {
        if !self.unauthenticated_flight.load(Ordering::SeqCst) {
            return Ok(());
        }
        let fields = secret_fields(&self.effective_spec(spec));
        if fields.is_empty() {
            Ok(())
        } else {
            Err(fields)
        }
    }

    /// The generation, the installed specs (as effective specs, Ruling
    /// R12) and this node's default catalog and schema, for a worker to
    /// apply. The generation is read first: [`apply_set`](Self::apply_set)
    /// advances it only after installing, so the specs are never older than
    /// it says.
    pub fn snapshot(&self) -> arneb_rpc::CatalogSet {
        let epoch = self.epoch();
        let generation = self.generation();
        let catalogs = self
            .specs()
            .iter()
            .filter_map(|spec| serde_json::to_value(self.effective_spec(spec)).ok())
            .collect();
        arneb_rpc::CatalogSet {
            epoch,
            generation,
            catalogs,
            default_catalog: Some(self.manager.default_catalog().to_owned()),
            default_schema: Some(self.manager.default_schema().to_owned()),
        }
    }

    /// Applies a set from the coordinator (a push or a pull). Within the
    /// epoch held, only a newer generation applies, so a stale push loses;
    /// a set from another epoch (a restarted coordinator) always applies
    /// and replaces the whole set, even at a lower generation (Ruling R6).
    /// A spec that does not decode is skipped with a WARN and listed as
    /// failed.
    pub async fn apply_remote(&self, set: arneb_rpc::CatalogSet) -> ApplyReport {
        let mut specs = Vec::with_capacity(set.catalogs.len());
        let mut undecodable = Vec::new();
        for value in set.catalogs {
            let name = value
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("<unnamed>")
                .to_owned();
            match serde_json::from_value::<CatalogConfig>(value) {
                Ok(spec) => specs.push(spec),
                Err(e) => {
                    // Only the error's category: its text may quote a value.
                    tracing::warn!(
                        catalog = %name,
                        error = ?e.classify(),
                        "catalog definition from the coordinator is invalid, skipping"
                    );
                    undecodable.push((name, "invalid catalog definition".to_owned()));
                }
            }
        }
        let mut report = self.apply(Some(set.epoch), set.generation, specs).await;
        if report.applied {
            report.failed.extend(undecodable);
            if let (Some(catalog), Some(schema)) = (set.default_catalog, set.default_schema) {
                *self
                    .pushed_defaults
                    .write()
                    .unwrap_or_else(|p| p.into_inner()) = Some((catalog, schema));
            }
        }
        report
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
        #[cfg(test)]
        if let Some(builder) = &self.test_builder {
            let delay = self
                .test_build_delay
                .lock()
                .unwrap()
                .get(&spec.name)
                .copied();
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            return builder(spec);
        }
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
        let merged_storage = self.storage_for(spec);
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
        // One swap: a statement pins both halves or neither.
        self.connectors.swap(|| {
            self.connectors.register(name.clone(), built.connector);
            self.manager.register_catalog(name.clone(), built.provider);
        });
        self.failed
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&name);
        self.write_specs().insert(name, spec);
    }

    /// Removes the installed catalog `name`. False when no such catalog is
    /// installed here (the startup-only catalogs are never touched).
    pub fn remove(&self, name: &str) -> bool {
        if self.write_specs().remove(name).is_none() {
            return false;
        }
        self.connectors.swap(|| {
            self.manager.deregister_catalog(name);
            self.connectors.deregister(name);
        });
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
        self.apply(None, generation, specs).await
    }

    /// [`apply_set`](Self::apply_set), except that a set from `epoch` other
    /// than the one held applies whatever its generation, and then records
    /// `epoch`. `None` checks the generation only and keeps the epoch.
    ///
    /// The new and changed catalogs are built in parallel without any
    /// lock; `applying` is held only to re-check the generation and swap.
    /// A set overtaken by a newer one while it was building changes
    /// nothing. When another apply swapped in between, a catalog this set
    /// skipped as unchanged may now differ: it is built (again outside the
    /// lock) before the swap.
    async fn apply(
        &self,
        epoch: Option<u64>,
        generation: u64,
        specs: Vec<CatalogConfig>,
    ) -> ApplyReport {
        if !self.applies(epoch, generation) {
            return ApplyReport::default();
        }
        let wanted: std::collections::BTreeSet<String> =
            specs.iter().map(|s| s.name.clone()).collect();
        let mut built: BTreeMap<String, (CatalogConfig, BuiltCatalog)> = BTreeMap::new();
        let mut failed: BTreeMap<String, String> = BTreeMap::new();
        let mut installed = self.read_specs().clone();
        loop {
            let todo: Vec<&CatalogConfig> = specs
                .iter()
                .filter(|spec| {
                    !built.contains_key(&spec.name)
                        && !failed.contains_key(&spec.name)
                        && !installed
                            .get(&spec.name)
                            .is_some_and(|current| same_spec(current, spec))
                })
                .collect();
            let results = futures::future::join_all(
                todo.into_iter()
                    .map(|spec| async move { (spec, self.build(spec).await) }),
            )
            .await;
            for (spec, result) in results {
                match result {
                    Ok(b) => {
                        built.insert(spec.name.clone(), (spec.clone(), b));
                    }
                    Err(e) => {
                        tracing::warn!(
                            catalog = %spec.name,
                            catalog_type = %spec.catalog_type,
                            error = %e,
                            "failed to build catalog, skipping"
                        );
                        failed.insert(spec.name.clone(), e.to_string());
                    }
                }
            }
            let _applying = self.applying.lock().await;
            // A newer set may have been applied while this one was building.
            if !self.applies(epoch, generation) {
                return ApplyReport::default();
            }
            let now = self.read_specs().clone();
            let stale = specs.iter().any(|spec| {
                !built.contains_key(&spec.name)
                    && !failed.contains_key(&spec.name)
                    && !now
                        .get(&spec.name)
                        .is_some_and(|current| same_spec(current, spec))
            });
            if stale {
                // Another apply swapped in between: build what now differs.
                installed = now;
                continue;
            }
            for (_, (spec, b)) in std::mem::take(&mut built) {
                tracing::info!(
                    catalog = %spec.name,
                    catalog_type = %spec.catalog_type,
                    generation,
                    "registered catalog"
                );
                self.install(spec, b);
            }
            for name in now.keys().filter(|n| !wanted.contains(*n)) {
                self.remove(name);
                tracing::info!(catalog = %name, generation, "removed catalog");
            }
            *self.failed.write().unwrap_or_else(|p| p.into_inner()) =
                failed.keys().cloned().collect();
            // Generation before epoch: a reader seeing the new epoch also sees
            // the generation that goes with it.
            self.set_generation(generation);
            if let Some(e) = epoch {
                self.set_epoch(e);
            }
            return ApplyReport {
                applied: true,
                failed: failed.into_iter().collect(),
            };
        }
    }

    /// Whether a set at `(epoch, generation)` applies now: a newer
    /// generation within the epoch held, any set from another epoch, or the
    /// set already held while some of it failed to build (Ruling R8).
    fn applies(&self, epoch: Option<u64>, generation: u64) -> bool {
        let same_epoch = epoch.is_none_or(|e| e == self.epoch());
        let retry = same_epoch && generation == self.generation() && self.has_failed_builds();
        !same_epoch || generation > self.generation() || retry
    }
}

/// How a worker pulls its catalogs from the coordinator (runtime-config
/// D8). Cloneable; the heartbeat loop and the task manager share one, and
/// concurrent [`pull`](Self::pull)s share a single fetch.
#[derive(Clone)]
pub struct CatalogPuller {
    inner: Arc<PullerInner>,
}

type PullFuture = futures::future::Shared<futures::future::BoxFuture<'static, Result<(), String>>>;

struct PullerInner {
    live: Arc<LiveCatalogs>,
    pull: Box<dyn Fn() -> futures::future::BoxFuture<'static, Result<(), String>> + Send + Sync>,
    in_flight: std::sync::Mutex<Option<PullFuture>>,
    /// When the last fetch started; rate-limits the heartbeat's retry of
    /// failed builds (Ruling R8).
    last_pull: std::sync::Mutex<Option<tokio::time::Instant>>,
}

/// The least time between two heartbeat-triggered retries of catalogs that
/// failed to build (Ruling R8).
pub const FAILED_BUILD_RETRY: Duration = Duration::from_secs(30);

impl CatalogPuller {
    /// A puller whose fetch-and-apply is `pull`; `live` is the set whose
    /// generation a heartbeat reply is compared with.
    pub fn new(
        live: Arc<LiveCatalogs>,
        pull: impl Fn() -> futures::future::BoxFuture<'static, Result<(), String>>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(PullerInner {
                live,
                pull: Box::new(pull),
                in_flight: std::sync::Mutex::new(None),
                last_pull: std::sync::Mutex::new(None),
            }),
        }
    }

    /// Pulls with `get_catalogs` from `coordinator` (under the installed
    /// cluster security) and applies the set to `live`.
    pub fn from_coordinator(coordinator: String, live: Arc<LiveCatalogs>) -> Self {
        let target = live.clone();
        Self::new(live, move || {
            let (coordinator, live) = (coordinator.clone(), target.clone());
            Box::pin(async move {
                let set =
                    arneb_rpc::fetch_catalogs(&coordinator, arneb_rpc::cluster_security()).await?;
                let generation = set.generation;
                let report = live.apply_remote(set).await;
                if report.applied {
                    tracing::info!(
                        target: "arneb::cluster",
                        generation,
                        catalogs = live.specs().len(),
                        failed = report.failed.len(),
                        "catalogs pulled from the coordinator"
                    );
                }
                Ok(())
            })
        })
    }

    /// The live set this puller applies to.
    pub fn live(&self) -> &Arc<LiveCatalogs> {
        &self.inner.live
    }

    /// Fetches the coordinator's set and applies it. A pull already running
    /// is joined instead of starting another.
    pub async fn pull(&self) -> Result<(), String> {
        self.pull_joined().await.0
    }

    /// [`pull`](Self::pull) for a task missing catalog `found` checks: when
    /// it joined a pull that was already running (whose fetch may predate
    /// the catalog) and the catalog is still missing, it starts one fresh
    /// pull.
    pub async fn pull_until(&self, found: impl Fn() -> bool) -> Result<(), String> {
        let (result, joined) = self.pull_joined().await;
        if joined && !found() {
            return self.pull_joined().await.0;
        }
        result
    }

    /// The pull's result, and whether it joined one already running.
    async fn pull_joined(&self) -> (Result<(), String>, bool) {
        use futures::FutureExt;
        let (fut, joined) = {
            let mut slot = self
                .inner
                .in_flight
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            match slot.as_ref() {
                Some(f) => (f.clone(), true),
                None => {
                    *self
                        .inner
                        .last_pull
                        .lock()
                        .unwrap_or_else(|p| p.into_inner()) = Some(tokio::time::Instant::now());
                    let f = (self.inner.pull)().shared();
                    *slot = Some(f.clone());
                    (f, false)
                }
            }
        };
        let result = fut.clone().await;
        let mut slot = self
            .inner
            .in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if slot.as_ref().is_some_and(|f| f.ptr_eq(&fut)) {
            *slot = None;
        }
        (result, joined)
    }

    /// After a heartbeat: pulls when the reply names another catalog
    /// generation or coordinator epoch than the applied ones (Ruling R6),
    /// or, while catalogs of the applied set failed to build, at most once
    /// per [`FAILED_BUILD_RETRY`] since the last fetch (Ruling R8). True
    /// when it pulled.
    pub async fn after_heartbeat(&self, reply: &arneb_rpc::HeartbeatReply) -> bool {
        let Some(generation) = reply.catalog_generation else {
            return false;
        };
        let live = &self.inner.live;
        let (applied, applied_epoch) = (live.generation(), live.epoch());
        let epoch_differs = reply.catalog_epoch.is_some_and(|e| e != applied_epoch);
        if generation == applied && !epoch_differs {
            if !live.has_failed_builds() || !self.retry_due() {
                return false;
            }
            tracing::debug!(
                target: "arneb::cluster",
                "retrying catalogs that failed to build"
            );
            if let Err(e) = self.pull().await {
                tracing::warn!(target: "arneb::cluster", error = %e, "catalog pull failed");
            }
            return true;
        }
        tracing::debug!(
            target: "arneb::cluster",
            coordinator_generation = generation,
            applied_generation = applied,
            epoch_differs,
            "catalog generation or epoch differs from the coordinator's, pulling"
        );
        if let Err(e) = self.pull().await {
            tracing::warn!(target: "arneb::cluster", error = %e, "catalog pull failed");
        }
        true
    }
}

impl CatalogPuller {
    fn retry_due(&self) -> bool {
        self.inner
            .last_pull
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_none_or(|at| at.elapsed() >= FAILED_BUILD_RETRY)
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

/// Whether `a` and `b` serialize the same (raw secrets included).
pub(crate) fn same_spec(a: &CatalogConfig, b: &CatalogConfig) -> bool {
    match (serde_json::to_value(a), serde_json::to_value(b)) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// Checks a runtime catalog spec (D6): the name is 1–63 characters of
/// `[a-z0-9_]` starting with a letter and not reserved; the type is `hive`
/// (with `metastore_uri`) or `postgresql` (with a valid `connection_url`
/// and settings). The message never carries a secret. Only a create checks
/// the name: an update or drop of an existing catalog uses
/// [`validate_spec_fields`], since config-seeded catalogs (such as
/// `pg-prod`) were never held to the name rule.
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
    validate_spec_fields(spec)
}

/// [`validate_spec`] without the name rule: the type and its settings, for
/// an update of a catalog that already exists under its name.
pub fn validate_spec_fields(spec: &CatalogConfig) -> Result<(), String> {
    let name = spec.name.as_str();
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

/// The secret fields of `spec` that are exactly `"<redacted>"` (the
/// placeholder of [`redacted_spec`]), by name. A spec that still has one
/// after [`fill_redacted`] would store and push the placeholder as a
/// real secret.
pub fn redacted_secrets(spec: &CatalogConfig) -> Vec<&'static str> {
    let is_redacted = |v: &Option<String>| v.as_deref() == Some(REDACTED);
    let mut found = Vec::new();
    if spec.password.as_ref().is_some_and(|p| p.0 == REDACTED) {
        found.push("password");
    }
    if let Some(url) = &spec.connection_url {
        let userinfo = url_password_range(&url.0).is_some_and(|r| &url.0[r] == REDACTED);
        let query = url_query_passwords(&url.0)
            .into_iter()
            .any(|(_, value)| &url.0[value] == REDACTED);
        if userinfo || query {
            found.push("connection_url password");
        }
    }
    if let Some(storage) = &spec.storage {
        if storage
            .s3
            .as_ref()
            .is_some_and(|s3| is_redacted(&s3.secret_access_key))
        {
            found.push("storage.s3.secret_access_key");
        }
        if storage
            .gcs
            .as_ref()
            .is_some_and(|gcs| is_redacted(&gcs.service_account_path))
        {
            found.push("storage.gcs.service_account_path");
        }
        if storage
            .azure
            .as_ref()
            .is_some_and(|azure| is_redacted(&azure.access_key))
        {
            found.push("storage.azure.access_key");
        }
    }
    found
}

/// The secrets `spec` carries, by field: `password`, a password in
/// `connection_url` (userinfo or query parameter), `storage.s3.secret_access_key`
/// and `storage.azure.access_key`. A GCS `service_account_path` is a path on
/// the node, not the key itself, so it is not listed.
pub fn secret_fields(spec: &CatalogConfig) -> Vec<&'static str> {
    let mut found = Vec::new();
    if spec.password.is_some() {
        found.push("password");
    }
    if let Some(url) = &spec.connection_url {
        if url_password_range(&url.0).is_some() || !url_query_passwords(&url.0).is_empty() {
            found.push("connection_url password");
        }
    }
    if let Some(storage) = &spec.storage {
        if storage
            .s3
            .as_ref()
            .is_some_and(|s3| s3.secret_access_key.is_some())
        {
            found.push("storage.s3.secret_access_key");
        }
        if storage
            .azure
            .as_ref()
            .is_some_and(|azure| azure.access_key.is_some())
        {
            found.push("storage.azure.access_key");
        }
    }
    found
}

/// Whether `a` and `b` send their secrets to the same places (Ruling R10):
/// the same `connection_url` apart from its passwords (host, port,
/// database, user, parameters), `metastore_uri`, S3 `endpoint` and
/// `region`, and Azure `storage_account`.
pub fn same_destination(a: &CatalogConfig, b: &CatalogConfig) -> bool {
    fn destinations(spec: &CatalogConfig) -> serde_json::Value {
        let redacted = redacted_spec(spec);
        let storage = spec.storage.as_ref();
        let s3 = storage.and_then(|s| s.s3.as_ref());
        serde_json::json!({
            "connection_url": redacted["connection_url"],
            "metastore_uri": spec.metastore_uri,
            "s3_endpoint": s3.and_then(|s| s.endpoint.as_deref()),
            "s3_region": s3.and_then(|s| s.region.as_deref()),
            "azure_account": storage
                .and_then(|s| s.azure.as_ref())
                .and_then(|a| a.storage_account.as_deref()),
        })
    }
    destinations(a) == destinations(b)
}

/// Where the credentials of `scheme://user:password@host/...` end, found
/// the way tokio-postgres finds them: everything after the scheme up to
/// the first `@` anywhere is the userinfo, so an unencoded `/`, `?` or `#`
/// in the password still belongs to it. `(after_scheme, at)`, with `at`
/// `None` when the URL has no `@` (no credentials).
fn url_userinfo(url: &str) -> Option<(usize, Option<usize>)> {
    let after_scheme = url.find("://")? + 3;
    let at = url[after_scheme..].find('@').map(|i| after_scheme + i);
    Some((after_scheme, at))
}

/// The byte range of the password in `scheme://user:password@host/...`,
/// if the URL's userinfo has one (the text after the userinfo's first `:`,
/// as tokio-postgres splits it).
fn url_password_range(url: &str) -> Option<std::ops::Range<usize>> {
    let (start, at) = url_userinfo(url)?;
    let at = at?;
    let colon = url[start..at].find(':')?;
    Some(start + colon + 1..at)
}

/// Whether a query parameter key is `password` once percent-decoded:
/// tokio-postgres decodes keys, so `pass%77ord=` sets the password too.
fn is_password_key(key: &str) -> bool {
    let bytes = key.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                decoded.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        decoded.push(bytes[i]);
        i += 1;
    }
    decoded == b"password"
}

/// `(parameter, value)` byte ranges of every query parameter whose key is
/// `password` once percent-decoded (libpq URIs accept the password there
/// too).
fn url_query_passwords(url: &str) -> Vec<(std::ops::Range<usize>, std::ops::Range<usize>)> {
    // As in tokio-postgres: the query starts at the first `?` after the
    // userinfo (a `?` inside the password is not one), and runs to the end
    // (a `#` is part of a value, not a fragment).
    let Some((start, at)) = url_userinfo(url) else {
        return Vec::new();
    };
    let host = at.map_or(start, |at| at + 1);
    let Some(q) = url[host..].find('?').map(|i| host + i + 1) else {
        return Vec::new();
    };
    let end = url.len();
    let mut out = Vec::new();
    let mut start = q;
    for param in url[q..end].split('&') {
        if let Some((key, value)) = param.split_once('=') {
            if is_password_key(key) {
                let value_start = start + param.len() - value.len();
                out.push((start..start + param.len(), value_start..start + param.len()));
            }
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

    /// Concurrent pulls share one fetch; a later pull fetches again.
    #[tokio::test]
    async fn concurrent_pulls_share_one_fetch() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (live, _, _) = live();
        let fetches = Arc::new(AtomicUsize::new(0));
        let (n, gate) = (fetches.clone(), Arc::new(tokio::sync::Notify::new()));
        let g = gate.clone();
        let puller = CatalogPuller::new(live, move || {
            n.fetch_add(1, Ordering::SeqCst);
            let g = g.clone();
            Box::pin(async move {
                g.notified().await;
                Ok(())
            })
        });
        let (a, b) = (puller.clone(), puller.clone());
        let ta = tokio::spawn(async move { a.pull().await });
        let tb = tokio::spawn(async move { b.pull().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        gate.notify_one();
        ta.await.unwrap().unwrap();
        tb.await.unwrap().unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        gate.notify_one();
        puller.pull().await.unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    /// A set from the coordinator: an undecodable spec is skipped and
    /// reported, the generation advances, and a stale set changes nothing.
    #[tokio::test]
    async fn apply_remote_skips_undecodable_specs() {
        let (live, _, _) = live();
        let set = arneb_rpc::CatalogSet {
            epoch: 0,
            generation: 4,
            catalogs: vec![serde_json::json!({"name": "bad", "type": 7})],
            ..Default::default()
        };
        let report = live.apply_remote(set.clone()).await;
        assert!(report.applied);
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.failed[0].0, "bad");
        assert_eq!(live.generation(), 4);
        assert!(!live.apply_remote(set).await.applied, "not newer");
        let snap = live.snapshot();
        assert_eq!((snap.generation, snap.catalogs.len()), (4, 0));
    }

    /// Ruling R12: the coordinator pushes each hive spec with its global
    /// `[storage]` merged in, plus its defaults; a worker with no storage
    /// of its own builds that spec with the coordinator's storage, and the
    /// pushed secret stays out of `Debug`.
    #[tokio::test]
    async fn pushed_hive_spec_builds_with_the_coordinators_storage() {
        let coordinator_storage: StorageConfig = toml::from_str(
            r#"
            [s3]
            endpoint = "http://rustfs:9000"
            region = "us-east-1"
            access_key_id = "coordkey"
            secret_access_key = "coordsecret"
            "#,
        )
        .unwrap();
        let coordinator = LiveCatalogs::with_test_builder_and_storage(
            Arc::new(CatalogManager::new("lake", "tpch")),
            Arc::new(ConnectorRegistry::new()),
            coordinator_storage,
            Box::new(|_| Ok(fake_built())),
        );
        coordinator.set_epoch(5);
        coordinator.set_generation(2);
        coordinator.install(hive("lake"), fake_built());
        coordinator.install(
            spec(serde_json::json!({"name": "pg", "type": "postgresql", "connection_url": "postgresql://db/x"})),
            fake_built(),
        );
        let set = coordinator.snapshot();
        assert_eq!(set.default_catalog.as_deref(), Some("lake"));
        assert_eq!(set.default_schema.as_deref(), Some("tpch"));
        assert!(!format!("{set:?}").contains("coordsecret"));
        // Only the hive spec carries storage.
        assert!(set
            .catalogs
            .iter()
            .any(|c| c["name"] == "pg" && c["storage"].is_null()));

        let seen: Arc<std::sync::Mutex<Vec<CatalogConfig>>> = Default::default();
        let s = seen.clone();
        let worker = LiveCatalogs::with_test_builder_and_storage(
            Arc::new(CatalogManager::new("memory", "default")),
            Arc::new(ConnectorRegistry::new()),
            StorageConfig::default(),
            Box::new(move |spec| {
                s.lock().unwrap().push(spec.clone());
                Ok(fake_built())
            }),
        );
        let report = worker.apply_remote(set).await;
        assert!(report.applied && report.failed.is_empty(), "{report:?}");
        let lake = seen
            .lock()
            .unwrap()
            .iter()
            .find(|c| c.name == "lake")
            .cloned()
            .unwrap();
        let s3 = worker.storage_for(&lake).s3.expect("coordinator's s3");
        assert_eq!(s3.endpoint.as_deref(), Some("http://rustfs:9000"));
        assert_eq!(s3.secret_access_key.as_deref(), Some("coordsecret"));
        assert_eq!(
            worker.scan_defaults(),
            ("lake".to_owned(), "tpch".to_owned())
        );
    }

    fn fake_built() -> BuiltCatalog {
        let catalog = Arc::new(MemoryCatalog::new());
        BuiltCatalog {
            provider: catalog.clone(),
            connector: Arc::new(MemoryConnectorFactory::new(catalog, "default")),
        }
    }

    fn set(epoch: u64, generation: u64, catalogs: Vec<serde_json::Value>) -> arneb_rpc::CatalogSet {
        arneb_rpc::CatalogSet {
            epoch,
            generation,
            catalogs,
            ..Default::default()
        }
    }

    /// Ruling R6: within one coordinator epoch only a newer generation
    /// applies (a stale push loses); a set from another epoch replaces the
    /// whole set, even at a lower or equal generation.
    #[tokio::test]
    async fn apply_remote_follows_the_coordinator_epoch() {
        let (live, _, connectors) = live();
        live.install(hive("old"), fake_built());
        assert!(
            live.apply_remote(set(11, 5, vec![serde_json::to_value(hive("old")).unwrap()]))
                .await
                .applied
        );
        assert_eq!((live.epoch(), live.generation()), (11, 5));
        // Same epoch, older generation: ignored.
        assert!(!live.apply_remote(set(11, 3, vec![])).await.applied);
        assert_eq!(live.specs().len(), 1);
        // A restarted coordinator (new epoch) with a lower generation wins.
        let report = live.apply_remote(set(22, 2, vec![])).await;
        assert!(report.applied);
        assert_eq!((live.epoch(), live.generation()), (22, 2));
        assert!(live.specs().is_empty());
        assert!(connectors.get("old").is_none());
        // A no-store coordinator restarted with a changed config: new epoch,
        // generation 1 again; the changed set is applied.
        let mysql = serde_json::json!({"name": "changed", "type": "mysql"});
        let report = live.apply_remote(set(33, 2, vec![mysql])).await;
        assert!(report.applied);
        assert_eq!(
            report.failed.len(),
            1,
            "the new spec was built (and failed)"
        );
        assert_eq!((live.epoch(), live.generation()), (33, 2));
        // Same epoch, older generation: stale even with failed builds.
        assert!(!live.apply_remote(set(33, 1, vec![])).await.applied);
        // Ruling R8: the same (epoch, generation) retries the failed build.
        let mysql = serde_json::json!({"name": "changed", "type": "mysql"});
        let report = live.apply_remote(set(33, 2, vec![mysql])).await;
        assert!(report.applied, "retry while a build failed");
        assert_eq!(report.failed.len(), 1);
        // Once nothing failed, the same (epoch, generation) is stale.
        assert!(live.apply_remote(set(33, 2, vec![])).await.applied, "retry");
        assert!(!live.has_failed_builds());
        assert!(!live.apply_remote(set(33, 2, vec![])).await.applied);
        // The coordinator's own path (no epoch) keeps the epoch.
        assert!(live.apply_set(3, vec![]).await.applied);
        assert_eq!(live.epoch(), 33);
    }

    /// Ruling R8: while a catalog of the applied set failed to build, a
    /// matching heartbeat retries the pull at most once per 30 s since the
    /// last fetch; once nothing failed, it stops.
    #[tokio::test(start_paused = true)]
    async fn heartbeat_retries_failed_builds_at_most_every_30s() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let reachable = Arc::new(AtomicBool::new(false));
        let r = reachable.clone();
        let live = LiveCatalogs::with_test_builder(
            Arc::new(CatalogManager::new("memory", "default")),
            Arc::new(ConnectorRegistry::new()),
            Box::new(move |_| {
                if r.load(Ordering::SeqCst) {
                    Ok(fake_built())
                } else {
                    Err(CatalogBuildError::Unreachable("down".into()))
                }
            }),
        );
        let held = set(9, 4, vec![serde_json::to_value(hive("lake")).unwrap()]);
        let fetches = Arc::new(AtomicUsize::new(0));
        let (n, l, s) = (fetches.clone(), live.clone(), held.clone());
        let puller = CatalogPuller::new(live.clone(), move || {
            n.fetch_add(1, Ordering::SeqCst);
            let (l, s) = (l.clone(), s.clone());
            Box::pin(async move {
                l.apply_remote(s).await;
                Ok(())
            })
        });
        let reply = arneb_rpc::HeartbeatReply {
            catalog_generation: Some(4),
            catalog_epoch: Some(9),
        };
        // The first heartbeat pulls (generation differs); lake fails.
        assert!(puller.after_heartbeat(&reply).await);
        assert!(live.has_failed_builds());
        assert_eq!(live.generation(), 4, "the generation still advances");
        // In sync, but lake failed: no retry before 30 s.
        assert!(!puller.after_heartbeat(&reply).await);
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(!puller.after_heartbeat(&reply).await);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(puller.after_heartbeat(&reply).await, "retry due");
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
        assert!(live.has_failed_builds(), "still down");
        // Reachable now: the next due retry installs it, then no more.
        reachable.store(true, Ordering::SeqCst);
        tokio::time::advance(Duration::from_secs(30)).await;
        assert!(puller.after_heartbeat(&reply).await);
        assert!(!live.has_failed_builds());
        assert_eq!(live.specs().len(), 1);
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(!puller.after_heartbeat(&reply).await);
        assert_eq!(fetches.load(Ordering::SeqCst), 3);
    }

    /// A coordinator that accepts connections and never answers: the pull
    /// fails within the 10 s fetch timeout instead of hanging, so later
    /// pulls (which would join it) do not hang either.
    #[tokio::test(start_paused = true)]
    async fn a_black_holed_coordinator_fails_the_pull_in_time() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        let (live, _, _) = live();
        let puller = CatalogPuller::from_coordinator(addr, live);
        let started = tokio::time::Instant::now();
        let err = puller.pull().await.unwrap_err();
        assert!(err.contains("timed out"), "{err}");
        assert!(started.elapsed() <= arneb_rpc::FETCH_CATALOGS_TIMEOUT + Duration::from_secs(1));
        let err = puller.pull().await.unwrap_err();
        assert!(err.contains("timed out"), "{err}");
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

    /// tokio-postgres percent-decodes query keys, so `pass%77ord=` is a
    /// working password: it is redacted, found as a placeholder, filled
    /// back from the stored spec, and ignored by the destination check.
    #[test]
    fn percent_encoded_password_key_is_a_secret() {
        let stored = spec(serde_json::json!({
            "name": "p", "type": "postgresql",
            "connection_url": "postgresql://bob@db/sales?sslmode=disable&pass%77ord=enc-secret-8&PASS%57ORD=x"
        }));
        let redacted = redacted_spec(&stored);
        assert!(!redacted.to_string().contains("enc-secret-8"));
        assert_eq!(
            redacted["connection_url"],
            "postgresql://bob@db/sales?sslmode=disable&pass%77ord=<redacted>&PASS%57ORD=x",
            "keys are case-sensitive, as in tokio-postgres"
        );
        let mut round_trip: CatalogConfig = serde_json::from_value(redacted).unwrap();
        assert_eq!(
            redacted_secrets(&round_trip),
            vec!["connection_url password"]
        );
        assert!(same_destination(&round_trip, &stored));
        fill_redacted(&mut round_trip, &stored);
        assert!(redacted_secrets(&round_trip).is_empty());
        assert_eq!(
            round_trip.connection_url.as_ref().unwrap().0,
            stored.connection_url.as_ref().unwrap().0
        );
        assert!(is_password_key("%70%61%73%73%77%6F%72%64"));
        assert!(!is_password_key("password%"));
        assert!(!is_password_key("passwor"));
    }

    /// tokio-postgres takes the credentials up to the first `@`, so an
    /// unencoded `/`, `?` or `#` in the password is still the password
    /// (and a `#` in a query value is part of it). The redaction finds the
    /// same password the driver connects with, and round-trips it.
    #[test]
    fn password_with_url_delimiters_is_redacted() {
        use std::str::FromStr;
        for (url, redacted) in [
            (
                "postgresql://app:pa/ss@db:5432/x",
                "postgresql://app:<redacted>@db:5432/x",
            ),
            (
                "postgresql://app:pa?ss@db:5432/x",
                "postgresql://app:<redacted>@db:5432/x",
            ),
            (
                "postgresql://app:pa#ss@db:5432/x",
                "postgresql://app:<redacted>@db:5432/x",
            ),
            (
                "postgres://app:a/b?c#d:e@db/x?sslmode=disable",
                "postgres://app:<redacted>@db/x?sslmode=disable",
            ),
            (
                "postgresql://app@db/x?password=pa#ss&sslmode=disable",
                "postgresql://app@db/x?password=<redacted>&sslmode=disable",
            ),
            (
                "postgresql://app:pa?password=no@db/x?password=q/s#t",
                "postgresql://app:<redacted>@db/x?password=<redacted>",
            ),
        ] {
            let driver = tokio_postgres::Config::from_str(url).unwrap();
            let password = String::from_utf8(driver.get_password().unwrap().to_vec()).unwrap();
            let stored = spec(serde_json::json!({
                "name": "p", "type": "postgresql", "connection_url": url
            }));
            let shown = redacted_spec(&stored);
            assert_eq!(shown["connection_url"], redacted, "{url}");
            assert!(
                !shown.to_string().contains(&password),
                "the driver's password leaked for {url}"
            );
            let mut back: CatalogConfig = serde_json::from_value(shown).unwrap();
            assert_eq!(redacted_secrets(&back), vec!["connection_url password"]);
            assert!(same_destination(&back, &stored));
            fill_redacted(&mut back, &stored);
            assert_eq!(back.connection_url.as_ref().unwrap().0, url);
        }
    }

    /// Without a cluster secret no served spec may carry a secret, the
    /// global `[storage]` keys a hive spec is served with included.
    #[test]
    fn exposure_check_refuses_secrets_over_unauthenticated_flight() {
        let manager = Arc::new(CatalogManager::new("memory", "default"));
        let storage: StorageConfig = serde_json::from_value(serde_json::json!({
            "s3": {"region": "us-east-1", "secret_access_key": "global-s3-secret"}
        }))
        .unwrap();
        let live = LiveCatalogs::with_test_builder_and_storage(
            manager,
            Arc::new(ConnectorRegistry::new()),
            storage,
            Box::new(|_| Ok(fake_built())),
        );
        let pg = spec(serde_json::json!({
            "name": "p", "type": "postgresql", "connection_url": "postgresql://u@h/db"
        }));
        let mut pg_pw = pg.clone();
        pg_pw.password = Some(RedactedString("pw".into()));
        let pg_url = spec(serde_json::json!({
            "name": "p", "type": "postgresql", "connection_url": "postgresql://u:p/w@h/db"
        }));
        // With a cluster secret everything may be served.
        for s in [&pg, &pg_pw, &pg_url, &hive("lake")] {
            assert_eq!(live.exposure_check(s), Ok(()));
        }
        live.set_unauthenticated_flight(true);
        assert_eq!(live.exposure_check(&pg), Ok(()));
        assert_eq!(live.exposure_check(&pg_pw), Err(vec!["password"]));
        assert_eq!(
            live.exposure_check(&pg_url),
            Err(vec!["connection_url password"])
        );
        assert_eq!(
            live.exposure_check(&hive("lake")),
            Err(vec!["storage.s3.secret_access_key"]),
            "the global [storage] key is served with every hive spec"
        );
        let gcs_only = spec(serde_json::json!({
            "name": "g", "type": "postgresql", "connection_url": "postgresql://u@h/db",
            "storage": {"gcs": {"service_account_path": "/keys/sa.json"}}
        }));
        assert_eq!(live.exposure_check(&gcs_only), Ok(()));

        assert!(live.served_secrets().is_empty());
        live.install(hive("lake"), fake_built());
        live.install(pg_pw, fake_built());
        assert_eq!(
            live.served_secrets(),
            vec![
                "catalog 'lake': storage.s3.secret_access_key",
                "catalog 'p': password"
            ]
        );
        assert_eq!(secret_fields(&secret_spec()).len(), 4);
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

    /// [`live`] whose builds succeed after the per-name delays in
    /// `delays`.
    fn slow_live(delays: &[(&str, u64)]) -> Arc<LiveCatalogs> {
        let live = LiveCatalogs::with_test_builder(
            Arc::new(CatalogManager::new("memory", "default")),
            Arc::new(ConnectorRegistry::new()),
            Box::new(|_| Ok(fake_built())),
        );
        let mut delay = live.test_build_delay.lock().unwrap();
        for (name, secs) in delays {
            delay.insert((*name).to_owned(), Duration::from_secs(*secs));
        }
        drop(delay);
        live
    }

    /// The new catalogs of a set build in parallel, and the apply lock is
    /// not held while they do.
    #[tokio::test(start_paused = true)]
    async fn builds_run_in_parallel_outside_the_apply_lock() {
        let live = slow_live(&[("a", 10), ("b", 10), ("c", 10)]);
        let started = tokio::time::Instant::now();
        let applying = tokio::spawn({
            let live = live.clone();
            async move {
                live.apply_set(1, vec![hive("a"), hive("b"), hive("c")])
                    .await
            }
        });
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(!applying.is_finished());
        assert!(
            live.applying.try_lock().is_ok(),
            "the apply lock is free while the catalogs build"
        );
        let report = applying.await.unwrap();
        assert!(report.applied && report.failed.is_empty());
        let took = started.elapsed();
        assert!(
            took < Duration::from_secs(15),
            "three 10 s builds took {took:?}: they did not run in parallel"
        );
        assert_eq!(live.specs().len(), 3);
        assert_eq!(live.generation(), 1);
    }

    /// A newer set is not held up behind an older set's slow build, and the
    /// older set, overtaken while it built, changes nothing.
    #[tokio::test(start_paused = true)]
    async fn a_slow_build_does_not_hold_up_a_newer_set() {
        let live = slow_live(&[("slow", 60)]);
        let older = tokio::spawn({
            let live = live.clone();
            async move { live.apply_set(1, vec![hive("slow")]).await }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        let newer = tokio::time::timeout(
            Duration::from_secs(5),
            live.apply_set(2, vec![hive("fast")]),
        )
        .await
        .expect("the newer set applies while the older one still builds");
        assert!(newer.applied);
        assert!(!older.is_finished());
        let older = older.await.unwrap();
        assert_eq!(older, ApplyReport::default(), "overtaken: nothing applied");
        assert_eq!(live.generation(), 2);
        let names: Vec<String> = live.specs().into_iter().map(|s| s.name).collect();
        assert_eq!(names, vec!["fast"]);
    }

    /// Two applies of sets that differ in one catalog: the one that swaps
    /// second rebuilds what the first changed under it.
    #[tokio::test(start_paused = true)]
    async fn an_apply_rebuilds_what_another_swapped_in_meanwhile() {
        let live = slow_live(&[("b", 30)]);
        live.install(hive("a"), fake_built());
        // Epoch 7 later than the held 0, so both apply regardless of order.
        let mut changed_a = hive("a");
        changed_a.metastore_uri = Some("10.0.0.2:9083".into());
        let slow = tokio::spawn({
            let live = live.clone();
            async move {
                live.apply_remote(arneb_rpc::CatalogSet {
                    epoch: 7,
                    generation: 2,
                    catalogs: vec![
                        serde_json::to_value(hive("a")).unwrap(),
                        serde_json::to_value(hive("b")).unwrap(),
                    ],
                    default_catalog: None,
                    default_schema: None,
                })
                .await
            }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        // An older generation of another epoch swaps in first and changes a.
        live.apply_remote(arneb_rpc::CatalogSet {
            epoch: 6,
            generation: 1,
            catalogs: vec![serde_json::to_value(&changed_a).unwrap()],
            default_catalog: None,
            default_schema: None,
        })
        .await;
        assert_eq!(live.epoch(), 6);
        let report = slow.await.unwrap();
        assert!(report.applied);
        assert_eq!((live.epoch(), live.generation()), (7, 2));
        let specs = live.specs();
        assert_eq!(specs.len(), 2);
        assert!(
            same_spec(&specs[0], &hive("a")),
            "a is the newer set's again"
        );
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
