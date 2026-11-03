//! The external catalogs (`hive`, `postgresql`) a node serves, kept live so
//! they can change at runtime (runtime-config D6, D8). The `memory` and
//! `file` catalogs are not managed here: they are startup-only.
//!
//! [`LiveCatalogs`] builds a catalog's provider and connector from its spec,
//! installs both into the shared [`CatalogManager`] and [`ConnectorRegistry`],
//! and remembers the installed specs plus the catalog generation they belong
//! to. A statement pins the providers and connectors together when it
//! starts ([`ConnectorRegistry::pinned`]; each change here, with the
//! `(epoch, generation)` tag it moves the registry to, is one
//! [`ConnectorRegistry::swap`]), so a change never disturbs a running
//! statement on the coordinator. Its distributed tasks carry the tag it
//! was planned at, and a worker runs a task only against that same set
//! (pulling once when it differs), so one query never mixes catalog
//! versions.

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
    /// True when the set was refused because it came from another epoch
    /// than the coordinator's current one ([`LiveCatalogs::current_epoch`]):
    /// an old coordinator's delayed push or pull. Nothing changed, and the
    /// worker is not in sync with the sender.
    pub stale_epoch: bool,
    /// `(catalog, error)` for each new or changed catalog that failed to
    /// build and was skipped.
    pub failed: Vec<(String, String)>,
}

/// What an applied set commits besides its installs and removes.
struct SetCommit {
    /// The set's catalogs that failed to build (Ruling R8).
    failed: std::collections::BTreeSet<String>,
    /// The fingerprint of each of the set's catalogs, as its sender serves
    /// it; recorded for the installed ones, changed or not.
    fingerprints: BTreeMap<String, u64>,
    /// The coordinator's default catalog and schema, when the set carries
    /// them (Ruling R12).
    defaults: Option<(String, String)>,
}

/// One caller's hold on an apply flight. Dropped (the apply finished, or
/// its caller was cancelled), it removes the flight from the map once no
/// other caller holds it, so a cancelled apply never leaves its entry.
struct FlightGuard<'a> {
    flights: &'a std::sync::Mutex<BTreeMap<(Option<u64>, u64), ApplyFlight>>,
    key: (Option<u64>, u64),
    flight: Option<ApplyFlight>,
}

impl Drop for FlightGuard<'_> {
    fn drop(&mut self) {
        let mut flights = self.flights.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(flight) = self.flight.take() {
            // Two holders left: the map and this caller. Dropped under the
            // lock, so concurrent callers count each other right.
            let last = Arc::strong_count(&flight) == 2;
            if last
                && flights
                    .get(&self.key)
                    .is_some_and(|f| Arc::ptr_eq(f, &flight))
            {
                flights.remove(&self.key);
            }
            drop(flight);
        }
    }
}

/// The fingerprint of a served spec's JSON.
fn value_fingerprint(value: &serde_json::Value) -> u64 {
    arneb_common::spec_fingerprint(&value.to_string())
}

impl ApplyReport {
    fn stale() -> Self {
        Self {
            stale_epoch: true,
            ..Self::default()
        }
    }
}

/// The node's live external catalogs. Share as `Arc<LiveCatalogs>`.
pub struct LiveCatalogs {
    manager: Arc<CatalogManager>,
    connectors: Arc<ConnectorRegistry>,
    global_storage: StorageConfig,
    /// The installed specs, by name.
    specs: RwLock<BTreeMap<String, CatalogConfig>>,
    /// The fingerprint of each installed spec as the coordinator serves it
    /// ([`arneb_common::spec_fingerprint`] of its effective spec's JSON):
    /// on a worker the coordinator's own, so both sides agree. Part of the
    /// registry's version tag; a task scans a catalog only at the
    /// fingerprint its query was planned with.
    fingerprints: RwLock<BTreeMap<String, u64>>,
    generation: AtomicU64,
    /// On a coordinator, its own epoch (ordered by start time); on a worker, the
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
    /// On a worker, the epoch the last heartbeat reply named (0 before
    /// any): the live coordinator's, so once known only a heartbeat moves
    /// it, down as well as up. A set from any other epoch is stale and
    /// never applies (a slow build of a set fetched before the coordinator
    /// restarted, a delayed push from the old one).
    heartbeat_epoch: AtomicU64,
    /// On a worker, the largest epoch a push or pull named; the current
    /// epoch only until a heartbeat names one.
    seen_epoch: AtomicU64,
    /// One apply per `(epoch, generation)` at a time: a push and a pull of
    /// the same set share one build of each changed catalog.
    apply_flights: std::sync::Mutex<BTreeMap<(Option<u64>, u64), ApplyFlight>>,
    /// Tests replace [`build`](Self::build) with this.
    #[cfg(test)]
    test_builder: Option<TestBuilder>,
    /// How long a test build of a catalog takes, by name.
    #[cfg(test)]
    test_build_delay: std::sync::Mutex<BTreeMap<String, Duration>>,
}

type ApplyFlight = Arc<tokio::sync::OnceCell<ApplyReport>>;

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
            fingerprints: RwLock::new(BTreeMap::new()),
            generation: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
            failed: RwLock::new(Default::default()),
            pushed_defaults: RwLock::new(None),
            unauthenticated_flight: std::sync::atomic::AtomicBool::new(false),
            applying: tokio::sync::Mutex::new(()),
            heartbeat_epoch: AtomicU64::new(0),
            seen_epoch: AtomicU64::new(0),
            apply_flights: Default::default(),
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

    /// Whether catalog `name` of the applied set failed to build here.
    pub fn failed_to_build(&self, name: &str) -> bool {
        self.failed
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .contains(name)
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
        self.connectors
            .swap(|| self.store_version(Some(epoch), self.generation()));
    }

    /// Sets the catalog generation (and moves the registry tag). Tests only:
    /// a real change commits its catalogs and generation together with
    /// [`commit`](Self::commit).
    #[cfg(test)]
    pub(crate) fn set_generation(&self, g: u64) {
        self.connectors.swap(|| self.store_version(None, g));
    }

    /// Moves to `generation` (and `epoch`, when given) and tags the
    /// registry with both, each catalog's fingerprint and the scan
    /// defaults. Only inside a [`ConnectorRegistry::swap`], after the
    /// catalogs, fingerprints and defaults changed. Generation before
    /// epoch: a reader seeing the new epoch also sees the generation that
    /// goes with it.
    fn store_version(&self, epoch: Option<u64>, generation: u64) {
        self.generation.store(generation, Ordering::SeqCst);
        if let Some(e) = epoch {
            self.epoch.store(e, Ordering::SeqCst);
        }
        let (default_catalog, default_schema) = self.scan_defaults();
        self.connectors
            .set_version(Some(arneb_common::CatalogVersion {
                epoch: self.epoch(),
                generation: self.generation(),
                catalogs: self
                    .fingerprints
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone(),
                default_catalog,
                default_schema,
            }));
    }

    /// `spec` as served to workers ([`snapshot`](Self::snapshot)): its
    /// effective spec as JSON.
    fn served_value(&self, spec: &CatalogConfig) -> Option<serde_json::Value> {
        serde_json::to_value(self.effective_spec(spec)).ok()
    }

    /// The fingerprint of `spec` as this node serves it.
    fn fingerprint(&self, spec: &CatalogConfig) -> u64 {
        value_fingerprint(&self.served_value(spec).unwrap_or_default())
    }

    /// The `(epoch, generation)` a [`ConnectorRegistry::pinned`] copy of
    /// this node's registry is tagged with when taken now.
    pub fn version(&self) -> (u64, u64) {
        (self.epoch(), self.generation())
    }

    /// Records an epoch a push or pull named. Until a heartbeat names the
    /// coordinator's epoch, the largest seen is taken as current (epochs
    /// grow with each coordinator start; 0 is ignored); after that a push
    /// or pull never moves it, so an old coordinator's delayed push cannot
    /// lock out the live one.
    pub fn note_coordinator_epoch(&self, epoch: u64) {
        if epoch != 0 {
            self.seen_epoch.fetch_max(epoch, Ordering::SeqCst);
        }
    }

    /// Records the epoch a heartbeat reply named. Only the live coordinator
    /// answers heartbeats, so its epoch is taken as is, even when smaller
    /// (a coordinator host whose clock went back): a worker never refuses
    /// the coordinator it is talking to.
    pub fn heartbeat_epoch(&self, epoch: u64) {
        if epoch != 0 {
            self.heartbeat_epoch.store(epoch, Ordering::SeqCst);
        }
    }

    /// The epoch of the coordinator this worker follows: the last
    /// heartbeat's, else the largest a push or pull named (0 before any).
    /// A set from another epoch is refused.
    pub fn current_epoch(&self) -> u64 {
        match self.heartbeat_epoch.load(Ordering::SeqCst) {
            0 => self.seen_epoch.load(Ordering::SeqCst),
            e => e,
        }
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
        effective_spec(&self.global_storage, spec)
    }

    /// Marks this coordinator as serving its set over Flight without a
    /// `[cluster] secret`: from then on [`exposure_check`](Self::exposure_check)
    /// refuses a spec that would serve a secret.
    pub fn set_unauthenticated_flight(&self, on: bool) {
        self.unauthenticated_flight.store(on, Ordering::SeqCst);
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

    /// The epoch, generation, installed specs (as effective specs, Ruling
    /// R12) and this node's default catalog and schema, for a worker to
    /// apply. The version and specs are read together outside any
    /// [`commit`](Self::commit) (under the registry's swap lock), so a
    /// served set is never tagged with another set's version: a worker
    /// that installed it under that tag would pass the task pin check
    /// with the wrong specs.
    pub fn snapshot(&self) -> arneb_rpc::CatalogSet {
        let (epoch, generation, specs) = self
            .connectors
            .consistent(|| (self.epoch(), self.generation(), self.specs()));
        let catalogs = specs
            .iter()
            .filter_map(|spec| self.served_value(spec))
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
    /// a set from another epoch (a restarted coordinator) applies and
    /// replaces the whole set, even at a lower generation (Ruling R6), but
    /// only when it is the coordinator's current epoch: the set's epoch is
    /// noted first ([`note_coordinator_epoch`](Self::note_coordinator_epoch)),
    /// and one older than the latest known is refused, so neither a slow
    /// build of a set fetched before a restart nor a delayed push from the
    /// old coordinator overwrites a newer set.
    /// A spec that does not decode is skipped with a WARN and listed as
    /// failed.
    pub async fn apply_remote(&self, set: arneb_rpc::CatalogSet) -> ApplyReport {
        self.note_coordinator_epoch(set.epoch);
        let mut specs = Vec::with_capacity(set.catalogs.len());
        let mut undecodable = Vec::new();
        for value in set.catalogs {
            let name = value
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("<unnamed>")
                .to_owned();
            // The coordinator's fingerprint: of the JSON it served, which
            // is what it tagged its own registry with.
            let fingerprint = value_fingerprint(&value);
            match serde_json::from_value::<CatalogConfig>(value) {
                Ok(spec) => specs.push((spec, fingerprint)),
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
        // The defaults go in with the catalogs, under the same tag.
        let defaults = set.default_catalog.zip(set.default_schema);
        let mut report = self
            .apply(Some(set.epoch), set.generation, specs, defaults)
            .await;
        if report.applied {
            report.failed.extend(undecodable);
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
    ///
    /// Tests only: it leaves the registry's version tag as it was, so a
    /// real change uses [`commit`](Self::commit).
    #[cfg(test)]
    pub(crate) fn install(&self, spec: CatalogConfig, built: BuiltCatalog) {
        // One swap: a statement pins both halves or neither.
        self.connectors.swap(|| self.install_unswapped(spec, built));
    }

    /// Removes the installed catalog `name`. False when no such catalog is
    /// installed here (the startup-only catalogs are never touched). Tests
    /// only, like [`install`](Self::install).
    #[cfg(test)]
    pub(crate) fn remove(&self, name: &str) -> bool {
        self.connectors.swap(|| self.remove_unswapped(name))
    }

    /// Installs `installs`, removes `removes` and moves to `generation`, all
    /// in one [`ConnectorRegistry::swap`]: a statement pins either the whole
    /// previous set and its tag or the whole new one. What a coordinator's
    /// runtime change and re-seed commit with.
    pub fn commit(
        &self,
        generation: u64,
        installs: Vec<(CatalogConfig, BuiltCatalog)>,
        removes: &[String],
    ) {
        self.commit_at(None, generation, installs, removes, None);
    }

    /// [`commit`](Self::commit) that also moves to `epoch` (when given) and
    /// commits what `set` adds when given (an applied set).
    fn commit_at(
        &self,
        epoch: Option<u64>,
        generation: u64,
        installs: Vec<(CatalogConfig, BuiltCatalog)>,
        removes: &[String],
        set: Option<SetCommit>,
    ) {
        self.connectors.swap(|| {
            for (spec, built) in installs {
                self.install_unswapped(spec, built);
            }
            for name in removes {
                self.remove_unswapped(name);
            }
            if let Some(set) = set {
                *self.failed.write().unwrap_or_else(|p| p.into_inner()) = set.failed;
                let installed = self.read_specs();
                let mut fingerprints = self.fingerprints.write().unwrap_or_else(|p| p.into_inner());
                for (name, fingerprint) in set.fingerprints {
                    if installed.contains_key(&name) {
                        fingerprints.insert(name, fingerprint);
                    }
                }
                drop((installed, fingerprints));
                if let Some(defaults) = set.defaults {
                    *self
                        .pushed_defaults
                        .write()
                        .unwrap_or_else(|p| p.into_inner()) = Some(defaults);
                }
            }
            self.store_version(epoch, generation);
        });
    }

    fn install_unswapped(&self, spec: CatalogConfig, built: BuiltCatalog) {
        let name = spec.name.clone();
        self.connectors.register(name.clone(), built.connector);
        self.manager.register_catalog(name.clone(), built.provider);
        self.failed
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&name);
        let fingerprint = self.fingerprint(&spec);
        self.fingerprints
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(name.clone(), fingerprint);
        self.write_specs().insert(name, spec);
    }

    fn remove_unswapped(&self, name: &str) -> bool {
        if self.write_specs().remove(name).is_none() {
            return false;
        }
        self.fingerprints
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(name);
        self.manager.deregister_catalog(name);
        self.connectors.deregister(name);
        true
    }

    /// Makes the installed set equal `specs` at `generation`, unless that
    /// generation is not newer than the one held (then nothing changes).
    /// Every new or changed catalog is built first; then they are swapped
    /// in and the catalogs not in the set are removed. A catalog that fails
    /// to build is skipped with a WARN and listed in the report; a changed
    /// one is removed rather than left at its previous version, so nothing
    /// tagged with the new generation serves the old spec (a task finds it
    /// missing, retries the build once and otherwise fails). The rest
    /// still apply and the generation still advances.
    pub async fn apply_set(&self, generation: u64, specs: Vec<CatalogConfig>) -> ApplyReport {
        let specs = specs
            .into_iter()
            .map(|spec| {
                let fingerprint = self.fingerprint(&spec);
                (spec, fingerprint)
            })
            .collect();
        self.apply(None, generation, specs, None).await
    }

    /// [`apply_set`](Self::apply_set), except that a set from `epoch` other
    /// than the one held applies whatever its generation (when `epoch` is
    /// the coordinator's current one, see [`applies`](Self::applies)), and
    /// then records `epoch`. `None` checks the generation only and keeps
    /// the epoch.
    ///
    /// The new and changed catalogs are built in parallel without any
    /// lock; `applying` is held only to re-check the generation and swap.
    /// A set overtaken by a newer one while it was building changes
    /// nothing. When another apply swapped in between, a catalog this set
    /// skipped as unchanged may now differ: it is built (again outside the
    /// lock) before the swap.
    ///
    /// Concurrent applies of the same `(epoch, generation)` (a push and a
    /// heartbeat-triggered pull of one change) share one run and its
    /// report, so each changed catalog is built once.
    ///
    /// Each spec comes with its fingerprint as the coordinator serves it,
    /// and `defaults` (the coordinator's default catalog and schema) are
    /// committed with the catalogs when given.
    async fn apply(
        &self,
        epoch: Option<u64>,
        generation: u64,
        specs: Vec<(CatalogConfig, u64)>,
        defaults: Option<(String, String)>,
    ) -> ApplyReport {
        let key = (epoch, generation);
        let flight = self
            .apply_flights
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(key)
            .or_default()
            .clone();
        // Leaves the map when its last caller does, finished or cancelled.
        let guard = FlightGuard {
            flights: &self.apply_flights,
            key,
            flight: Some(flight),
        };
        let flight = guard.flight.as_ref().expect("set above");
        flight
            .get_or_init(|| self.apply_once(epoch, generation, specs, defaults))
            .await
            .clone()
    }

    async fn apply_once(
        &self,
        epoch: Option<u64>,
        generation: u64,
        specs: Vec<(CatalogConfig, u64)>,
        defaults: Option<(String, String)>,
    ) -> ApplyReport {
        let fingerprints: BTreeMap<String, u64> = specs
            .iter()
            .map(|(spec, fingerprint)| (spec.name.clone(), *fingerprint))
            .collect();
        let specs: Vec<CatalogConfig> = specs.into_iter().map(|(spec, _)| spec).collect();
        if self.stale_epoch(epoch) {
            return ApplyReport::stale();
        }
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
            // A newer set may have been applied, or a heartbeat named
            // another epoch, while this one was building.
            if self.stale_epoch(epoch) {
                return ApplyReport::stale();
            }
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
            let installs: Vec<_> = std::mem::take(&mut built).into_values().collect();
            for (spec, _) in &installs {
                tracing::info!(
                    catalog = %spec.name,
                    catalog_type = %spec.catalog_type,
                    generation,
                    "registered catalog"
                );
            }
            // A changed catalog whose build failed goes too: the registry is
            // about to be tagged with this generation, which must never pair
            // with the previous spec.
            let removes: Vec<String> = now
                .keys()
                .filter(|n| !wanted.contains(*n) || failed.contains_key(*n))
                .cloned()
                .collect();
            for name in &removes {
                tracing::info!(catalog = %name, generation, "removed catalog");
            }
            self.commit_at(
                epoch,
                generation,
                installs,
                &removes,
                Some(SetCommit {
                    failed: failed.keys().cloned().collect(),
                    fingerprints,
                    defaults,
                }),
            );
            return ApplyReport {
                applied: true,
                failed: failed.into_iter().collect(),
                stale_epoch: false,
            };
        }
    }

    /// Whether a set from `epoch` comes from another coordinator than the
    /// current one ([`current_epoch`](Self::current_epoch)); any is
    /// current before one is known.
    fn stale_epoch(&self, epoch: Option<u64>) -> bool {
        let current = self.current_epoch();
        epoch.is_some_and(|e| current != 0 && e != current)
    }

    /// Whether a set at `(epoch, generation)` that is not
    /// [stale](Self::stale_epoch) applies now: a newer generation within
    /// the epoch held, a set from another epoch, or the set already held
    /// while some of it failed to build (Ruling R8).
    fn applies(&self, epoch: Option<u64>, generation: u64) -> bool {
        let same_epoch = epoch.is_none_or(|e| e == self.epoch());
        let retry = same_epoch && generation == self.generation() && self.has_failed_builds();
        !same_epoch || generation > self.generation() || retry
    }
}

/// Why a set from coordinator `epoch` was refused by a worker following
/// `current`.
pub fn stale_epoch_error(epoch: u64, current: u64) -> String {
    format!(
        "catalog set from coordinator epoch {epoch} refused: this worker follows epoch \
         {current} (its last heartbeat's coordinator)"
    )
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
                let (epoch, generation) = (set.epoch, set.generation);
                let report = live.apply_remote(set).await;
                if report.stale_epoch {
                    return Err(stale_epoch_error(epoch, live.current_epoch()));
                }
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
        if let Some(epoch) = reply.catalog_epoch {
            live.heartbeat_epoch(epoch);
        }
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
            let pg = spec.postgres_config().map_err(|e| e.to_string())?;
            // Also refuses a `password` parameter set twice (only the last
            // parameter may be one), which a GET → PUT round trip could not
            // tell apart once redacted.
            pg.validate_url()
                .map_err(|e| format!("catalog '{name}': {e}"))?;
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
/// `password`, the password inside `connection_url` (all of it after a
/// leading `postgresql://` when the URL fails validation, or the whole
/// value when it starts with no such scheme), and the storage
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
    if let Some(url) = spec.connection_url.as_mut().filter(|u| {
        arneb_postgresql::PostgresConfig::new(u.0.clone())
            .validate_url()
            .is_err()
    }) {
        // Where the driver ends the password of a URL that validation
        // refuses (it may be stored: config-file catalogs, older states)
        // is no guide to what is secret: hide everything after a leading
        // `postgresql://` or `postgres://`, and the whole value otherwise
        // (a libpq keyword string `host=db password=x sslrootcert=file:///…`,
        // a mistyped `postgresql:/db?password=x`): a `://` further in may
        // sit inside a parameter or the password itself.
        match pg_scheme_len(&url.0) {
            Some(scheme) => url.0.replace_range(scheme.., REDACTED),
            None => url.0 = REDACTED.to_owned(),
        }
    } else if let Some(url) = &mut spec.connection_url {
        let mut ranges: Vec<_> = url_query_passwords(&url.0)
            .into_iter()
            .map(|(_, value)| value)
            .chain(url_password_range(&url.0))
            .collect();
        // Also any `password=` after the first `?`, wherever the driver's
        // userinfo heuristic ends (a URL with an unencoded `@` in a query
        // value; refused by validation, but it may be stored).
        let loose: Vec<_> = url_loose_query_passwords(&url.0)
            .into_iter()
            .filter(|l| !ranges.iter().any(|r| r.start < l.end && l.start < r.end))
            .collect();
        ranges.extend(loose);
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
        let whole = url.0 == REDACTED
            || pg_scheme_len(&url.0).is_some_and(|scheme| url.0[scheme..] == *REDACTED);
        let userinfo = url_password_range(&url.0).is_some_and(|r| &url.0[r] == REDACTED);
        let query = url_query_passwords(&url.0)
            .into_iter()
            .any(|(_, value)| &url.0[value] == REDACTED);
        if whole || userinfo || query {
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

/// `spec` as a node whose global `[storage]` is `global` serves it (Ruling
/// R12): a hive spec with the global storage merged in.
fn effective_spec(global: &StorageConfig, spec: &CatalogConfig) -> CatalogConfig {
    let mut effective = spec.clone();
    if spec.catalog_type == "hive" {
        let storage = StorageConfig::merge(global, spec.storage.as_ref());
        let empty = storage.s3.is_none() && storage.gcs.is_none() && storage.azure.is_none();
        effective.storage = (!empty).then_some(storage);
    }
    effective
}

/// The secrets `specs` would serve from a node whose global `[storage]` is
/// `global`, as `catalog 'x': field` (effective specs, so the global keys
/// merged into a hive spec count).
pub fn effective_secrets(global: &StorageConfig, specs: &[CatalogConfig]) -> Vec<String> {
    specs
        .iter()
        .flat_map(|spec| {
            secret_fields(&effective_spec(global, spec))
                .into_iter()
                .map(|field| format!("catalog '{}': {field}", spec.name))
        })
        .collect()
}

/// The secrets `spec` carries, by field: `password`, a password in
/// `connection_url` (userinfo or query parameter; any URL that fails
/// validation counts whole, as [`redacted_spec`] hides it whole), `storage.s3.secret_access_key`
/// and `storage.azure.access_key`. A GCS `service_account_path` is a path on
/// the node, not the key itself, so it is not listed.
pub fn secret_fields(spec: &CatalogConfig) -> Vec<&'static str> {
    let mut found = Vec::new();
    if spec.password.is_some() {
        found.push("password");
    }
    if let Some(url) = &spec.connection_url {
        // A refused URL (a libpq keyword string, or one on a spec of
        // another type that nothing validates) may hold a password the
        // finders above cannot place.
        if url_password_range(&url.0).is_some()
            || !url_query_passwords(&url.0).is_empty()
            || !url_loose_query_passwords(&url.0).is_empty()
            || arneb_postgresql::PostgresConfig::new(url.0.clone())
                .validate_url()
                .is_err()
        {
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

/// The length of the `postgresql://` or `postgres://` (any case) that
/// `url` starts with: the only prefix a refused URL keeps when redacted.
fn pg_scheme_len(url: &str) -> Option<usize> {
    ["postgresql://", "postgres://"]
        .into_iter()
        .find(|scheme| {
            url.get(..scheme.len())
                .is_some_and(|p| p.eq_ignore_ascii_case(scheme))
        })
        .map(str::len)
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
    query_passwords_from(url, host)
}

/// The value ranges of every `password` query parameter (key
/// percent-decoded) after the first `?` past the scheme, ignoring the
/// userinfo: the redaction and secret checks use these on top of the
/// driver-faithful ranges, so a URL the driver misreads still hides them.
fn url_loose_query_passwords(url: &str) -> Vec<std::ops::Range<usize>> {
    let Some(scheme_end) = url.find("://") else {
        return Vec::new();
    };
    query_passwords_from(url, scheme_end)
        .into_iter()
        .map(|(_, value)| value)
        .collect()
}

/// `(parameter, value)` ranges of the `password` parameters of the query
/// that starts at the first `?` at or after byte `from`.
fn query_passwords_from(
    url: &str,
    from: usize,
) -> Vec<(std::ops::Range<usize>, std::ops::Range<usize>)> {
    let Some(q) = url[from..].find('?').map(|i| from + i + 1) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut start = q;
    for param in url[q..].split('&') {
        if let Some((key, value)) = param.split_once('=') {
            if arneb_postgresql::is_password_key(key) {
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

    /// A registry tag's `(epoch, generation)`.
    pub(crate) fn eg(tag: Option<Arc<arneb_common::CatalogVersion>>) -> Option<(u64, u64)> {
        tag.map(|t| (t.epoch, t.generation))
    }

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
        // The old coordinator's epoch is now older: refused (M1).
        assert!(!live.apply_remote(set(11, 9, vec![])).await.applied);
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
                "postgresql://bob:ui-secret-7@db/sales?sslmode=disable&application_name=x&password=q-secret-6"
        })));
        let text = query.to_string();
        assert!(!text.contains("q-secret-6") && !text.contains("ui-secret-7"));
        assert_eq!(
            query["connection_url"],
            "postgresql://bob:<redacted>@db/sales?sslmode=disable&application_name=x&password=<redacted>"
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
            "connection_url": "postgresql://bob@db/sales?sslmode=disable&pass%77ord=enc-secret-8"
        }));
        let redacted = redacted_spec(&stored);
        assert!(!redacted.to_string().contains("enc-secret-8"));
        assert_eq!(
            redacted["connection_url"],
            "postgresql://bob@db/sales?sslmode=disable&pass%77ord=<redacted>"
        );
        // Keys are case-sensitive, as in tokio-postgres, which refuses the
        // unknown `PASSWORD`: a URL it refuses is hidden whole.
        let upper = redacted_spec(&spec(serde_json::json!({
            "name": "p", "type": "postgresql",
            "connection_url": "postgresql://bob@db/sales?pass%77ord=enc-secret-8&PASS%57ORD=x"
        })));
        assert_eq!(upper["connection_url"], "postgresql://<redacted>");
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
        assert!(arneb_postgresql::is_password_key(
            "%70%61%73%73%77%6F%72%64"
        ));
        assert!(!arneb_postgresql::is_password_key("password%"));
        assert!(!arneb_postgresql::is_password_key("passwor"));
    }

    /// tokio-postgres takes the credentials up to the first `@`, so an
    /// unencoded `/`, `?` or `#` in the password is still the password
    /// (and a `#` in a query value is part of it). Such a userinfo puts
    /// the `@` past the authority, so the whole URL after the scheme is
    /// hidden (validation refuses it); a `#` in a query value is redacted
    /// in place and round-trips.
    #[test]
    fn password_with_url_delimiters_is_redacted() {
        use std::str::FromStr;
        for (url, redacted) in [
            (
                "postgresql://app:pa/ss@db:5432/x",
                "postgresql://<redacted>",
            ),
            (
                "postgresql://app:pa?ss@db:5432/x",
                "postgresql://<redacted>",
            ),
            (
                "postgresql://app:pa#ss@db:5432/x",
                "postgresql://<redacted>",
            ),
            (
                "postgres://app:a/b?c#d:e@db/x?sslmode=disable",
                "postgres://<redacted>",
            ),
            (
                "postgresql://app@db/x?sslmode=disable&password=pa#ss",
                "postgresql://app@db/x?sslmode=disable&password=<redacted>",
            ),
            (
                "postgresql://app:pa?password=no@db/x?password=q/s#t",
                "postgresql://<redacted>",
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
            if redacted.ends_with("://<redacted>") {
                assert!(validate_spec(&stored).is_err(), "{url}");
                continue;
            }
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
        // Round 4 follow-up: a URL that fails validation counts as a
        // secret whole, even on a hive spec, where nothing validates it.
        for stray in [
            serde_json::json!({"name": "k", "type": "postgresql",
                "connection_url": "host=db password=kw-secret"}),
            serde_json::json!({"name": "h", "type": "hive", "metastore_uri": "h:9083",
                "connection_url": "host=db password=kw-secret"}),
        ] {
            let stray = spec(stray);
            assert_eq!(secret_fields(&stray), vec!["connection_url password"]);
        }
        let gcs_only = spec(serde_json::json!({
            "name": "g", "type": "postgresql", "connection_url": "postgresql://u@h/db",
            "storage": {"gcs": {"service_account_path": "/keys/sa.json"}}
        }));
        assert_eq!(live.exposure_check(&gcs_only), Ok(()));

        let (lake, pg) = (hive("lake"), pg_pw);
        assert_eq!(
            effective_secrets(&live.global_storage, &[lake, pg]),
            vec![
                "catalog 'lake': storage.s3.secret_access_key",
                "catalog 'p': password"
            ]
        );
        assert_eq!(secret_fields(&secret_spec()).len(), 4);
    }

    /// M2: an unencoded `@` in a query value makes the driver read the
    /// rest as the host; the query password is still redacted and found as
    /// a secret, and validation refuses the URL without echoing it.
    #[test]
    fn query_password_after_an_unencoded_at_is_redacted_and_refused() {
        for (url, parts) in [
            (
                "postgresql://db.internal:5432/sales?user=admin@srv&password=S3cret",
                &["S3cret"][..],
            ),
            (
                "postgresql://db/sales?user=admin@srv&password=S3cret",
                &["S3cret"],
            ),
            // A port or an IPv6 host: the driver would take `9zK` as the
            // host (round 3).
            (
                "postgresql://db.internal:5432/sales?user=app&password=Xy@9zK",
                &["Xy", "9zK"],
            ),
            ("postgresql://[::1]:5432/db?password=Xy@9zK", &["Xy", "9zK"]),
            // An unencoded `@` inside the authority: the driver's host holds
            // the password's tail (round 3, I3).
            ("postgresql://u:p@Qz7@db/x", &["Qz7"]),
            ("postgresql://u:p@Qz7,h2@db/x", &["Qz7"]),
        ] {
            let s = spec(serde_json::json!({
                "name": "p", "type": "postgresql", "connection_url": url
            }));
            let shown = redacted_spec(&s);
            assert_eq!(shown["connection_url"], "postgresql://<redacted>", "{url}");
            assert_eq!(secret_fields(&s), vec!["connection_url password"]);
            let e = validate_spec(&s).unwrap_err();
            assert!(e.contains("%40"), "{e}");
            // A GET → PUT round trip of the redacted form is refused.
            let back: CatalogConfig = serde_json::from_value(shown.clone()).unwrap();
            assert_eq!(redacted_secrets(&back), vec!["connection_url password"]);
            let pg = s.postgres_config().unwrap();
            assert_eq!(pg.display_target(), "<invalid connection_url>");
            // Nor does building it log or return any part of the password.
            let live = LiveCatalogs::new(
                Arc::new(CatalogManager::new("memory", "default")),
                Arc::new(ConnectorRegistry::new()),
                StorageConfig::default(),
            );
            let (err, logs) = capture_logs(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(live.build(&s))
                    .unwrap_err()
            });
            for part in parts {
                for shown in [
                    &e,
                    &shown.to_string(),
                    &format!("{pg:?}"),
                    &format!("{s:?}"),
                    &err.to_string(),
                    &logs,
                ] {
                    assert!(!shown.contains(part), "{url}: {part} in {shown}");
                }
            }
        }
    }

    /// Runs `f` and returns what it logged.
    fn capture_logs<R>(f: impl FnOnce() -> R) -> (R, String) {
        #[derive(Clone, Default)]
        struct Buf(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        // A global subscriber interested in everything, so a callsite is
        // never cached as disabled by a test on another thread.
        static GLOBAL: std::sync::Once = std::sync::Once::new();
        GLOBAL.call_once(|| {
            let _ = tracing::subscriber::set_global_default(
                tracing_subscriber::fmt()
                    .with_writer(std::io::sink)
                    .with_max_level(tracing::Level::TRACE)
                    .finish(),
            );
        });
        let buf = Buf::default();
        let sink = buf.clone();
        let sub = tracing_subscriber::fmt()
            .with_writer(move || sink.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish();
        let r = tracing::subscriber::with_default(sub, f);
        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        (r, out)
    }

    /// M6: a URL that sets `password=` twice is refused, so a GET → PUT
    /// round trip cannot change which one the driver uses. Since round 4
    /// the password must be the last parameter, which covers it, and the
    /// refused URL is hidden whole.
    #[test]
    fn duplicate_query_passwords_are_refused() {
        let s = spec(serde_json::json!({
            "name": "p", "type": "postgresql",
            "connection_url": "postgresql://u@db/x?password=a1&sslmode=disable&pass%77ord=b2"
        }));
        let e = validate_spec(&s).unwrap_err();
        assert!(e.contains("last one") && !e.contains("a1"), "{e}");
        let shown = redacted_spec(&s);
        assert_eq!(shown["connection_url"], "postgresql://<redacted>");
    }

    /// Round 4: a raw `&` in a query password would turn its tail into a
    /// real driver parameter (a host sent to DNS). Such a URL is refused,
    /// and shown with nothing after the scheme.
    #[test]
    fn a_query_password_with_a_raw_ampersand_is_refused_and_hidden() {
        for url in [
            "postgresql://db/x?password=Xy&host=9zK",
            "postgresql://db/x?sslmode=disable&password=Xy&dbname=9zK",
            "postgresql://u@db/x?pass%77ord=Xy&options=9zK",
        ] {
            let s = spec(serde_json::json!({
                "name": "p", "type": "postgresql", "connection_url": url
            }));
            let e = validate_spec(&s).unwrap_err();
            assert!(!e.contains("Xy") && !e.contains("9zK"), "{e}");
            let shown = redacted_spec(&s);
            assert_eq!(shown["connection_url"], "postgresql://<redacted>", "{url}");
            assert!(!shown.to_string().contains("9zK"), "{shown}");
        }
        let encoded = spec(serde_json::json!({
            "name": "p", "type": "postgresql",
            "connection_url": "postgresql://db/x?sslmode=disable&password=Xy%26host%3D9zK"
        }));
        validate_spec(&encoded).unwrap();
        assert_eq!(
            redacted_spec(&encoded)["connection_url"],
            "postgresql://db/x?sslmode=disable&password=<redacted>"
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

    /// Round 3: a pull fetched epoch 1's set just before the coordinator
    /// restarted, and its slow build finishes after the new coordinator's
    /// (epoch 2) push installed: the stale set changes nothing.
    #[tokio::test(start_paused = true)]
    async fn a_slow_set_from_an_older_epoch_never_overwrites_a_newer_one() {
        let live = slow_live(&[("slow", 10)]);
        live.note_coordinator_epoch(1);
        assert!(live.apply_remote(set(1, 4, vec![])).await.applied);
        let stale = tokio::spawn({
            let live = live.clone();
            async move {
                let slow = serde_json::to_value(hive("slow")).unwrap();
                live.apply_remote(set(1, 5, vec![slow])).await
            }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        // The restarted coordinator pushes.
        let fast = serde_json::to_value(hive("fast")).unwrap();
        assert!(live.apply_remote(set(2, 1, vec![fast])).await.applied);
        assert_eq!(stale.await.unwrap(), ApplyReport::stale());
        assert_eq!((live.epoch(), live.generation()), (2, 1));
        let names: Vec<String> = live.specs().into_iter().map(|s| s.name).collect();
        assert_eq!(names, vec!["fast"]);
        // And a late set from the old epoch is refused outright.
        assert!(!live.apply_remote(set(1, 9, vec![])).await.applied);
    }

    /// Round 3 (M1): a push from the old coordinator delivered after the
    /// new one's heartbeat or push is refused and leaves the newer epoch in
    /// place; a heartbeat names the live coordinator's epoch as is, so a
    /// coordinator whose clock went back is still followed.
    #[tokio::test]
    async fn a_delayed_push_from_an_older_epoch_is_refused() {
        let (live, _, _) = live();
        assert!(live.apply_remote(set(200, 3, vec![])).await.applied);
        live.heartbeat_epoch(300);
        let fast = serde_json::to_value(hive("fast")).unwrap();
        assert!(live.apply_remote(set(300, 1, vec![fast])).await.applied);
        let old = serde_json::to_value(hive("old")).unwrap();
        let report = live.apply_remote(set(200, 4, vec![old])).await;
        assert!(!report.applied && report.stale_epoch);
        assert_eq!((live.epoch(), live.generation()), (300, 1));
        assert_eq!(live.specs()[0].name, "fast");
        // A later pull from the same old epoch is refused too.
        assert!(live.apply_remote(set(200, 5, vec![])).await.stale_epoch);
        // The live coordinator restarted with a clock that went back.
        live.heartbeat_epoch(250);
        assert!(live.apply_remote(set(250, 1, vec![])).await.applied);
        assert_eq!(live.epoch(), 250);
    }

    /// Round 4: once a heartbeat lowered the epoch (the coordinator moved to
    /// a host whose clock is behind), a delayed push from the old, larger
    /// epoch neither applies nor raises the epoch back, so pulls from the
    /// live coordinator keep applying.
    #[tokio::test]
    async fn a_push_from_a_larger_old_epoch_cannot_undo_a_heartbeat() {
        let (live, _, _) = live();
        live.heartbeat_epoch(500);
        assert!(live.apply_remote(set(500, 7, vec![])).await.applied);
        // The coordinator moved; its clock is behind.
        live.heartbeat_epoch(400);
        assert!(live.apply_remote(set(400, 1, vec![])).await.applied);
        let zombie = serde_json::to_value(hive("zombie")).unwrap();
        let report = live.apply_remote(set(500, 8, vec![zombie])).await;
        assert!(report.stale_epoch && !report.applied);
        assert_eq!(live.current_epoch(), 400);
        assert_eq!((live.epoch(), live.generation()), (400, 1));
        assert!(live.specs().is_empty());
        // The live coordinator's next change still applies.
        assert!(live.apply_remote(set(400, 2, vec![])).await.applied);
        assert_eq!(live.version(), (400, 2));
    }

    #[test]
    fn epochs_grow_with_the_start_time() {
        assert!(crate::epoch_from(1_000, u16::MAX) < crate::epoch_from(1_001, 0));
        assert_eq!(crate::epoch_from(0, 0), 1);
    }

    /// Round 4: an apply whose caller is cancelled mid-build leaves no
    /// entry in the single-flight map, and a later apply of the same set
    /// runs.
    #[tokio::test(start_paused = true)]
    async fn a_cancelled_apply_leaves_no_flight_behind() {
        let live = slow_live(&[("slow", 10)]);
        let task = tokio::spawn({
            let live = live.clone();
            async move {
                let slow = serde_json::to_value(hive("slow")).unwrap();
                live.apply_remote(set(1, 1, vec![slow])).await
            }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(live.apply_flights.lock().unwrap().len(), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(live.apply_flights.lock().unwrap().is_empty());
        let slow = serde_json::to_value(hive("slow")).unwrap();
        assert!(live.apply_remote(set(1, 1, vec![slow])).await.applied);
        assert!(live.apply_flights.lock().unwrap().is_empty());
    }

    /// Round 3: a push and a pull of the same `(epoch, generation)` that
    /// overlap build each changed catalog once and report the same result.
    #[tokio::test(start_paused = true)]
    async fn overlapping_applies_of_one_set_build_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let builds = Arc::new(AtomicUsize::new(0));
        let n = builds.clone();
        let live = LiveCatalogs::with_test_builder(
            Arc::new(CatalogManager::new("memory", "default")),
            Arc::new(ConnectorRegistry::new()),
            Box::new(move |_| {
                n.fetch_add(1, Ordering::SeqCst);
                Ok(fake_built())
            }),
        );
        live.test_build_delay
            .lock()
            .unwrap()
            .insert("a".into(), Duration::from_secs(5));
        let changed = || set(7, 2, vec![serde_json::to_value(hive("a")).unwrap()]);
        let (push, pull) = tokio::join!(live.apply_remote(changed()), live.apply_remote(changed()));
        assert!(push.applied && pull.applied);
        assert_eq!(builds.load(Ordering::SeqCst), 1, "one build of a");
        assert_eq!(live.generation(), 2);
        // A later apply of the same set is stale, as before.
        assert!(!live.apply_remote(changed()).await.applied);
        assert_eq!(builds.load(Ordering::SeqCst), 1);
    }

    /// Round 3 (R1): a stored URL that validation refuses and that has no
    /// `://` (a libpq keyword string, a one-slash typo) is hidden whole,
    /// and the redacted form is a placeholder a PUT cannot store.
    #[test]
    fn a_refused_url_without_a_scheme_separator_is_hidden_whole() {
        for url in [
            "host=db.internal user=app password=hunter2 dbname=sales",
            "postgresql:/db?password=hunter2",
            "hunter2",
            // Round 4: a `://` inside a value is no scheme.
            "host=db user=app password=hunter2 sslrootcert=file:///etc/ca.pem",
            "password=hunter2 host=db sslrootcert=file:///etc/ca.pem",
            "mysql://app:hunter2@db/x",
        ] {
            let s = spec(serde_json::json!({
                "name": "p", "type": "postgresql", "connection_url": url
            }));
            let shown = redacted_spec(&s);
            assert_eq!(shown["connection_url"], REDACTED, "{url}");
            assert!(!shown.to_string().contains("hunter2"), "{shown}");
            assert!(!format!("{s:?}").contains("hunter2"));
            let e = validate_spec(&s).unwrap_err();
            assert!(!e.contains("hunter2"), "{e}");
            let back: CatalogConfig = serde_json::from_value(shown).unwrap();
            assert_eq!(redacted_secrets(&back), vec!["connection_url password"]);
        }
    }

    /// Round 4: a refused URL keeps only a leading `postgresql://` or
    /// `postgres://` (any case), never text before a `://` in its password.
    #[test]
    fn a_refused_url_keeps_only_a_leading_postgres_scheme() {
        for (url, shown_as) in [
            ("postgresql://u:p@hunter2@db/x", "postgresql://<redacted>"),
            ("POSTGRES://u:hunter2@db/x", "POSTGRES://<redacted>"),
            (
                "postgresql://db/x?password=hunter2://y@z",
                "postgresql://<redacted>",
            ),
            ("hunter2://x@db/y", REDACTED),
            ("u:hunter2://@db", REDACTED),
        ] {
            let s = spec(serde_json::json!({
                "name": "p", "type": "postgresql", "connection_url": url
            }));
            assert!(validate_spec(&s).is_err(), "{url}");
            let shown = redacted_spec(&s);
            assert_eq!(shown["connection_url"], shown_as, "{url}");
            assert!(!shown.to_string().contains("hunter2"), "{shown}");
            let back: CatalogConfig = serde_json::from_value(shown).unwrap();
            assert_eq!(redacted_secrets(&back), vec!["connection_url password"]);
        }
    }

    /// Round 3 (I2): a snapshot taken while commits run never pairs one
    /// generation's specs with another's version.
    #[test]
    fn a_snapshot_is_never_torn_by_a_commit() {
        let (live, _, _) = live();
        live.set_epoch(3);
        let uri = |g: u64| format!("h{g}:9083");
        let writer = std::thread::spawn({
            let live = live.clone();
            move || {
                for g in 1..=3000u64 {
                    let mut spec = hive("a");
                    spec.metastore_uri = Some(uri(g));
                    live.commit(g, vec![(spec, fake_built())], &[]);
                }
            }
        });
        let mut checked = 0;
        while !writer.is_finished() {
            let set = live.snapshot();
            if let Some(spec) = set.catalogs.first() {
                assert_eq!(
                    spec["metastore_uri"],
                    uri(set.generation),
                    "specs of another generation"
                );
                assert_eq!(set.epoch, 3);
                checked += 1;
            }
        }
        writer.join().unwrap();
        assert!(checked > 0);
    }

    /// Round 4: a worker's tag carries the coordinator's fingerprint of
    /// each catalog after the set crossed the wire, even when the
    /// coordinator merged its global `[storage]` in and the worker has a
    /// `[storage]` of its own; an unchanged catalog keeps its fingerprint
    /// across generations, a changed one gets a new one.
    #[tokio::test]
    async fn worker_and_coordinator_agree_on_each_catalog_fingerprint() {
        let storage = |region: &str| -> StorageConfig {
            serde_json::from_value(serde_json::json!({"s3": {"region": region}})).unwrap()
        };
        let coordinator_registry = Arc::new(ConnectorRegistry::new());
        let coordinator = LiveCatalogs::with_test_builder_and_storage(
            Arc::new(CatalogManager::new("lake", "tpch")),
            coordinator_registry.clone(),
            storage("us-east-1"),
            Box::new(|_| Ok(fake_built())),
        );
        coordinator.set_epoch(4);
        let pg = spec(serde_json::json!({
            "name": "pg", "type": "postgresql", "connection_url": "postgresql://u@h/db"
        }));
        coordinator.commit(
            1,
            vec![(hive("lake"), fake_built()), (pg, fake_built())],
            &[],
        );
        let worker_registry = Arc::new(ConnectorRegistry::new());
        let worker = LiveCatalogs::with_test_builder_and_storage(
            Arc::new(CatalogManager::new("memory", "default")),
            worker_registry.clone(),
            storage("eu-west-1"),
            Box::new(|_| Ok(fake_built())),
        );
        let wire = arneb_rpc::CatalogSet::decode(&coordinator.snapshot().encode()).unwrap();
        assert!(worker.apply_remote(wire).await.applied);
        let (planned, held) = (
            coordinator_registry.version().unwrap(),
            worker_registry.version().unwrap(),
        );
        assert_eq!(planned.catalogs.len(), 2);
        assert_eq!(
            held, planned,
            "same fingerprints, epoch, generation and defaults"
        );
        assert_eq!(held.default_catalog, "lake");
        // A change to `pg` alone leaves `lake`'s fingerprint as it was.
        let pg2 = spec(serde_json::json!({
            "name": "pg", "type": "postgresql", "connection_url": "postgresql://u@h2/db"
        }));
        coordinator.commit(2, vec![(pg2, fake_built())], &[]);
        let after = coordinator_registry.version().unwrap();
        assert_eq!(after.catalog("lake"), planned.catalog("lake"));
        assert_ne!(after.catalog("pg"), planned.catalog("pg"));
    }

    /// Every change moves the registry's `(epoch, generation)` tag in the
    /// same swap as the connectors, so a pinned copy names the set it holds.
    #[tokio::test]
    async fn the_registry_tag_follows_every_change() {
        let (live, _, connectors) = live();
        live.set_epoch(5);
        assert_eq!(eg(connectors.version()), Some((5, 0)));
        live.commit(3, vec![(hive("a"), fake_built())], &[]);
        assert_eq!(eg(connectors.version()), Some((5, 3)));
        let (pinned, _) = connectors.pinned(|| ());
        live.commit(4, Vec::new(), &["a".to_owned()]);
        assert_eq!(eg(connectors.version()), Some((5, 4)));
        assert!(connectors.get("a").is_none());
        assert_eq!(eg(pinned.version()), Some((5, 3)));
        assert!(pinned.get("a").is_some());
        assert!(live.apply_set(6, vec![hive("b")]).await.applied);
        assert_eq!(eg(connectors.version()), Some((5, 6)));
        assert_eq!(live.version(), (5, 6));
    }

    /// Two applies of sets that differ in one catalog: the one that swaps
    /// second rebuilds what the first changed under it.
    #[tokio::test(start_paused = true)]
    async fn an_apply_rebuilds_what_another_swapped_in_meanwhile() {
        let live = slow_live(&[("b", 30)]);
        live.install(hive("a"), fake_built());
        live.note_coordinator_epoch(7);
        // Generations 2 and 1 of epoch 7: both newer than the held 0.
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
        // The older generation swaps in first and changes a.
        live.apply_remote(arneb_rpc::CatalogSet {
            epoch: 7,
            generation: 1,
            catalogs: vec![serde_json::to_value(&changed_a).unwrap()],
            default_catalog: None,
            default_schema: None,
        })
        .await;
        assert_eq!((live.epoch(), live.generation()), (7, 1));
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
                ..Default::default()
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
