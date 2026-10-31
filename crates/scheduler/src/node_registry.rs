//! Node registry: tracks known workers in the cluster.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

/// Information about a registered worker node.
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    /// Unique worker identifier.
    pub worker_id: String,
    /// Network address (host:port) for RPC communication.
    pub address: String,
    /// Last heartbeat timestamp.
    pub last_heartbeat: Instant,
    /// Maximum number of splits this worker can handle concurrently.
    pub max_splits: usize,
    /// Whether the worker is considered alive.
    pub alive: bool,
    /// The worker's release version (always the coordinator's own, since
    /// [`NodeRegistry::admit`] refuses any other).
    pub version: String,
    /// Shutting down: alive for the queries it runs, but not scheduled.
    pub draining: bool,
}

/// A worker [`NodeRegistry::lost_worker`] found lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostWorker {
    /// The worker's id.
    pub worker_id: String,
    /// Its last advertised Flight address (empty when unknown).
    pub address: String,
    /// Why it counts as lost.
    pub reason: String,
}

/// How an operator took a worker out of scheduling (manage-api D8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CordonMode {
    /// No new queries; nothing else is tracked.
    Cordon,
    /// No new queries, and the queries it runs are watched until none is left.
    Drain,
}

/// A worker's coordinator-side scheduling state, separate from the
/// lifecycle `draining` flag the worker itself reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingState {
    /// Takes new queries.
    Schedulable,
    /// Cordoned: takes no new queries.
    Cordoned,
    /// Drain requested; distributed queries still run on it.
    Draining,
    /// Drain requested; no distributed query runs on it.
    Drained,
}

impl SchedulingState {
    /// `schedulable`, `cordoned`, `draining` or `drained`.
    pub fn as_str(self) -> &'static str {
        match self {
            SchedulingState::Schedulable => "schedulable",
            SchedulingState::Cordoned => "cordoned",
            SchedulingState::Draining => "draining",
            SchedulingState::Drained => "drained",
        }
    }
}

/// Cordons and in-flight counts, under one lock so a selection and its
/// increments never interleave with a cordon change.
#[derive(Debug, Default)]
struct Scheduling {
    cordons: HashMap<String, CordonMode>,
    /// Distributed queries holding an [`InFlightGuard`] over each worker.
    in_flight: HashMap<String, usize>,
}

fn lock(scheduling: &Mutex<Scheduling>) -> std::sync::MutexGuard<'_, Scheduling> {
    scheduling.lock().unwrap_or_else(|p| p.into_inner())
}

/// Counts one distributed query on each worker it was given by
/// [`NodeRegistry::schedule_workers`]; dropping it un-counts them.
#[must_use = "dropping the guard ends the in-flight count at once"]
pub struct InFlightGuard {
    scheduling: Arc<Mutex<Scheduling>>,
    worker_ids: Vec<String>,
}

impl std::fmt::Debug for InFlightGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InFlightGuard")
            .field("worker_ids", &self.worker_ids)
            .finish()
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let mut scheduling = lock(&self.scheduling);
        for id in &self.worker_ids {
            if let Some(count) = scheduling.in_flight.get_mut(id) {
                *count -= 1;
                if *count == 0 {
                    scheduling.in_flight.remove(id);
                }
            }
        }
    }
}

/// Thread-safe registry of worker nodes.
#[derive(Clone)]
pub struct NodeRegistry {
    workers: Arc<RwLock<HashMap<String, WorkerInfo>>>,
    /// Heartbeat timeout in seconds. Workers not heard from within this
    /// duration are considered dead.
    heartbeat_timeout_secs: u64,
    /// The release this coordinator runs; only workers on it are admitted.
    version: &'static str,
    /// Last refused version per worker id, so each refusal is logged once.
    rejected: Arc<Mutex<HashMap<String, String>>>,
    /// Address of each worker that deregistered after a drain, so a query
    /// it still ran reports why it is gone. Cleared when the id rejoins.
    departed: Arc<Mutex<HashMap<String, String>>>,
    /// Cordons and in-flight counts. Lock order: `workers`, then this.
    scheduling: Arc<Mutex<Scheduling>>,
}

impl NodeRegistry {
    /// Creates an empty registry with the given heartbeat timeout.
    pub fn new(heartbeat_timeout_secs: u64) -> Self {
        Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_timeout_secs,
            version: env!("CARGO_PKG_VERSION"),
            rejected: Arc::new(Mutex::new(HashMap::new())),
            departed: Arc::new(Mutex::new(HashMap::new())),
            scheduling: Arc::new(Mutex::new(Scheduling::default())),
        }
    }

    /// Register or update a worker that reported `version`, if it is this
    /// coordinator's release, recording whether it is `draining`. A draining
    /// heartbeat from a worker that already deregistered is a late one and
    /// is ignored; an active one is a restart and rejoins. Otherwise
    /// the worker is removed if registered, the refusal is logged once per
    /// worker and version, and the returned error explains it.
    pub fn admit(
        &self,
        worker_id: String,
        address: String,
        max_splits: usize,
        version: &str,
        draining: bool,
    ) -> Result<(), String> {
        if version == self.version {
            self.rejected.lock().unwrap().remove(&worker_id);
            self.record(worker_id, address, max_splits, draining);
            return Ok(());
        }
        let message = format!(
            "worker '{worker_id}' runs arneb {version} but the coordinator runs {}; \
             every node must run the same release",
            self.version
        );
        let removed = self.remove_worker(&worker_id);
        if self.first_rejection(&worker_id, version) {
            tracing::warn!(
                target: "arneb::cluster",
                worker_id = %worker_id,
                address = %address,
                version,
                coordinator_version = self.version,
                removed,
                "worker rejected: version mismatch"
            );
        }
        Err(message)
    }

    /// A drained worker said it is exiting: forget it.
    pub fn deregister(&self, worker_id: &str) {
        if let Some(worker) = self.workers.write().unwrap().remove(worker_id) {
            tracing::info!(target: "arneb::cluster", worker_id = %worker_id, "worker deregistered");
            self.departed
                .lock()
                .unwrap()
                .insert(worker.worker_id, worker.address);
        }
    }

    /// Record that `worker_id` was refused on `version`; true unless it was
    /// already refused on that same version.
    fn first_rejection(&self, worker_id: &str, version: &str) -> bool {
        let previous = self
            .rejected
            .lock()
            .unwrap()
            .insert(worker_id.to_owned(), version.to_owned());
        previous.as_deref() != Some(version)
    }

    /// Register or update an active worker's heartbeat, taking it to run
    /// this coordinator's release (see [`NodeRegistry::admit`]).
    pub fn heartbeat(&self, worker_id: String, address: String, max_splits: usize) {
        self.record(worker_id, address, max_splits, false);
    }

    fn record(&self, worker_id: String, address: String, max_splits: usize, draining: bool) {
        // One write lock for liveness and drain state, so no reader sees a
        // draining worker as active in between. Lock order: workers, then
        // departed (as in `deregister` and `lost_worker`).
        let mut workers = self.workers.write().unwrap();
        {
            let mut departed = self.departed.lock().unwrap();
            if draining && departed.contains_key(&worker_id) {
                tracing::debug!(
                    target: "arneb::cluster",
                    worker_id = %worker_id,
                    "ignoring a late heartbeat from a deregistered worker"
                );
                return;
            }
            departed.remove(&worker_id);
        }
        let entry = workers.entry(worker_id.clone()).or_insert_with(|| {
            tracing::info!(worker_id = %worker_id, address = %address, "new worker registered");
            WorkerInfo {
                worker_id: worker_id.clone(),
                address: address.clone(),
                last_heartbeat: Instant::now(),
                max_splits,
                alive: true,
                version: self.version.to_string(),
                draining,
            }
        });
        if !entry.alive {
            tracing::info!(
                target: "arneb::cluster",
                worker_id = %worker_id,
                address = %address,
                "worker re-registered"
            );
        }
        if entry.draining != draining {
            tracing::info!(
                target: "arneb::cluster",
                worker_id = %worker_id,
                draining,
                "{}",
                if draining { "worker draining" } else { "worker active again" }
            );
        }
        entry.last_heartbeat = Instant::now();
        entry.address = address;
        entry.max_splits = max_splits;
        entry.alive = true;
        entry.draining = draining;
    }

    /// The heartbeat timeout this registry applies.
    pub fn heartbeat_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.heartbeat_timeout_secs)
    }

    /// The first of `worker_ids` that is lost: marked dead, past its
    /// heartbeat deadline, or no longer registered. A worker past its
    /// deadline is marked dead as a side effect.
    pub fn lost_worker(&self, worker_ids: &[String]) -> Option<LostWorker> {
        let timeout = self.heartbeat_timeout();
        let now = Instant::now();
        let mut workers = self.workers.write().unwrap();
        for id in worker_ids {
            let Some(worker) = workers.get_mut(id) else {
                let departed = self.departed.lock().unwrap().get(id).cloned();
                let (address, reason) = match departed {
                    Some(address) => (address, "left the cluster when its drain ended"),
                    None => (String::new(), "no longer registered"),
                };
                return Some(LostWorker {
                    worker_id: id.clone(),
                    address,
                    reason: reason.to_string(),
                });
            };
            expire(worker, now, timeout);
            if !worker.alive {
                let silent = now.duration_since(worker.last_heartbeat);
                let reason = if silent > timeout {
                    format!("no heartbeat for {} s", silent.as_secs())
                } else {
                    "marked dead after a transport failure".to_string()
                };
                return Some(LostWorker {
                    worker_id: worker.worker_id.clone(),
                    address: worker.address.clone(),
                    reason,
                });
            }
        }
        None
    }

    /// Mark a worker dead after a transport failure, until its next
    /// heartbeat. Returns whether it was alive.
    pub fn mark_dead(&self, worker_id: &str, reason: &str) -> bool {
        let mut workers = self.workers.write().unwrap();
        let Some(worker) = workers.get_mut(worker_id) else {
            return false;
        };
        let was_alive = worker.alive;
        if was_alive {
            tracing::warn!(
                target: "arneb::cluster",
                worker_id = %worker_id,
                reason,
                "worker marked dead"
            );
        }
        worker.alive = false;
        was_alive
    }

    /// Returns all alive workers, draining ones included.
    pub fn alive_workers(&self) -> Vec<WorkerInfo> {
        let mut workers = self.workers.write().unwrap();
        self.expire_silent(&mut workers);
        workers.values().filter(|w| w.alive).cloned().collect()
    }

    /// Alive workers that take new queries: neither draining (lifecycle)
    /// nor cordoned (scheduling).
    pub fn active_workers(&self) -> Vec<WorkerInfo> {
        let mut workers = self.workers.write().unwrap();
        self.expire_silent(&mut workers);
        let scheduling = lock(&self.scheduling);
        schedulable(&workers, &scheduling).cloned().collect()
    }

    /// [`Self::active_workers`], each counted as running one more
    /// distributed query until the returned guard drops. Selection and
    /// counting happen under the cordon lock, so once
    /// [`Self::set_cordon`] returns no later selection includes that worker.
    pub fn schedule_workers(&self) -> (Vec<WorkerInfo>, InFlightGuard) {
        let mut workers = self.workers.write().unwrap();
        self.expire_silent(&mut workers);
        let mut scheduling = lock(&self.scheduling);
        let picked: Vec<WorkerInfo> = schedulable(&workers, &scheduling).cloned().collect();
        let worker_ids: Vec<String> = picked.iter().map(|w| w.worker_id.clone()).collect();
        for id in &worker_ids {
            *scheduling.in_flight.entry(id.clone()).or_default() += 1;
        }
        drop(scheduling);
        let guard = InFlightGuard {
            scheduling: Arc::clone(&self.scheduling),
            worker_ids,
        };
        (picked, guard)
    }

    /// Cordon or drain `worker_id` (`Some`), or uncordon it (`None`). Any
    /// id is accepted, registered or not; a cordon outlives the worker's
    /// registration and applies again when it re-registers.
    pub fn set_cordon(&self, worker_id: &str, mode: Option<CordonMode>) {
        let mut scheduling = lock(&self.scheduling);
        let before = match mode {
            Some(mode) => scheduling.cordons.insert(worker_id.to_owned(), mode),
            None => scheduling.cordons.remove(worker_id),
        };
        drop(scheduling);
        if before != mode {
            tracing::info!(
                target: "arneb::cluster",
                worker_id = %worker_id,
                scheduling = match mode {
                    Some(CordonMode::Cordon) => "cordoned",
                    Some(CordonMode::Drain) => "draining",
                    None => "schedulable",
                },
                "worker scheduling changed"
            );
        }
    }

    /// Replace the cordons with `cordons` (the state store's, at startup).
    pub fn restore_cordons(&self, cordons: impl IntoIterator<Item = (String, CordonMode)>) {
        lock(&self.scheduling).cordons = cordons.into_iter().collect();
    }

    /// The cordon on `worker_id`, if any.
    pub fn cordon_of(&self, worker_id: &str) -> Option<CordonMode> {
        lock(&self.scheduling).cordons.get(worker_id).copied()
    }

    /// Every cordoned or draining id, registered or not; sorted.
    pub fn cordoned_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = lock(&self.scheduling).cordons.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// `worker_id`'s scheduling state.
    pub fn scheduling_state(&self, worker_id: &str) -> SchedulingState {
        let scheduling = lock(&self.scheduling);
        match scheduling.cordons.get(worker_id) {
            None => SchedulingState::Schedulable,
            Some(CordonMode::Cordon) => SchedulingState::Cordoned,
            Some(CordonMode::Drain) if scheduling.in_flight.contains_key(worker_id) => {
                SchedulingState::Draining
            }
            Some(CordonMode::Drain) => SchedulingState::Drained,
        }
    }

    /// Distributed queries running on `worker_id` (held guards over it).
    pub fn in_flight(&self, worker_id: &str) -> usize {
        lock(&self.scheduling)
            .in_flight
            .get(worker_id)
            .copied()
            .unwrap_or(0)
    }

    /// Whether `worker_id` is registered, alive or not.
    pub fn is_known(&self, worker_id: &str) -> bool {
        self.workers.read().unwrap().contains_key(worker_id)
    }

    /// Mark every worker silent past the heartbeat timeout as dead.
    fn expire_silent(&self, workers: &mut HashMap<String, WorkerInfo>) {
        let timeout = self.heartbeat_timeout();
        let now = Instant::now();
        for worker in workers.values_mut() {
            expire(worker, now, timeout);
        }
    }

    /// Every registered worker, alive or not, with liveness re-evaluated
    /// against the heartbeat deadline; sorted by worker id.
    pub fn all_workers(&self) -> Vec<WorkerInfo> {
        let mut workers = self.workers.write().unwrap();
        self.expire_silent(&mut workers);
        let mut all: Vec<WorkerInfo> = workers.values().cloned().collect();
        all.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
        all
    }

    /// Registered worker `worker_id`, alive or not, with liveness
    /// re-evaluated against the heartbeat deadline.
    pub fn worker(&self, worker_id: &str) -> Option<WorkerInfo> {
        let timeout = self.heartbeat_timeout();
        let mut workers = self.workers.write().unwrap();
        let worker = workers.get_mut(worker_id)?;
        expire(worker, Instant::now(), timeout);
        Some(worker.clone())
    }

    /// Returns the total number of registered workers (alive or dead).
    pub fn total_count(&self) -> usize {
        self.workers.read().unwrap().len()
    }

    /// Returns the number of alive workers.
    pub fn alive_count(&self) -> usize {
        self.alive_workers().len()
    }

    /// Remove a worker from the registry; `true` if it was registered.
    pub fn remove_worker(&self, worker_id: &str) -> bool {
        self.workers.write().unwrap().remove(worker_id).is_some()
    }
}

/// The workers that take new queries: alive, not draining, not cordoned.
fn schedulable<'a>(
    workers: &'a HashMap<String, WorkerInfo>,
    scheduling: &'a Scheduling,
) -> impl Iterator<Item = &'a WorkerInfo> + 'a {
    workers
        .values()
        .filter(|w| w.alive && !w.draining && !scheduling.cordons.contains_key(&w.worker_id))
}

/// Mark `worker` dead once it has been silent longer than `timeout`.
fn expire(worker: &mut WorkerInfo, now: Instant, timeout: std::time::Duration) {
    let silent = now.duration_since(worker.last_heartbeat);
    if worker.alive && silent > timeout {
        tracing::warn!(
            target: "arneb::cluster",
            worker_id = %worker.worker_id,
            silent_secs = silent.as_secs(),
            "worker heartbeat timeout, marking as dead"
        );
        worker.alive = false;
    }
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new(30) // 30 second timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_list_workers() {
        let registry = NodeRegistry::new(30);
        registry.heartbeat("w1".into(), "host1:9090".into(), 256);
        registry.heartbeat("w2".into(), "host2:9090".into(), 128);

        let alive = registry.alive_workers();
        assert_eq!(alive.len(), 2);
        assert_eq!(registry.total_count(), 2);
    }

    #[test]
    fn heartbeat_updates_existing() {
        let registry = NodeRegistry::new(30);
        registry.heartbeat("w1".into(), "host1:9090".into(), 256);
        registry.heartbeat("w1".into(), "host1:9091".into(), 512); // update address and capacity

        let alive = registry.alive_workers();
        assert_eq!(alive.len(), 1);
        assert_eq!(alive[0].address, "host1:9091");
        assert_eq!(alive[0].max_splits, 512);
    }

    #[test]
    fn admit_registers_a_worker_of_this_release() {
        let registry = NodeRegistry::new(30);
        let own = env!("CARGO_PKG_VERSION");
        registry
            .admit("w1".into(), "host1:9090".into(), 256, own, false)
            .unwrap();
        let alive = registry.alive_workers();
        assert_eq!(alive.len(), 1);
        assert_eq!(alive[0].version, own);
    }

    #[test]
    fn admit_refuses_another_release_and_drops_a_registered_worker() {
        let registry = NodeRegistry::new(30);
        registry.heartbeat("w1".into(), "host1:9090".into(), 256);
        let err = registry
            .admit("w1".into(), "host1:9090".into(), 256, "9.9.9", false)
            .unwrap_err();
        assert!(err.contains("9.9.9"), "{err}");
        assert!(err.contains(env!("CARGO_PKG_VERSION")), "{err}");
        assert_eq!(registry.total_count(), 0);
        assert!(registry.alive_workers().is_empty());
    }

    #[test]
    fn a_rejection_is_new_only_when_the_version_changes() {
        let registry = NodeRegistry::new(30);
        assert!(registry.first_rejection("w1", "9.9.8"));
        assert!(!registry.first_rejection("w1", "9.9.8"));
        assert!(registry.first_rejection("w1", "9.9.9"));
        assert!(registry.first_rejection("w2", "9.9.9"));
        // Admitting the worker clears its record, so a later refusal logs again.
        let own = env!("CARGO_PKG_VERSION");
        registry
            .admit("w1".into(), "host1:9090".into(), 256, own, false)
            .unwrap();
        assert!(registry.first_rejection("w1", "9.9.9"));
    }

    #[test]
    fn a_draining_worker_is_alive_but_not_active_until_it_says_otherwise() {
        let registry = NodeRegistry::new(30);
        let own = env!("CARGO_PKG_VERSION");
        registry.heartbeat("w1".into(), "host1:9090".into(), 256);
        registry
            .admit("w2".into(), "host2:9090".into(), 256, own, true)
            .unwrap();
        let mut active: Vec<_> = registry
            .active_workers()
            .into_iter()
            .map(|w| w.worker_id)
            .collect();
        active.sort();
        assert_eq!(active, vec!["w1"]);
        assert_eq!(registry.alive_count(), 2);
        assert_eq!(registry.lost_worker(&["w2".into()]), None);

        registry
            .admit("w2".into(), "host2:9090".into(), 256, own, false)
            .unwrap();
        assert_eq!(registry.active_workers().len(), 2);
    }

    #[test]
    fn deregister_forgets_the_worker() {
        let registry = NodeRegistry::new(30);
        registry.heartbeat("w1".into(), "host1:9090".into(), 256);
        registry.deregister("w1");
        registry.deregister("w1");
        assert_eq!(registry.total_count(), 0);
        let lost = registry.lost_worker(&["w1".into()]).unwrap();
        assert_eq!(lost.address, "host1:9090");
        assert_eq!(lost.reason, "left the cluster when its drain ended");

        registry.heartbeat("w1".into(), "host1:9090".into(), 256);
        registry.remove_worker("w1");
        let lost = registry.lost_worker(&["w1".into()]).unwrap();
        assert_eq!(lost.reason, "no longer registered");
    }

    #[test]
    fn a_late_draining_heartbeat_does_not_bring_a_deregistered_worker_back() {
        let registry = NodeRegistry::new(30);
        let own = env!("CARGO_PKG_VERSION");
        registry
            .admit("w1".into(), "host1:9090".into(), 256, own, true)
            .unwrap();
        registry.deregister("w1");
        registry
            .admit("w1".into(), "host1:9090".into(), 256, own, true)
            .unwrap();
        assert_eq!(registry.total_count(), 0);

        // A restart under the same id comes back active and rejoins.
        registry
            .admit("w1".into(), "host1:9090".into(), 256, own, false)
            .unwrap();
        assert_eq!(registry.active_workers().len(), 1);
    }

    #[test]
    fn remove_worker() {
        let registry = NodeRegistry::new(30);
        registry.heartbeat("w1".into(), "host1:9090".into(), 256);
        assert_eq!(registry.alive_count(), 1);

        registry.remove_worker("w1");
        assert_eq!(registry.total_count(), 0);
    }

    #[test]
    fn a_silent_worker_is_lost_and_its_heartbeat_brings_it_back() {
        let registry = NodeRegistry::new(10);
        registry.heartbeat("w1".into(), "host1:9090".into(), 256);
        registry
            .workers
            .write()
            .unwrap()
            .get_mut("w1")
            .unwrap()
            .last_heartbeat = Instant::now() - std::time::Duration::from_secs(11);
        let lost = registry.lost_worker(&["w1".into()]).unwrap();
        assert_eq!(lost.worker_id, "w1");
        assert_eq!(lost.address, "host1:9090");
        assert!(
            lost.reason.starts_with("no heartbeat for"),
            "{}",
            lost.reason
        );
        assert_eq!(registry.alive_count(), 0);

        let fresh = NodeRegistry::new(30);
        fresh.heartbeat("w1".into(), "host1:9090".into(), 256);
        assert!(fresh.lost_worker(&["w1".into()]).is_none());
    }

    #[test]
    fn mark_dead_excludes_a_worker_until_its_next_heartbeat() {
        let registry = NodeRegistry::new(30);
        registry.heartbeat("w1".into(), "host1:9090".into(), 256);
        registry.heartbeat("w2".into(), "host2:9090".into(), 256);
        assert!(registry.mark_dead("w2", "connect refused"));
        assert!(!registry.mark_dead("w2", "again"));
        assert_eq!(
            registry.lost_worker(&["w2".into()]).unwrap().reason,
            "marked dead after a transport failure"
        );
        assert_eq!(
            registry
                .lost_worker(&["w1".into(), "w2".into()])
                .unwrap()
                .worker_id,
            "w2"
        );
        let alive: Vec<_> = registry
            .alive_workers()
            .into_iter()
            .map(|w| w.worker_id)
            .collect();
        assert_eq!(alive, vec!["w1".to_string()]);

        registry.heartbeat("w2".into(), "host2:9090".into(), 256);
        assert_eq!(registry.alive_count(), 2);
        assert!(registry.lost_worker(&["w2".into()]).is_none());
    }

    fn ids(workers: Vec<WorkerInfo>) -> Vec<String> {
        let mut ids: Vec<String> = workers.into_iter().map(|w| w.worker_id).collect();
        ids.sort();
        ids
    }

    fn two_workers() -> NodeRegistry {
        let registry = NodeRegistry::new(30);
        registry.heartbeat("w1".into(), "host1:9090".into(), 256);
        registry.heartbeat("w2".into(), "host2:9090".into(), 256);
        registry
    }

    #[test]
    fn cordoned_worker_is_alive_but_not_scheduled() {
        let registry = two_workers();
        registry.set_cordon("w1", Some(CordonMode::Cordon));
        assert_eq!(ids(registry.active_workers()), vec!["w2"]);
        assert_eq!(ids(registry.alive_workers()), vec!["w1", "w2"]);
        assert_eq!(registry.scheduling_state("w1"), SchedulingState::Cordoned);
        assert_eq!(
            registry.scheduling_state("w2"),
            SchedulingState::Schedulable
        );
        let (picked, _guard) = registry.schedule_workers();
        assert_eq!(ids(picked), vec!["w2"]);
        assert_eq!(registry.cordoned_ids(), vec!["w1"]);

        registry.set_cordon("w1", None);
        assert_eq!(ids(registry.active_workers()), vec!["w1", "w2"]);
        assert_eq!(registry.cordon_of("w1"), None);
    }

    #[test]
    fn drain_goes_draining_then_drained() {
        let registry = two_workers();
        let (picked, guard) = registry.schedule_workers();
        assert_eq!(ids(picked), vec!["w1", "w2"]);
        assert_eq!(registry.in_flight("w1"), 1);
        registry.set_cordon("w1", Some(CordonMode::Drain));
        assert_eq!(registry.scheduling_state("w1"), SchedulingState::Draining);
        drop(guard);
        assert_eq!(registry.scheduling_state("w1"), SchedulingState::Drained);
        assert_eq!(registry.in_flight("w1"), 0);
        assert_eq!(registry.in_flight("w2"), 0);
    }

    #[test]
    fn no_query_is_scheduled_onto_a_worker_cordoned_meanwhile() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let registry = two_workers();
        let cordoned = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let schedulers: Vec<_> = (0..8)
            .map(|_| {
                let (registry, cordoned, stop) =
                    (registry.clone(), Arc::clone(&cordoned), Arc::clone(&stop));
                std::thread::spawn(move || {
                    let mut held = Vec::new();
                    let mut late = 0usize;
                    while !stop.load(Ordering::SeqCst) {
                        let after_cordon = cordoned.load(Ordering::SeqCst);
                        let (picked, guard) = registry.schedule_workers();
                        if after_cordon {
                            late += 1;
                            assert!(
                                picked.iter().all(|w| w.worker_id != "w1"),
                                "a selection after the cordon included w1"
                            );
                        }
                        held.push(guard);
                        if held.len() > 4 {
                            drop(held.remove(0));
                        }
                    }
                    late
                })
            })
            .collect();
        std::thread::sleep(std::time::Duration::from_millis(20));
        registry.set_cordon("w1", Some(CordonMode::Drain));
        cordoned.store(true, Ordering::SeqCst);
        let mut last = registry.in_flight("w1");
        for _ in 0..2000 {
            let now = registry.in_flight("w1");
            assert!(
                now <= last,
                "w1's in-flight count rose from {last} to {now}"
            );
            last = now;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        stop.store(true, Ordering::SeqCst);
        let late: usize = schedulers.into_iter().map(|t| t.join().unwrap()).sum();
        assert!(late > 0, "no selection ran after the cordon");
        assert_eq!(registry.in_flight("w1"), 0);
        assert_eq!(registry.scheduling_state("w1"), SchedulingState::Drained);
    }

    #[test]
    fn restored_cordon_applies_when_the_worker_registers() {
        let registry = NodeRegistry::new(30);
        registry.restore_cordons([("w9".to_string(), CordonMode::Cordon)]);
        assert!(!registry.is_known("w9"));
        assert_eq!(registry.scheduling_state("w9"), SchedulingState::Cordoned);
        registry.heartbeat("w9".into(), "host9:9090".into(), 256);
        assert!(registry.is_known("w9"));
        assert_eq!(registry.worker("w9").unwrap().address, "host9:9090");
        assert!(registry.worker("w8").is_none());
        assert!(registry.active_workers().is_empty());
        assert_eq!(ids(registry.alive_workers()), vec!["w9"]);
    }

    #[test]
    fn an_unregistered_worker_is_lost() {
        let registry = NodeRegistry::new(30);
        let lost = registry.lost_worker(&["gone".into()]).unwrap();
        assert_eq!(lost.reason, "no longer registered");
    }
}
