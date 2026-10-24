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
        }
    }

    /// Register or update a worker that reported `version`, if it is this
    /// coordinator's release, recording whether it is `draining`. Otherwise
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
            self.heartbeat(worker_id.clone(), address, max_splits);
            self.set_draining(&worker_id, draining);
            return Ok(());
        }
        let message = format!(
            "worker '{worker_id}' runs arneb {version} but the coordinator runs {}; \
             every node must run the same release",
            self.version
        );
        let removed = self.workers.write().unwrap().remove(&worker_id).is_some();
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

    /// Mark a registered worker draining (or active again), logging the change.
    fn set_draining(&self, worker_id: &str, draining: bool) {
        let mut workers = self.workers.write().unwrap();
        let Some(worker) = workers.get_mut(worker_id) else {
            return;
        };
        if worker.draining != draining {
            tracing::info!(
                target: "arneb::cluster",
                worker_id = %worker_id,
                draining,
                "{}",
                if draining { "worker draining" } else { "worker active again" }
            );
        }
        worker.draining = draining;
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

    /// Register or update a worker's heartbeat, taking it to run this
    /// coordinator's release (see [`NodeRegistry::admit`]).
    pub fn heartbeat(&self, worker_id: String, address: String, max_splits: usize) {
        self.departed.lock().unwrap().remove(&worker_id);
        let mut workers = self.workers.write().unwrap();
        let entry = workers.entry(worker_id.clone()).or_insert_with(|| {
            tracing::info!(worker_id = %worker_id, address = %address, "new worker registered");
            WorkerInfo {
                worker_id: worker_id.clone(),
                address: address.clone(),
                last_heartbeat: Instant::now(),
                max_splits,
                alive: true,
                version: self.version.to_string(),
                draining: false,
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
        entry.last_heartbeat = Instant::now();
        entry.address = address;
        entry.max_splits = max_splits;
        entry.alive = true;
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

    /// Alive workers that take new queries (not draining).
    pub fn active_workers(&self) -> Vec<WorkerInfo> {
        let mut workers = self.workers.write().unwrap();
        self.expire_silent(&mut workers);
        workers
            .values()
            .filter(|w| w.alive && !w.draining)
            .cloned()
            .collect()
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

    /// Returns the total number of registered workers (alive or dead).
    pub fn total_count(&self) -> usize {
        self.workers.read().unwrap().len()
    }

    /// Returns the number of alive workers.
    pub fn alive_count(&self) -> usize {
        self.alive_workers().len()
    }

    /// Remove a worker from the registry.
    pub fn remove_worker(&self, worker_id: &str) {
        self.workers.write().unwrap().remove(worker_id);
    }
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

    #[test]
    fn an_unregistered_worker_is_lost() {
        let registry = NodeRegistry::new(30);
        let lost = registry.lost_worker(&["gone".into()]).unwrap();
        assert_eq!(lost.reason, "no longer registered");
    }
}
