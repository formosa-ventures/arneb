//! Node registry: tracks known workers in the cluster.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
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
}

/// Thread-safe registry of worker nodes.
#[derive(Clone)]
pub struct NodeRegistry {
    workers: Arc<RwLock<HashMap<String, WorkerInfo>>>,
    /// Heartbeat timeout in seconds. Workers not heard from within this
    /// duration are considered dead.
    heartbeat_timeout_secs: u64,
}

impl NodeRegistry {
    /// Creates an empty registry with the given heartbeat timeout.
    pub fn new(heartbeat_timeout_secs: u64) -> Self {
        Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_timeout_secs,
        }
    }

    /// Register or update a worker's heartbeat.
    pub fn heartbeat(&self, worker_id: String, address: String, max_splits: usize) {
        let mut workers = self.workers.write().unwrap();
        let entry = workers.entry(worker_id.clone()).or_insert_with(|| {
            tracing::info!(worker_id = %worker_id, address = %address, "new worker registered");
            WorkerInfo {
                worker_id: worker_id.clone(),
                address: address.clone(),
                last_heartbeat: Instant::now(),
                max_splits,
                alive: true,
            }
        });
        entry.last_heartbeat = Instant::now();
        entry.address = address;
        entry.max_splits = max_splits;
        entry.alive = true;
    }

    /// Returns all alive workers.
    pub fn alive_workers(&self) -> Vec<WorkerInfo> {
        let mut workers = self.workers.write().unwrap();
        let timeout = std::time::Duration::from_secs(self.heartbeat_timeout_secs);
        let now = Instant::now();

        for worker in workers.values_mut() {
            if now.duration_since(worker.last_heartbeat) > timeout && worker.alive {
                tracing::warn!(
                    worker_id = %worker.worker_id,
                    "worker heartbeat timeout, marking as dead"
                );
                worker.alive = false;
            }
        }

        workers.values().filter(|w| w.alive).cloned().collect()
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
    fn remove_worker() {
        let registry = NodeRegistry::new(30);
        registry.heartbeat("w1".into(), "host1:9090".into(), 256);
        assert_eq!(registry.alive_count(), 1);

        registry.remove_worker("w1");
        assert_eq!(registry.total_count(), 0);
    }
}
