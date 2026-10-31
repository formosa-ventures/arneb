//! Worker cordons (manage-api D8): persisted before they are applied, and
//! restored into the coordinator's node registry at startup.

use serde::{Deserialize, Serialize};

use crate::file::{StateError, StateStore};

/// How a worker was taken out of scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CordonKind {
    /// No new queries.
    Cordon,
    /// No new queries; running ones are watched until none is left.
    Drain,
}

/// A stored cordon on one worker id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CordonRecord {
    /// The cordoned worker's id (registered or not).
    pub worker_id: String,
    /// Cordon or drain.
    pub kind: CordonKind,
    /// When it was set, milliseconds since the Unix epoch.
    pub since_ms: u64,
    /// The user who set it.
    pub by: String,
}

impl StateStore {
    /// Every stored cordon.
    pub fn cordons(&self) -> Vec<CordonRecord> {
        self.snapshot().cordons.clone()
    }

    /// Stores `record`, replacing any cordon on the same `worker_id`.
    pub fn set_cordon(&self, record: CordonRecord) -> Result<(), StateError> {
        self.mutate(|d| {
            match d
                .cordons
                .iter_mut()
                .find(|c| c.worker_id == record.worker_id)
            {
                Some(stored) => *stored = record,
                None => d.cordons.push(record),
            }
            Ok(())
        })
    }

    /// Removes the cordon on `worker_id`; whether there was one.
    pub fn clear_cordon(&self, worker_id: &str) -> Result<bool, StateError> {
        self.mutate(|d| {
            let before = d.cordons.len();
            d.cordons.retain(|c| c.worker_id != worker_id);
            Ok(d.cordons.len() != before)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(worker_id: &str, kind: CordonKind) -> CordonRecord {
        CordonRecord {
            worker_id: worker_id.into(),
            kind,
            since_ms: 1,
            by: "root".into(),
        }
    }

    #[test]
    fn cordons_survive_reopen_and_clear() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        store.set_cordon(record("w1", CordonKind::Cordon)).unwrap();
        store.set_cordon(record("w2", CordonKind::Cordon)).unwrap();
        store.set_cordon(record("w1", CordonKind::Drain)).unwrap();
        assert!(store.clear_cordon("w2").unwrap());
        assert!(!store.clear_cordon("w2").unwrap());
        drop(store);

        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.cordons(), vec![record("w1", CordonKind::Drain)]);
    }

    #[test]
    fn a_state_file_without_cordons_loads_with_none() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        std::fs::write(&p, r#"{"version":1,"users":[],"tokens":[]}"#).unwrap();
        let store = StateStore::open(Some(&p)).unwrap();
        assert!(store.cordons().is_empty());
    }
}
