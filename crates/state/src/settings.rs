//! Runtime settings changed through the manage API and kept across restarts
//! (manage-api D9, runtime-config D9).

use serde::{Deserialize, Serialize};

use crate::file::{StateError, StateStore};

/// Admission limits set at runtime; a stored value wins over the config seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionLimits {
    /// Cap on running queries; `0` means unlimited.
    pub max_running_queries: usize,
    /// Cap on queued queries.
    pub max_queued_queries: usize,
}

/// Everything under `settings` in the state file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Settings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<AdmissionLimits>,
    /// The slow-query threshold in ms (`0` = log off), set at runtime
    /// (runtime-config D9).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slow_query_threshold_ms: Option<u64>,
}

impl StateStore {
    /// The admission limits set at runtime, or `None` when none were set.
    pub fn admission_limits(&self) -> Option<AdmissionLimits> {
        self.snapshot().settings.admission
    }

    /// Stores the admission limits, replacing any stored ones.
    pub fn set_admission_limits(&self, limits: AdmissionLimits) -> Result<(), StateError> {
        self.mutate(|d| {
            d.settings.admission = Some(limits);
            Ok(())
        })
    }

    /// The slow-query threshold set at runtime, in ms (`0` = log off), or
    /// `None` when none was set.
    pub fn slow_query_threshold_ms(&self) -> Option<u64> {
        self.snapshot().settings.slow_query_threshold_ms
    }

    /// Stores the slow-query threshold, in ms (`0` = log off).
    pub fn set_slow_query_threshold_ms(&self, ms: u64) -> Result<(), StateError> {
        self.mutate(|d| {
            d.settings.slow_query_threshold_ms = Some(ms);
            Ok(())
        })
    }

    /// Clears every stored setting, so the config values apply (a re-seed,
    /// runtime-config D2).
    pub fn clear_settings(&self) -> Result<(), StateError> {
        self.mutate(|d| {
            d.settings = Settings::default();
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_protocol::ScramVerifier;

    #[test]
    fn admission_limits_survive_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.admission_limits(), None);
        let limits = AdmissionLimits {
            max_running_queries: 2,
            max_queued_queries: 7,
        };
        store.set_admission_limits(limits).unwrap();
        // A reseed keeps the settings.
        let v = ScramVerifier::from_password("pw", b"0123456789abcdef", 4096);
        store.seed_users(&[("alice".into(), v)], true).unwrap();
        drop(store);

        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.admission_limits(), Some(limits));
    }

    #[test]
    fn slow_query_threshold_survives_reopen_and_clear_settings_resets_all() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.slow_query_threshold_ms(), None);
        store.set_slow_query_threshold_ms(0).unwrap();
        store
            .set_admission_limits(AdmissionLimits {
                max_running_queries: 1,
                max_queued_queries: 1,
            })
            .unwrap();
        drop(store);

        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.slow_query_threshold_ms(), Some(0));
        store.clear_settings().unwrap();
        assert_eq!(store.slow_query_threshold_ms(), None);
        assert_eq!(store.admission_limits(), None);
        drop(store);
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.slow_query_threshold_ms(), None);
        assert_eq!(store.admission_limits(), None);
    }

    #[test]
    fn a_state_file_without_settings_loads_with_none() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        std::fs::write(&p, r#"{"version":1,"users":[],"tokens":[]}"#).unwrap();
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.admission_limits(), None);
    }
}
