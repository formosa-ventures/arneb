//! The re-seed as one write (runtime-config D2): users, access rules,
//! settings and catalogs are replaced with the config file's in a single
//! [`StateStore`] mutation, so a crash or a failed write never leaves the
//! state half re-seeded.

use arneb_protocol::ScramVerifier;

use crate::file::{StateError, StateStore};
use crate::users::SeedReport;

/// What [`StateStore::reseed`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReseedOutcome {
    /// What user seeding (with `reseed`) did.
    pub users: SeedReport,
    /// The catalog generation after the re-seed.
    pub catalog_generation: u64,
}

impl StateStore {
    /// Replaces, in one atomic write, the config users (as
    /// [`seed_users`](Self::seed_users) with `reseed`, tombstones cleared),
    /// the access rules (as [`seed_rules`](Self::seed_rules) with `reseed`;
    /// `None`: no rules), the stored settings (cleared) and the catalogs (as
    /// [`seed_catalogs`](Self::seed_catalogs) with `reseed`). On an error
    /// nothing changes, on disk or in memory. Logs the user changes like
    /// `seed_users`.
    pub fn reseed(
        &self,
        users: &[(String, ScramVerifier)],
        rules_json: Option<&str>,
        catalogs: &[(String, serde_json::Value)],
    ) -> Result<ReseedOutcome, StateError> {
        let outcome = self.mutate(|d| {
            let users = crate::users::apply_seed(d, users, true);
            crate::rules::apply_rules_seed(d, rules_json);
            d.settings = Default::default();
            let catalog_generation = crate::catalogs::apply_catalog_seed(d, catalogs);
            Ok(ReseedOutcome {
                users,
                catalog_generation,
            })
        })?;
        crate::users::log_seed_report(&outcome.users);
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AdmissionLimits, StoredRules};
    use serde_json::json;

    fn v(pw: &str) -> ScramVerifier {
        ScramVerifier::from_password(pw, b"0123456789abcdef", 4096)
    }

    /// A state with a runtime user, runtime rules, stored settings and a
    /// runtime catalog.
    fn changed_state(path: &std::path::Path) -> StateStore {
        let store = StateStore::open(Some(path)).unwrap();
        store
            .seed_users(&[("alice".into(), v("a"))], false)
            .unwrap();
        store.seed_rules(Some("{}"), false).unwrap();
        store.seed_catalogs(&[], false).unwrap();
        store
            .set_access_rules(Some(StoredRules {
                json: r#"{"catalogs":[]}"#.into(),
                from_config: false,
            }))
            .unwrap();
        store
            .set_admission_limits(AdmissionLimits {
                max_running_queries: 1,
                max_queued_queries: 2,
            })
            .unwrap();
        store.set_slow_query_threshold_ms(5).unwrap();
        store
            .put_catalog(crate::CatalogRecord {
                name: "rt".into(),
                spec: json!({"name": "rt"}),
                from_config: false,
            })
            .unwrap();
        store
    }

    #[test]
    fn reseed_replaces_everything_in_one_write() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = changed_state(&p);
        let before = store.catalogs().0;
        let out = store
            .reseed(
                &[("bob".into(), v("b"))],
                Some("{}"),
                &[("lake".into(), json!({"name": "lake"}))],
            )
            .unwrap();
        assert_eq!(out.users.added, vec!["bob"]);
        assert_eq!(out.users.removed, vec!["alice"]);
        assert_eq!(out.catalog_generation, before + 1);
        drop(store);
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(
            store
                .list_users()
                .iter()
                .map(|u| u.name.as_str())
                .collect::<Vec<_>>(),
            vec!["bob"]
        );
        assert_eq!(store.access_rules().unwrap().json, "{}");
        assert!(!store.rules_changed_at_runtime());
        assert_eq!(store.admission_limits(), None);
        assert_eq!(store.slow_query_threshold_ms(), None);
        let (generation, catalogs) = store.catalogs();
        assert_eq!(generation, before + 1);
        assert_eq!(catalogs.len(), 1);
        assert!(catalogs[0].from_config && catalogs[0].name == "lake");
    }

    #[test]
    fn a_failed_reseed_write_changes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = changed_state(&p);
        let on_disk = std::fs::read(&p).unwrap();
        let users = store.list_users();
        let catalogs = store.catalogs();
        store.set_fail_writes(true);
        assert!(store.reseed(&[("bob".into(), v("b"))], None, &[]).is_err());
        store.set_fail_writes(false);
        assert_eq!(std::fs::read(&p).unwrap(), on_disk, "nothing persisted");
        assert_eq!(store.list_users(), users, "nothing swapped in");
        assert_eq!(store.catalogs(), catalogs);
        assert!(store.rules_changed_at_runtime());
        assert_eq!(store.slow_query_threshold_ms(), Some(5));
    }
}
