//! The access rules kept in the state store (runtime-config D1, D3): seeded
//! from the `[access_control] rules_file` on the first start, changed at
//! runtime through `/api/v2/access-rules`, and winning over the file on
//! every later start.

use serde::{Deserialize, Serialize};

use crate::file::{StateError, StateStore};

/// The stored access rules: the rules file's JSON text as it was seeded or
/// set. Validated before it is stored, so it always parses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRules {
    /// The rules JSON text.
    pub json: String,
    /// True when it was copied from the config's rules file.
    pub from_config: bool,
}

/// Which config objects the state was seeded with. Absent in state files
/// written before runtime config existed, which load as nothing seeded.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Seeded {
    /// `[[catalogs]]` were copied into the state.
    #[serde(default)]
    pub catalogs: bool,
    /// The rules file was copied into the state.
    #[serde(default)]
    pub rules: bool,
}

/// What [`StateStore::seed_rules`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RulesSeed {
    /// The config's rules were stored (first start, or a re-seed).
    Seeded,
    /// The stored rules equal the config's.
    Unchanged,
    /// The stored rules differ from the config's and were kept.
    Diverged,
}

impl StateStore {
    /// The stored access rules; `None` means no rules (allow everything).
    pub fn access_rules(&self) -> Option<StoredRules> {
        self.snapshot().rules.clone()
    }

    /// Stores `rules`, replacing the stored ones (`None` clears them).
    pub fn set_access_rules(&self, rules: Option<StoredRules>) -> Result<(), StateError> {
        self.mutate(|d| {
            d.rules = rules;
            d.seeded.rules = true;
            Ok(())
        })
    }

    /// Which config objects the state was seeded with.
    pub fn seeded(&self) -> Seeded {
        self.snapshot().seeded
    }

    /// Seeds the rules from the config's rules file text (`None`: no rules
    /// file). The first call on a state that was never seeded, or any call
    /// with `reseed`, stores it with `from_config = true` (or clears the
    /// rules) and returns [`RulesSeed::Seeded`]. Otherwise the stored rules
    /// are kept, and the result says whether their text equals the
    /// config's. The caller validates `config_json` first.
    pub fn seed_rules(
        &self,
        config_json: Option<&str>,
        reseed: bool,
    ) -> Result<RulesSeed, StateError> {
        let snapshot = self.snapshot();
        if snapshot.seeded.rules && !reseed {
            let stored = snapshot.rules.as_ref().map(|r| r.json.as_str());
            return Ok(if stored == config_json {
                RulesSeed::Unchanged
            } else {
                RulesSeed::Diverged
            });
        }
        let rules = config_json.map(|json| StoredRules {
            json: json.to_owned(),
            from_config: true,
        });
        self.set_access_rules(rules)?;
        Ok(RulesSeed::Seeded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = r#"{"catalogs":[{"catalog":"tpch","allow":"all"}]}"#;
    const B: &str = r#"{"catalogs":[{"allow":"none"}]}"#;

    #[test]
    fn seed_rules_first_start_then_diverged() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.seed_rules(Some(A), false).unwrap(), RulesSeed::Seeded);
        let seeded = StoredRules {
            json: A.into(),
            from_config: true,
        };
        assert_eq!(store.access_rules(), Some(seeded.clone()));
        assert!(store.seeded().rules);
        drop(store);

        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(
            store.seed_rules(Some(A), false).unwrap(),
            RulesSeed::Unchanged
        );
        assert_eq!(
            store.seed_rules(Some(B), false).unwrap(),
            RulesSeed::Diverged
        );
        assert_eq!(store.access_rules(), Some(seeded), "the stored copy wins");
        // A rules file removed from the config also diverges.
        assert_eq!(store.seed_rules(None, false).unwrap(), RulesSeed::Diverged);

        assert_eq!(store.seed_rules(Some(B), true).unwrap(), RulesSeed::Seeded);
        assert_eq!(
            store.access_rules(),
            Some(StoredRules {
                json: B.into(),
                from_config: true,
            })
        );
        assert_eq!(store.seed_rules(None, true).unwrap(), RulesSeed::Seeded);
        assert_eq!(store.access_rules(), None);
        assert_eq!(store.seed_rules(None, false).unwrap(), RulesSeed::Unchanged);
    }

    #[test]
    fn a_runtime_change_survives_reopen_and_diverges_from_the_file() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        store.seed_rules(None, false).unwrap();
        let set = StoredRules {
            json: B.into(),
            from_config: false,
        };
        store.set_access_rules(Some(set.clone())).unwrap();
        drop(store);
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.access_rules(), Some(set));
        assert_eq!(store.seed_rules(None, false).unwrap(), RulesSeed::Diverged);
    }

    #[test]
    fn h4_state_file_loads_unseeded() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        std::fs::write(&p, r#"{"version":1,"users":[],"tokens":[]}"#).unwrap();
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(
            store.seeded(),
            Seeded {
                catalogs: false,
                rules: false,
            }
        );
        assert_eq!(store.access_rules(), None);
        // So the first H5 start seeds the rules file.
        assert_eq!(store.seed_rules(Some(A), false).unwrap(), RulesSeed::Seeded);
    }
}
