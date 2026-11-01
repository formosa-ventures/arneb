//! The access rules kept in the state store (runtime-config D1, D3): seeded
//! from the `[access_control] rules_file` on the first start and changed at
//! runtime through `/api/v2/access-rules`.
//!
//! Rules that came from the file follow the file: while they were never
//! changed at runtime, a later start whose rules file differs re-seeds them.
//! Rules changed at runtime are kept over an unchanged rules file; a rules
//! file edited since it was last seeded then refuses the start, since
//! neither copy can be assumed to be the one the operator wants.

use serde::{Deserialize, Serialize};

use crate::file::{StateData, StateError, StateStore};

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

/// Where the stored rules came from (state file field `rules_origin`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RulesOrigin {
    /// [`rules_fingerprint`] of the rules file text last seeded.
    pub file_sha256: String,
    /// The rules were set or cleared at runtime after that seed.
    pub runtime: bool,
}

/// What [`StateStore::seed_rules`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RulesSeed {
    /// The config's rules were stored (first start, or a re-seed).
    Seeded,
    /// The stored rules equal the config's.
    Unchanged,
    /// The stored rules came from the rules file and were never changed at
    /// runtime, and the file changed: the file's rules were stored.
    FollowedFile,
    /// The rules were changed at runtime and the rules file is unchanged
    /// since it was last seeded: the runtime rules were kept.
    Diverged,
    /// The rules were changed at runtime and the rules file also changed
    /// since it was last seeded: nothing was written, and the caller must
    /// refuse to start until the operator re-seeds or reverts the file.
    FileChanged,
}

/// The fingerprint of a rules file's text (`None`: no rules file), as
/// stored in the state to tell whether the file changed since it was last
/// seeded: the hex SHA-256 of `file:<text>`, or `none`.
pub fn rules_fingerprint(config_json: Option<&str>) -> String {
    match config_json {
        None => "none".to_owned(),
        Some(text) => {
            let mut bytes = b"file:".to_vec();
            bytes.extend_from_slice(text.as_bytes());
            crate::tokens::sha256(&bytes)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect()
        }
    }
}

/// Stores the rules file's rules in `d` as seeded from the file.
pub(crate) fn apply_rules_seed(d: &mut StateData, config_json: Option<&str>) {
    d.rules = config_json.map(|json| StoredRules {
        json: json.to_owned(),
        from_config: true,
    });
    d.rules_origin = Some(RulesOrigin {
        file_sha256: rules_fingerprint(config_json),
        runtime: false,
    });
    d.seeded.rules = true;
}

impl StateStore {
    /// The stored access rules; `None` means no rules (allow everything).
    pub fn access_rules(&self) -> Option<StoredRules> {
        self.snapshot().rules.clone()
    }

    /// Stores `rules` set at runtime, replacing the stored ones (`None`
    /// clears them). From then on a restart keeps them over an unchanged
    /// rules file ([`seed_rules`](Self::seed_rules)).
    pub fn set_access_rules(&self, rules: Option<StoredRules>) -> Result<(), StateError> {
        self.mutate(|d| {
            d.rules = rules;
            d.seeded.rules = true;
            let file_sha256 = d
                .rules_origin
                .take()
                .map(|o| o.file_sha256)
                .unwrap_or_default();
            d.rules_origin = Some(RulesOrigin {
                file_sha256,
                runtime: true,
            });
            Ok(())
        })
    }

    /// Which config objects the state was seeded with.
    pub fn seeded(&self) -> Seeded {
        self.snapshot().seeded
    }

    /// Whether the stored rules were set or cleared at runtime since the
    /// rules file was last seeded.
    pub fn rules_changed_at_runtime(&self) -> bool {
        let d = self.snapshot();
        rules_are_runtime(&d)
    }

    /// Seeds the rules from the config's rules file text (`None`: no rules
    /// file). The caller validates `config_json` first.
    ///
    /// - The first call on a state that was never seeded, or any call with
    ///   `reseed`: stores it with `from_config = true` (or clears the
    ///   rules), [`RulesSeed::Seeded`].
    /// - Stored rules equal to the file's: [`RulesSeed::Unchanged`].
    /// - Stored rules from the file, never changed at runtime: the file
    ///   wins and is stored, [`RulesSeed::FollowedFile`].
    /// - Rules changed at runtime: kept while the file is unchanged since
    ///   its last seed ([`RulesSeed::Diverged`]), else nothing is written
    ///   and the result is [`RulesSeed::FileChanged`].
    pub fn seed_rules(
        &self,
        config_json: Option<&str>,
        reseed: bool,
    ) -> Result<RulesSeed, StateError> {
        let snapshot = self.snapshot();
        if !snapshot.seeded.rules || reseed {
            self.mutate(|d| {
                apply_rules_seed(d, config_json);
                Ok(())
            })?;
            return Ok(RulesSeed::Seeded);
        }
        let fingerprint = rules_fingerprint(config_json);
        if snapshot.rules.as_ref().map(|r| r.json.as_str()) == config_json {
            return Ok(RulesSeed::Unchanged);
        }
        if !rules_are_runtime(&snapshot) {
            self.mutate(|d| {
                apply_rules_seed(d, config_json);
                Ok(())
            })?;
            return Ok(RulesSeed::FollowedFile);
        }
        match snapshot
            .rules_origin
            .as_ref()
            .map(|o| o.file_sha256.as_str())
        {
            Some(seeded) if seeded == fingerprint => Ok(RulesSeed::Diverged),
            // Set at runtime before the fingerprint was recorded (a state
            // written by an earlier build): take this file as the seeded
            // one, so a later edit of it is caught.
            None | Some("") => {
                self.mutate(|d| {
                    d.rules_origin = Some(RulesOrigin {
                        file_sha256: fingerprint,
                        runtime: true,
                    });
                    Ok(())
                })?;
                Ok(RulesSeed::Diverged)
            }
            Some(_) => Ok(RulesSeed::FileChanged),
        }
    }
}

/// Whether `d`'s rules were changed at runtime. A state without
/// `rules_origin` (written by an earlier build) counts stored rules with
/// `from_config = false` as runtime ones, and no rules as the file's.
fn rules_are_runtime(d: &StateData) -> bool {
    match &d.rules_origin {
        Some(origin) => origin.runtime,
        None => d.rules.as_ref().is_some_and(|r| !r.from_config),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = r#"{"catalogs":[{"catalog":"tpch","allow":"all"}]}"#;
    const B: &str = r#"{"catalogs":[{"allow":"none"}]}"#;

    fn file_rules(json: &str) -> Option<StoredRules> {
        Some(StoredRules {
            json: json.into(),
            from_config: true,
        })
    }

    /// Rules seeded from the file and never changed at runtime follow the
    /// file: a changed or removed rules file is stored on the next start.
    #[test]
    fn file_rules_follow_the_file() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.seed_rules(Some(A), false).unwrap(), RulesSeed::Seeded);
        assert_eq!(store.access_rules(), file_rules(A));
        assert!(store.seeded().rules);
        assert!(!store.rules_changed_at_runtime());
        drop(store);

        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(
            store.seed_rules(Some(A), false).unwrap(),
            RulesSeed::Unchanged
        );
        // Tightening the file takes effect on the next start.
        assert_eq!(
            store.seed_rules(Some(B), false).unwrap(),
            RulesSeed::FollowedFile
        );
        assert_eq!(store.access_rules(), file_rules(B), "the file wins");
        drop(store);
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.access_rules(), file_rules(B), "and it was persisted");
        // A rules file removed from the config clears them.
        assert_eq!(
            store.seed_rules(None, false).unwrap(),
            RulesSeed::FollowedFile
        );
        assert_eq!(store.access_rules(), None);
        assert_eq!(store.seed_rules(None, false).unwrap(), RulesSeed::Unchanged);
        assert_eq!(store.seed_rules(Some(B), true).unwrap(), RulesSeed::Seeded);
        assert_eq!(store.access_rules(), file_rules(B));
    }

    /// Rules changed at runtime are kept over an unchanged rules file, and
    /// a file edited since its last seed is reported, with nothing written,
    /// until a re-seed.
    #[test]
    fn runtime_rules_are_kept_until_the_file_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        store.seed_rules(Some(A), false).unwrap();
        let set = StoredRules {
            json: B.into(),
            from_config: false,
        };
        store.set_access_rules(Some(set.clone())).unwrap();
        assert!(store.rules_changed_at_runtime());
        drop(store);

        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.access_rules(), Some(set.clone()));
        assert_eq!(
            store.seed_rules(Some(A), false).unwrap(),
            RulesSeed::Diverged
        );
        assert_eq!(store.access_rules(), Some(set.clone()));
        // The file changed since it was seeded: refused, nothing written.
        const C: &str = r#"{"catalogs":[{"catalog":"c","allow":"all"}]}"#;
        assert_eq!(
            store.seed_rules(Some(C), false).unwrap(),
            RulesSeed::FileChanged
        );
        assert_eq!(
            store.seed_rules(None, false).unwrap(),
            RulesSeed::FileChanged
        );
        assert_eq!(store.access_rules(), Some(set));
        // A file that now equals the runtime rules is simply unchanged.
        assert_eq!(
            store.seed_rules(Some(B), false).unwrap(),
            RulesSeed::Unchanged
        );
        // A re-seed takes the file and its origin again.
        assert_eq!(store.seed_rules(Some(C), true).unwrap(), RulesSeed::Seeded);
        assert!(!store.rules_changed_at_runtime());
        assert_eq!(store.access_rules(), file_rules(C));

        // A runtime clear counts as a runtime change too.
        store.set_access_rules(None).unwrap();
        assert_eq!(
            store.seed_rules(Some(C), false).unwrap(),
            RulesSeed::Diverged
        );
        assert_eq!(store.access_rules(), None);
    }

    /// A state written before `rules_origin` existed: runtime rules
    /// (`from_config = false`) are kept and the current file becomes the
    /// seeded one, so a later edit of it is caught.
    #[test]
    fn state_without_rules_origin_records_the_file() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        std::fs::write(
            &p,
            format!(
                r#"{{"version":1,"seeded":{{"rules":true}},"rules":{{"json":{},"from_config":false}}}}"#,
                serde_json::to_string(B).unwrap()
            ),
        )
        .unwrap();
        let store = StateStore::open(Some(&p)).unwrap();
        assert!(store.rules_changed_at_runtime());
        assert_eq!(
            store.seed_rules(Some(A), false).unwrap(),
            RulesSeed::Diverged
        );
        assert_eq!(
            store.seed_rules(Some(A), false).unwrap(),
            RulesSeed::Diverged
        );
        assert_eq!(
            store.seed_rules(None, false).unwrap(),
            RulesSeed::FileChanged
        );
    }

    #[test]
    fn fingerprints_tell_files_apart() {
        assert_eq!(rules_fingerprint(Some(A)), rules_fingerprint(Some(A)));
        assert_ne!(rules_fingerprint(Some(A)), rules_fingerprint(Some(B)));
        assert_ne!(rules_fingerprint(None), rules_fingerprint(Some("")));
        assert_eq!(rules_fingerprint(Some(A)).len(), 64);
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
