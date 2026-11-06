//! Users in the state store: seeding them from the config file, and
//! creating, changing and deleting them at runtime.

use arneb_protocol::ScramVerifier;

use crate::file::{StateData, StateError, StateStore, UserRecord};

/// What [`StateStore::seed_users`] did. Holds user names only, never
/// verifiers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeedReport {
    /// The state held no users before this call.
    pub first_start: bool,
    /// Config users added to the state (new in the config file).
    pub added: Vec<String>,
    /// Users whose `password_hash` changed in the config file. The new
    /// verifier is stored and all of the user's tokens are revoked. With a
    /// reseed this also lists state-created users the config overwrote.
    pub updated: Vec<String>,
    /// Config-seeded users that vanished from the config file; they are
    /// deleted along with their tokens.
    pub removed: Vec<String>,
    /// State-created users whose name is also in the config file, left
    /// alone (the config only manages users it seeded). Empty on a reseed.
    pub conflicts: Vec<String>,
    /// State-created users whose stored verifier already equals the config's.
    /// They are adopted (marked config-managed) so later config edits or
    /// removals apply to them; their tokens are kept.
    pub adopted: Vec<String>,
    /// Tokens revoked because their user was updated or removed.
    pub revoked_tokens: usize,
    /// Config users skipped because they were deleted at runtime (their
    /// tombstone stands until a reseed). Empty on a reseed.
    pub tombstoned: Vec<String>,
}

/// What [`StateStore::set_user_password`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PasswordChange {
    /// The user's API tokens revoked by the change (all of them).
    pub revoked_tokens: usize,
    /// The user was seeded from the config file and no longer is managed
    /// by it.
    pub detached: bool,
}

/// What [`StateStore::delete_user`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserDeletion {
    /// The user's API tokens deleted with them.
    pub revoked_tokens: usize,
    /// The user was seeded from the config file, so a tombstone now keeps
    /// seeding from re-adding them.
    pub tombstoned: bool,
}

/// One user as listed by [`StateStore::list_users`]; never a verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserInfo {
    /// The user name.
    pub name: String,
    /// Whether the config file manages (seeded) the user.
    pub from_config: bool,
    /// Whether the user was ever seeded from the config file (true also
    /// after a password change detached them); deleting such a user
    /// records a tombstone.
    pub config_seeded: bool,
    /// The user's live (unexpired) API tokens.
    pub tokens: usize,
}

impl SeedReport {
    /// Whether the state was left exactly as it was.
    pub fn is_noop(&self) -> bool {
        self.added.is_empty()
            && self.updated.is_empty()
            && self.removed.is_empty()
            && self.adopted.is_empty()
    }
}

impl StateStore {
    /// Applies `users` (from the config file) to the state.
    ///
    /// Users seeded from the config follow it on every start: a new user is
    /// added, a changed `password_hash` replaces the stored verifier and
    /// revokes all of that user's tokens, and a user gone from the config
    /// is deleted with their tokens. Users created through the state store
    /// are never touched, except that one whose verifier already equals the
    /// config's is adopted (`from_config = true`), and that `reseed` lets a config user overwrite
    /// a state-created user of the same name (revoking their tokens).
    /// Without `reseed` such a name collision keeps the state user and logs
    /// a WARN. Every change logs one line on `arneb::config` naming the user.
    pub fn seed_users(
        &self,
        users: &[(String, ScramVerifier)],
        reseed: bool,
    ) -> Result<SeedReport, StateError> {
        let report = self.mutate(|data| Ok(apply_seed(data, users, reseed)))?;
        log_seed_report(&report);
        Ok(report)
    }
}

/// Logs one line on `arneb::config` per user [`apply_seed`] changed or
/// skipped.
pub(crate) fn log_seed_report(report: &SeedReport) {
    for user in &report.added {
        tracing::info!(
            target: "arneb::config",
            user = %user,
            "user {user:?} added to the state from the config file"
        );
    }
    for user in &report.adopted {
        tracing::info!(
            target: "arneb::config",
            user = %user,
            "user {user:?} created through the state store has the same \
             password_hash as the config file; adopted it, so later config \
             edits or removal of the user now apply to it"
        );
    }
    for user in &report.updated {
        tracing::warn!(
            target: "arneb::config",
            user = %user,
            "user {user:?}: password_hash changed in the config file; \
             stored the new verifier and revoked all of the user's API tokens"
        );
    }
    for user in &report.removed {
        tracing::warn!(
            target: "arneb::config",
            user = %user,
            "user {user:?} is no longer in the config file; \
             deleted the user and their API tokens"
        );
    }
    for user in &report.tombstoned {
        tracing::warn!(
            target: "arneb::config",
            user = %user,
            "user {user:?} in the config file was deleted at runtime; not re-adding \
             it (its config entry is not in force). To restore the config's users, \
             preview a re-seed with POST /api/v2/state/reseed?dry_run=true"
        );
    }
    for user in &report.conflicts {
        tracing::warn!(
            target: "arneb::config",
            user = %user,
            "user {user:?} in the config file collides with a user created \
             through the state store; kept the state user (its config \
             password_hash is not in force). Restart with --reseed-state to \
             overwrite it with the config"
        );
    }
}

impl StateStore {
    /// What [`seed_users`](Self::seed_users) with the same arguments would
    /// do now, without writing anything (a re-seed preview).
    pub fn preview_seed_users(
        &self,
        users: &[(String, ScramVerifier)],
        reseed: bool,
    ) -> SeedReport {
        let mut data = StateData::clone(&self.snapshot());
        apply_seed(&mut data, users, reseed)
    }

    /// The config users deleted at runtime whose tombstone keeps seeding
    /// from re-adding them (cleared by a reseed).
    pub fn tombstones(&self) -> Vec<String> {
        self.snapshot().deleted_config_users.clone()
    }

    /// Creates the state user `name` (not seeded from the config) with
    /// `verifier`. An existing name fails with [`StateError::UserExists`].
    pub fn create_user(&self, name: &str, verifier: ScramVerifier) -> Result<(), StateError> {
        let scram = verifier.to_string();
        self.mutate(|data| {
            if data.users.iter().any(|u| u.name == name) {
                return Err(StateError::UserExists(name.to_owned()));
            }
            data.users.push(UserRecord {
                name: name.to_owned(),
                scram,
                from_config: false,
                config_seeded: false,
            });
            Ok(())
        })
    }

    /// Replaces `name`'s verifier and revokes all of their API tokens. A
    /// config-seeded user is detached: the config file no longer manages
    /// them. An unknown name fails with [`StateError::UnknownUser`].
    pub fn set_user_password(
        &self,
        name: &str,
        verifier: ScramVerifier,
    ) -> Result<PasswordChange, StateError> {
        let scram = verifier.to_string();
        self.mutate(|data| {
            let user = data
                .users
                .iter_mut()
                .find(|u| u.name == name)
                .ok_or_else(|| StateError::UnknownUser(name.to_owned()))?;
            let detached = user.from_config;
            user.scram = scram;
            user.from_config = false;
            Ok(PasswordChange {
                revoked_tokens: drop_tokens_of(data, name),
                detached,
            })
        })
    }

    /// Deletes `name` and all of their API tokens. Deleting a user that was
    /// ever seeded from the config (also one a password change detached)
    /// records a tombstone, so seeding does not re-add them until a reseed. An unknown name fails with [`StateError::UnknownUser`].
    pub fn delete_user(&self, name: &str) -> Result<UserDeletion, StateError> {
        self.mutate(|data| {
            let at = data
                .users
                .iter()
                .position(|u| u.name == name)
                .ok_or_else(|| StateError::UnknownUser(name.to_owned()))?;
            let removed = data.users.remove(at);
            let tombstoned = removed.from_config || removed.config_seeded;
            if tombstoned && !data.deleted_config_users.iter().any(|n| n == name) {
                data.deleted_config_users.push(name.to_owned());
            }
            Ok(UserDeletion {
                revoked_tokens: drop_tokens_of(data, name),
                tombstoned,
            })
        })
    }

    /// Whether the state user `name` exists.
    pub fn user_exists(&self, name: &str) -> bool {
        self.snapshot().users.iter().any(|u| u.name == name)
    }

    /// Every user, in name order, with their live token count.
    pub fn list_users(&self) -> Vec<UserInfo> {
        let data = self.snapshot();
        let now = crate::tokens::now_ms();
        let mut users: Vec<UserInfo> = data
            .users
            .iter()
            .map(|u| UserInfo {
                name: u.name.clone(),
                from_config: u.from_config,
                config_seeded: u.from_config || u.config_seeded,
                tokens: crate::tokens::live_token_count(&data, &u.name, now),
            })
            .collect();
        users.sort_by(|a, b| a.name.cmp(&b.name));
        users
    }
}

/// Removes every token of `user`; returns how many.
fn drop_tokens_of(data: &mut StateData, user: &str) -> usize {
    let before = data.tokens.len();
    data.tokens.retain(|t| t.user != user);
    before - data.tokens.len()
}

pub(crate) fn apply_seed(
    data: &mut StateData,
    users: &[(String, ScramVerifier)],
    reseed: bool,
) -> SeedReport {
    let mut report = SeedReport {
        first_start: data.users.is_empty(),
        ..SeedReport::default()
    };
    let mut revoke: Vec<String> = Vec::new();
    if reseed {
        data.deleted_config_users.clear();
    }
    for (name, verifier) in users {
        if data.deleted_config_users.contains(name) {
            report.tombstoned.push(name.clone());
            continue;
        }
        let scram = verifier.to_string();
        let record = || UserRecord {
            name: name.clone(),
            scram: scram.clone(),
            from_config: true,
            config_seeded: true,
        };
        match data.users.iter_mut().find(|u| &u.name == name) {
            None => {
                data.users.push(record());
                report.added.push(name.clone());
            }
            Some(existing) if existing.scram == scram => {
                // Same verifier as a state-created user: take it over so
                // later config edits/removals revoke it. Nothing changes
                // for the user, so their tokens stay.
                if !existing.from_config {
                    existing.from_config = true;
                    report.adopted.push(name.clone());
                }
            }
            Some(existing) if !existing.from_config && !reseed => {
                report.conflicts.push(name.clone());
            }
            Some(existing) => {
                *existing = record();
                report.updated.push(name.clone());
                revoke.push(name.clone());
            }
        }
    }
    // Config-seeded users that vanished from the config file. A tombstone
    // whose name left the config has done its job: drop it, so adding the
    // name back later seeds it again.
    let in_config = |n: &str| users.iter().any(|(u, _)| u == n);
    data.deleted_config_users.retain(|n| in_config(n));
    data.users.retain(|u| {
        let gone = u.from_config && !in_config(&u.name);
        if gone {
            report.removed.push(u.name.clone());
            revoke.push(u.name.clone());
        }
        !gone
    });
    let before = data.tokens.len();
    data.tokens.retain(|t| !revoke.contains(&t.user));
    report.revoked_tokens = before - data.tokens.len();
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(pw: &str) -> ScramVerifier {
        ScramVerifier::from_password(pw, b"0123456789abcdef", 4096)
    }

    fn cfg(pairs: &[(&str, &str)]) -> Vec<(String, ScramVerifier)> {
        pairs.iter().map(|(n, p)| (n.to_string(), v(p))).collect()
    }

    fn stored(store: &StateStore) -> Vec<(String, String, bool)> {
        let mut out = store
            .mutate(|d| {
                Ok(d.users
                    .iter()
                    .map(|u| (u.name.clone(), u.scram.clone(), u.from_config))
                    .collect::<Vec<_>>())
            })
            .unwrap();
        out.sort();
        out
    }

    use crate::test_log::capture;

    /// Adds a user the way the state store's own API would (not seeded).
    fn add_state_user(store: &StateStore, name: &str, pw: &str) {
        let scram = v(pw).to_string();
        store
            .mutate(|d| {
                d.users.push(UserRecord {
                    name: name.into(),
                    scram,
                    from_config: false,
                    config_seeded: false,
                });
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn first_start_seeds_every_config_user() {
        let store = StateStore::open(None).unwrap();
        let r = store
            .seed_users(&cfg(&[("alice", "a"), ("bob", "b")]), false)
            .unwrap();
        assert!(r.first_start);
        assert_eq!(r.added, vec!["alice", "bob"]);
        assert!(r.updated.is_empty() && r.removed.is_empty());
        let s = stored(&store);
        assert_eq!(s.len(), 2);
        assert!(s.iter().all(|(_, _, from_config)| *from_config));
    }

    #[test]
    fn new_config_user_is_added_on_a_later_start() {
        let store = StateStore::open(None).unwrap();
        store.seed_users(&cfg(&[("alice", "a")]), false).unwrap();
        let r = store
            .seed_users(&cfg(&[("alice", "a"), ("bob", "b")]), false)
            .unwrap();
        assert!(!r.first_start);
        assert_eq!(r.added, vec!["bob"]);
        assert!(r.updated.is_empty() && r.removed.is_empty());
        assert_eq!(stored(&store).len(), 2);
    }

    #[test]
    fn unchanged_config_is_a_noop() {
        let store = StateStore::open(None).unwrap();
        let users = cfg(&[("alice", "a"), ("bob", "b")]);
        store.seed_users(&users, false).unwrap();
        let t = store.issue_token("alice", None, None).unwrap();
        let before = stored(&store);
        let (r, log) = capture(|| store.seed_users(&users, false).unwrap());
        assert!(r.is_noop(), "{r:?}");
        assert_eq!(r.revoked_tokens, 0);
        assert!(r.conflicts.is_empty());
        assert_eq!(stored(&store), before);
        assert_eq!(store.check_token(&t.token), Some(t.info));
        assert!(!log.contains("WARN"), "{log}");
    }

    #[test]
    fn changed_config_password_applies_and_revokes_tokens() {
        use arneb_protocol::CredentialSource;
        let store = StateStore::open(None).unwrap();
        store
            .seed_users(&cfg(&[("alice", "old"), ("bob", "b")]), false)
            .unwrap();
        let ta = store.issue_token("alice", None, None).unwrap();
        let tb = store.issue_token("bob", None, None).unwrap();
        let (r, log) = capture(|| {
            store
                .seed_users(&cfg(&[("alice", "new"), ("bob", "b")]), false)
                .unwrap()
        });
        assert_eq!(r.updated, vec!["alice"]);
        assert_eq!(r.revoked_tokens, 1);
        assert!(!store.verify_password("alice", "old"), "old password");
        assert!(store.verify_password("alice", "new"));
        assert_eq!(store.check_token(&ta.token), None);
        assert!(store.list_tokens("alice").is_empty());
        assert_eq!(store.check_token(&tb.token), Some(tb.info), "bob untouched");
        assert!(
            log.contains("WARN") && log.contains("arneb::config"),
            "{log}"
        );
        assert!(log.contains("alice") && log.contains("revoked"), "{log}");
        assert!(
            !log.contains("SCRAM-SHA-256$"),
            "no verifiers in logs: {log}"
        );
    }

    #[test]
    fn removed_config_user_is_dropped_with_their_tokens() {
        let store = StateStore::open(None).unwrap();
        store
            .seed_users(&cfg(&[("alice", "a"), ("bob", "b")]), false)
            .unwrap();
        let tb = store.issue_token("bob", None, None).unwrap();
        let (r, log) = capture(|| store.seed_users(&cfg(&[("alice", "a")]), false).unwrap());
        assert_eq!(r.removed, vec!["bob"]);
        assert_eq!(r.revoked_tokens, 1);
        let s = stored(&store);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].0, "alice");
        assert_eq!(store.check_token(&tb.token), None);
        assert!(store.snapshot().tokens.is_empty());
        assert!(
            log.contains("bob") && log.contains("no longer in the config file"),
            "{log}"
        );
    }

    #[test]
    fn state_created_users_are_never_touched_by_seeding() {
        let store = StateStore::open(None).unwrap();
        store.seed_users(&cfg(&[("alice", "a")]), false).unwrap();
        add_state_user(&store, "dave", "d");
        let td = store.issue_token("dave", None, None).unwrap();
        // dave is not in the config: not removed.
        let r = store.seed_users(&cfg(&[("alice", "a")]), false).unwrap();
        assert!(r.is_noop(), "{r:?}");
        assert_eq!(store.check_token(&td.token), Some(td.info.clone()));
        // A config user of the same name: kept, reported as a conflict.
        let (r, log) = capture(|| {
            store
                .seed_users(&cfg(&[("alice", "a"), ("dave", "other")]), false)
                .unwrap()
        });
        assert_eq!(r.conflicts, vec!["dave"]);
        assert!(r.is_noop(), "{r:?}");
        let dave = stored(&store).into_iter().find(|u| u.0 == "dave").unwrap();
        assert_eq!(dave.1, v("d").to_string());
        assert!(!dave.2, "still state-created");
        assert_eq!(store.check_token(&td.token), Some(td.info));
        assert!(
            log.contains("dave") && log.contains("--reseed-state"),
            "{log}"
        );
    }

    #[test]
    fn matching_state_user_is_adopted_then_follows_the_config() {
        let store = StateStore::open(None).unwrap();
        add_state_user(&store, "dave", "d");
        let td = store.issue_token("dave", None, None).unwrap();
        let (r, log) = capture(|| store.seed_users(&cfg(&[("dave", "d")]), false).unwrap());
        assert_eq!(r.adopted, vec!["dave"]);
        assert!(!r.is_noop());
        assert!(r.conflicts.is_empty() && r.updated.is_empty());
        assert_eq!(r.revoked_tokens, 0);
        assert!(stored(&store)[0].2, "now config-managed");
        assert_eq!(store.check_token(&td.token), Some(td.info.clone()));
        assert!(log.contains("dave") && log.contains("adopted"), "{log}");
        assert!(log.contains("arneb::config"), "{log}");
        // Re-seeding the same config is a no-op again.
        let r = store.seed_users(&cfg(&[("dave", "d")]), false).unwrap();
        assert!(r.is_noop(), "{r:?}");
        // A later hash change updates the user and revokes tokens.
        let r = store.seed_users(&cfg(&[("dave", "new")]), false).unwrap();
        assert_eq!(r.updated, vec!["dave"]);
        assert!(r.conflicts.is_empty());
        assert_eq!(store.check_token(&td.token), None);
        // Removal from the config deletes him.
        let r = store.seed_users(&[], false).unwrap();
        assert_eq!(r.removed, vec!["dave"]);
        assert!(stored(&store).is_empty());
    }

    #[test]
    fn reseed_overwrites_a_colliding_state_user_and_drops_their_tokens() {
        let store = StateStore::open(None).unwrap();
        add_state_user(&store, "dave", "d");
        let td = store.issue_token("dave", None, None).unwrap();
        let r = store.seed_users(&cfg(&[("dave", "cfg")]), true).unwrap();
        assert_eq!(r.updated, vec!["dave"]);
        assert!(r.conflicts.is_empty());
        assert_eq!(r.revoked_tokens, 1);
        let s = stored(&store);
        assert_eq!(s, vec![("dave".into(), v("cfg").to_string(), true)]);
        assert_eq!(store.check_token(&td.token), None);
    }
    #[test]
    fn create_user_then_conflict() {
        let store = StateStore::open(None).unwrap();
        store.seed_users(&cfg(&[("alice", "a")]), false).unwrap();
        store.create_user("dave", v("davepass")).unwrap();
        let err = store.create_user("dave", v("other")).unwrap_err();
        assert!(
            matches!(err, StateError::UserExists(ref n) if n == "dave"),
            "{err:?}"
        );
        let err = store.create_user("alice", v("other")).unwrap_err();
        assert!(matches!(err, StateError::UserExists(_)), "{err:?}");
        assert_eq!(
            store.list_users(),
            vec![
                UserInfo {
                    name: "alice".into(),
                    from_config: true,
                    config_seeded: true,
                    tokens: 0
                },
                UserInfo {
                    name: "dave".into(),
                    from_config: false,
                    config_seeded: false,
                    tokens: 0
                },
            ]
        );
        let dave = stored(&store).into_iter().find(|u| u.0 == "dave").unwrap();
        assert_eq!(
            dave.1,
            v("davepass").to_string(),
            "the first verifier is kept"
        );
    }

    #[test]
    fn set_password_revokes_tokens_and_detaches_config_user() {
        use arneb_protocol::CredentialSource;
        let store = StateStore::open(None).unwrap();
        store
            .seed_users(&cfg(&[("alice", "a"), ("bob", "b")]), false)
            .unwrap();
        let t1 = store.issue_token("alice", None, None).unwrap();
        store.issue_token("alice", None, None).unwrap();
        let tb = store.issue_token("bob", None, None).unwrap();
        let change = store.set_user_password("alice", v("newpass")).unwrap();
        assert_eq!(
            change,
            PasswordChange {
                revoked_tokens: 2,
                detached: true
            }
        );
        assert!(store.verify_password("alice", "newpass"));
        assert!(!store.verify_password("alice", "a"));
        assert_eq!(store.check_token(&t1.token), None);
        assert_eq!(store.check_token(&tb.token), Some(tb.info), "bob untouched");
        let alice = store
            .list_users()
            .into_iter()
            .find(|u| u.name == "alice")
            .unwrap();
        assert!(!alice.from_config, "detached");
        assert_eq!(alice.tokens, 0);
        // Detached: the next start reports a conflict and keeps the state user.
        let r = store
            .seed_users(&cfg(&[("alice", "a"), ("bob", "b")]), false)
            .unwrap();
        assert_eq!(r.conflicts, vec!["alice"]);
        assert!(store.verify_password("alice", "newpass"));
        // A state-created user is not "detached" again.
        store.create_user("dave", v("davepass")).unwrap();
        let change = store.set_user_password("dave", v("davepass2")).unwrap();
        assert_eq!(
            change,
            PasswordChange {
                revoked_tokens: 0,
                detached: false
            }
        );
        let err = store.set_user_password("nobody", v("x")).unwrap_err();
        assert!(matches!(err, StateError::UnknownUser(_)), "{err:?}");
    }

    #[test]
    fn delete_config_user_tombstones_and_seed_skips_it() {
        let store = StateStore::open(None).unwrap();
        let config = cfg(&[("alice", "a"), ("bob", "b")]);
        store.seed_users(&config, false).unwrap();
        let tb = store.issue_token("bob", None, None).unwrap();
        let gone = store.delete_user("bob").unwrap();
        assert_eq!(
            gone,
            UserDeletion {
                revoked_tokens: 1,
                tombstoned: true
            }
        );
        assert_eq!(store.check_token(&tb.token), None);
        assert!(store.snapshot().tokens.is_empty());
        assert_eq!(store.snapshot().deleted_config_users, vec!["bob"]);

        let (r, log) = capture(|| store.seed_users(&config, false).unwrap());
        assert_eq!(r.tombstoned, vec!["bob"]);
        assert!(r.added.is_empty(), "{r:?}");
        assert!(r.is_noop(), "{r:?}");
        assert!(store.list_users().iter().all(|u| u.name != "bob"));
        assert!(
            log.contains("WARN") && log.contains("arneb::config"),
            "{log}"
        );
        assert!(log.contains("bob"), "{log}");
        assert!(
            log.contains("preview a re-seed with POST /api/v2/state/reseed?dry_run=true"),
            "{log}"
        );

        // A reseed re-adds the user and clears the tombstone.
        let r = store.seed_users(&config, true).unwrap();
        assert_eq!(r.added, vec!["bob"]);
        assert!(r.tombstoned.is_empty());
        assert!(store.snapshot().deleted_config_users.is_empty());
        let r = store.seed_users(&config, false).unwrap();
        assert!(r.is_noop() && r.tombstoned.is_empty(), "{r:?}");

        // A state-created user leaves no tombstone.
        store.create_user("dave", v("davepass")).unwrap();
        let gone = store.delete_user("dave").unwrap();
        assert_eq!(
            gone,
            UserDeletion {
                revoked_tokens: 0,
                tombstoned: false
            }
        );
        assert!(store.snapshot().deleted_config_users.is_empty());
        let err = store.delete_user("dave").unwrap_err();
        assert!(matches!(err, StateError::UnknownUser(_)), "{err:?}");
    }

    #[test]
    fn preview_seed_users_reports_without_writing() {
        let store = StateStore::open(None).unwrap();
        store
            .seed_users(&cfg(&[("alice", "a"), ("bob", "b")]), false)
            .unwrap();
        store.delete_user("bob").unwrap();
        assert_eq!(store.tombstones(), vec!["bob"]);
        let before = stored(&store);
        let preview = store.preview_seed_users(&cfg(&[("alice", "a2"), ("bob", "b")]), true);
        assert_eq!(preview.added, vec!["bob"]);
        assert_eq!(preview.updated, vec!["alice"]);
        assert_eq!(stored(&store), before, "a preview writes nothing");
        assert_eq!(store.tombstones(), vec!["bob"]);
    }

    #[test]
    fn tombstones_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        let config = cfg(&[("alice", "a"), ("bob", "b")]);
        {
            let store = StateStore::open(Some(&path)).unwrap();
            store.seed_users(&config, false).unwrap();
            store.delete_user("bob").unwrap();
        }
        let store = StateStore::open(Some(&path)).unwrap();
        let r = store.seed_users(&config, false).unwrap();
        assert_eq!(r.tombstoned, vec!["bob"]);
        assert_eq!(store.list_users().len(), 1);
        // Once bob leaves the config the tombstone goes; adding him back
        // seeds him again.
        store.seed_users(&cfg(&[("alice", "a")]), false).unwrap();
        assert!(store.snapshot().deleted_config_users.is_empty());
        let r = store.seed_users(&config, false).unwrap();
        assert_eq!(r.added, vec!["bob"]);
    }

    #[test]
    fn created_user_authenticates_with_password_and_token() {
        use arneb_protocol::CredentialSource;
        let store = StateStore::open(None).unwrap();
        store.seed_users(&cfg(&[("alice", "a")]), false).unwrap();
        let verifier = ScramVerifier::generate("davepass").unwrap();
        assert_eq!(verifier.salt().len(), 16);
        assert_eq!(verifier.iterations(), 4096);
        store.create_user("dave", verifier).unwrap();
        assert!(store.verify_password("dave", "davepass"));
        assert!(!store.verify_password("dave", "wrong"));
        let t = store.issue_token("dave", None, None).unwrap();
        assert_eq!(store.verify_token(&t.token).as_deref(), Some("dave"));
        assert!(store.verify_password("dave", &t.token));
        let verifiers = store.scram_verifiers("dave");
        assert_eq!(verifiers.len(), 2);
        assert!(verifiers[0].verify_password("davepass"));
        assert!(verifiers[1].verify_password(&t.token));
        assert_eq!(
            store
                .list_users()
                .into_iter()
                .find(|u| u.name == "dave")
                .unwrap()
                .tokens,
            1
        );
    }
    #[test]
    fn deleting_a_detached_config_user_still_tombstones() {
        use arneb_protocol::CredentialSource;
        let store = StateStore::open(None).unwrap();
        let config = cfg(&[("alice", "a"), ("bob", "b")]);
        store.seed_users(&config, false).unwrap();
        let change = store.set_user_password("alice", v("newpass")).unwrap();
        assert!(change.detached);
        let alice = store
            .list_users()
            .into_iter()
            .find(|u| u.name == "alice")
            .unwrap();
        assert!(!alice.from_config && alice.config_seeded, "{alice:?}");
        let gone = store.delete_user("alice").unwrap();
        assert!(gone.tombstoned, "{gone:?}");
        let r = store.seed_users(&config, false).unwrap();
        assert_eq!(r.tombstoned, vec!["alice"]);
        assert!(r.added.is_empty(), "{r:?}");
        assert!(store.list_users().iter().all(|u| u.name != "alice"));
        assert!(
            !store.verify_password("alice", "a"),
            "the config password stays dead"
        );
    }
}
