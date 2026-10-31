//! Seeding users from the config file into the state store.

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
    /// Tokens revoked because their user was updated or removed.
    pub revoked_tokens: usize,
}

impl SeedReport {
    /// Whether the state was left exactly as it was.
    pub fn is_noop(&self) -> bool {
        self.added.is_empty() && self.updated.is_empty() && self.removed.is_empty()
    }
}

impl StateStore {
    /// Applies `users` (from the config file) to the state.
    ///
    /// Users seeded from the config follow it on every start: a new user is
    /// added, a changed `password_hash` replaces the stored verifier and
    /// revokes all of that user's tokens, and a user gone from the config
    /// is deleted with their tokens. Users created through the state store
    /// are never touched, except that `reseed` lets a config user overwrite
    /// a state-created user of the same name (revoking their tokens).
    /// Without `reseed` such a name collision keeps the state user and logs
    /// a WARN. Every change logs one line on `arneb::config` naming the user.
    pub fn seed_users(
        &self,
        users: &[(String, ScramVerifier)],
        reseed: bool,
    ) -> Result<SeedReport, StateError> {
        let report = self.mutate(|data| Ok(apply_seed(data, users, reseed)))?;
        for user in &report.added {
            tracing::info!(
                target: "arneb::config",
                user = %user,
                "user {user:?} added to the state from the config file"
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
        Ok(report)
    }
}

fn apply_seed(data: &mut StateData, users: &[(String, ScramVerifier)], reseed: bool) -> SeedReport {
    let mut report = SeedReport {
        first_start: data.users.is_empty(),
        ..SeedReport::default()
    };
    let mut revoke: Vec<String> = Vec::new();
    for (name, verifier) in users {
        let scram = verifier.to_string();
        let record = || UserRecord {
            name: name.clone(),
            scram: scram.clone(),
            from_config: true,
        };
        match data.users.iter_mut().find(|u| &u.name == name) {
            None => {
                data.users.push(record());
                report.added.push(name.clone());
            }
            Some(existing) if existing.scram == scram => {}
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
    // Config-seeded users that vanished from the config file.
    let in_config = |n: &str| users.iter().any(|(u, _)| u == n);
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
    use std::io;
    use std::sync::{Arc, Mutex};

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

    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);
    impl io::Write for Buf {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Runs `f` and returns what it logged.
    fn capture<R>(f: impl FnOnce() -> R) -> (R, String) {
        let buf = Buf::default();
        let sink = buf.clone();
        let sub = tracing_subscriber::fmt()
            .with_writer(move || sink.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .finish();
        // Tests on other threads (no subscriber) can cache a callsite as
        // disabled; rebuild the interest cache once this one is installed.
        let r = tracing::subscriber::with_default(sub, || {
            tracing::callsite::rebuild_interest_cache();
            f()
        });
        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        (r, out)
    }

    /// Adds a user the way the state store's own API would (not seeded).
    fn add_state_user(store: &StateStore, name: &str, pw: &str) {
        let scram = v(pw).to_string();
        store
            .mutate(|d| {
                d.users.push(UserRecord {
                    name: name.into(),
                    scram,
                    from_config: false,
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
}
