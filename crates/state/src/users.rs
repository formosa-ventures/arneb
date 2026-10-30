//! Seeding users from the config file into the state store.

use arneb_protocol::ScramVerifier;

use crate::file::{StateData, StateError, StateStore, UserRecord};

/// What [`StateStore::seed_users`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeedReport {
    /// The state held no users before this call.
    pub first_start: bool,
    /// Config users written to the state (new, or replaced by a reseed).
    pub seeded: Vec<String>,
    /// Users whose state differs from the config and was left alone.
    pub diverged: Vec<String>,
}

impl StateStore {
    /// Seeds `users` (from the config file) into the state.
    ///
    /// The config is a seed: an existing user is never overwritten unless
    /// `reseed` is set, and then only users that came from the config.
    /// Every divergence between state and config logs one WARN on
    /// `arneb::config` naming the user.
    pub fn seed_users(
        &self,
        users: &[(String, ScramVerifier)],
        reseed: bool,
    ) -> Result<SeedReport, StateError> {
        let report = self.mutate(|data| Ok(apply_seed(data, users, reseed)))?;
        for user in &report.diverged {
            tracing::warn!(
                target: "arneb::config",
                user = %user,
                "user {user:?} in the state differs from the config file; \
                 restart with --reseed-state to apply the config file"
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
                report.seeded.push(name.clone());
            }
            Some(existing) if existing.scram == scram => {}
            Some(existing) if reseed && existing.from_config => {
                *existing = record();
                data.tokens.retain(|t| &t.user != name);
                report.seeded.push(name.clone());
            }
            Some(_) => report.diverged.push(name.clone()),
        }
    }
    // Config users that vanished from the config file.
    let in_config = |n: &str| users.iter().any(|(u, _)| u == n);
    if reseed {
        let dropped: Vec<String> = data
            .users
            .iter()
            .filter(|u| u.from_config && !in_config(&u.name))
            .map(|u| u.name.clone())
            .collect();
        data.users.retain(|u| !u.from_config || in_config(&u.name));
        data.tokens.retain(|t| !dropped.contains(&t.user));
    } else {
        for u in &data.users {
            if u.from_config && !in_config(&u.name) {
                report.diverged.push(u.name.clone());
            }
        }
    }
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
        let r = tracing::subscriber::with_default(sub, f);
        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        (r, out)
    }

    #[test]
    fn first_start_seeds_every_config_user() {
        let store = StateStore::open(None).unwrap();
        let r = store
            .seed_users(&cfg(&[("alice", "a"), ("bob", "b")]), false)
            .unwrap();
        assert!(r.first_start);
        assert_eq!(r.seeded, vec!["alice", "bob"]);
        assert!(r.diverged.is_empty());
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
        assert_eq!(r.seeded, vec!["bob"]);
        assert!(r.diverged.is_empty());
        assert_eq!(stored(&store).len(), 2);
    }

    #[test]
    fn changed_config_password_warns_and_keeps_state() {
        let store = StateStore::open(None).unwrap();
        store.seed_users(&cfg(&[("alice", "old")]), false).unwrap();
        let before = stored(&store);
        let (r, log) = capture(|| store.seed_users(&cfg(&[("alice", "new")]), false).unwrap());
        assert_eq!(r.diverged, vec!["alice"]);
        assert!(r.seeded.is_empty());
        assert_eq!(stored(&store), before);
        assert!(log.contains("WARN"), "{log}");
        assert!(log.contains("arneb::config"), "{log}");
        assert!(log.contains("alice"), "{log}");
        assert!(log.contains("--reseed-state"), "{log}");
    }

    #[test]
    fn reseed_replaces_config_users_and_drops_their_tokens() {
        let store = StateStore::open(None).unwrap();
        store
            .seed_users(&cfg(&[("alice", "old"), ("carol", "c")]), false)
            .unwrap();
        let ta = store.issue_token("alice", None, None).unwrap();
        let tc = store.issue_token("carol", None, None).unwrap();
        let r = store.seed_users(&cfg(&[("alice", "new")]), true).unwrap();
        assert_eq!(r.seeded, vec!["alice"]);
        assert!(r.diverged.is_empty());
        let s = stored(&store);
        assert_eq!(s.len(), 1, "carol left the config and is dropped");
        assert_eq!(s[0].0, "alice");
        assert_eq!(s[0].1, v("new").to_string());
        assert!(
            store.list_tokens("alice").is_empty(),
            "replaced user's tokens"
        );
        assert!(
            store.list_tokens("carol").is_empty(),
            "removed user's tokens"
        );
        assert_eq!(store.check_token(&ta.token), None);
        assert_eq!(store.check_token(&tc.token), None);
    }

    #[test]
    fn removed_config_user_is_reported_not_deleted() {
        let store = StateStore::open(None).unwrap();
        store
            .seed_users(&cfg(&[("alice", "a"), ("bob", "b")]), false)
            .unwrap();
        let (r, log) = capture(|| store.seed_users(&cfg(&[("alice", "a")]), false).unwrap());
        assert_eq!(r.diverged, vec!["bob"]);
        assert_eq!(stored(&store).len(), 2);
        assert!(
            log.contains("bob") && log.contains("--reseed-state"),
            "{log}"
        );
    }
}
