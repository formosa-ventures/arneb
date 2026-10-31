//! Durable coordinator state (users, API tokens) kept in one atomic,
//! versioned JSON file. Only the coordinator / standalone server opens it.

mod file;
mod tokens;
mod users;

pub use file::{StateError, StateStore};
pub use tokens::{IssuedToken, TokenInfo, MAX_TOKENS_PER_USER};
pub use users::SeedReport;

use arneb_protocol::{decoy_verifier, is_token_shaped, CredentialSource, ScramVerifier};

/// Users and live API tokens for pgwire SCRAM, Trino Basic and bearer auth.
/// Plaintext passwords and tokens are never logged or stored.
impl CredentialSource for StateStore {
    fn scram_verifiers(&self, user: &str) -> Vec<ScramVerifier> {
        let now = tokens::now_ms();
        let guard = self.snapshot();
        let Some(record) = guard.users.iter().find(|u| u.name == user) else {
            return Vec::new();
        };
        let Ok(password) = record.scram.parse::<ScramVerifier>() else {
            tracing::warn!(user = %user, "stored SCRAM verifier is invalid; refusing the user");
            return Vec::new();
        };
        // Token verifiers were derived with the user's salt and iterations;
        // a mismatch (e.g. a corrupt record) could never verify, so skip it.
        let live_tokens = guard
            .tokens
            .iter()
            .filter(|t| t.user == user && tokens::is_live(t, now))
            .filter_map(|t| t.scram.parse::<ScramVerifier>().ok())
            .filter(|v| v.salt() == password.salt() && v.iterations() == password.iterations())
            .collect::<Vec<_>>();
        std::iter::once(password).chain(live_tokens).collect()
    }

    fn verify_password(&self, user: &str, password: &str) -> bool {
        if is_token_shaped(password) {
            return self.verify_token(password).as_deref() == Some(user);
        }
        let stored = self
            .snapshot()
            .users
            .iter()
            .find(|u| u.name == user)
            .and_then(|u| u.scram.parse::<ScramVerifier>().ok());
        let known = stored.is_some();
        let verifier = stored.unwrap_or_else(|| decoy_verifier(user));
        let ok = verifier.verify_password(password);
        ok && known
    }

    fn verify_token(&self, token: &str) -> Option<String> {
        self.check_token(token).map(|info| info.user)
    }

    fn user_count(&self) -> usize {
        self.snapshot().users.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_protocol::{CredentialSource, ScramVerifier};

    fn store_with_alice() -> StateStore {
        let store = StateStore::open(None).unwrap();
        let v = ScramVerifier::from_password("pw", b"0123456789abcdef", 4096);
        store.seed_users(&[("alice".into(), v)], false).unwrap();
        store
    }

    #[test]
    fn expired_token_is_not_live() {
        let store = store_with_alice();
        let expired = store.issue_token("alice", None, Some(1)).unwrap();
        let live = store.issue_token("alice", None, Some(u64::MAX)).unwrap();

        assert_eq!(store.verify_token(&expired.token), None);
        assert!(!store.verify_password("alice", &expired.token));
        assert_eq!(store.verify_token(&live.token).as_deref(), Some("alice"));
        assert!(store.verify_password("alice", &live.token));

        // Password first, then only the live token.
        let verifiers = store.scram_verifiers("alice");
        assert_eq!(verifiers.len(), 2);
        assert!(verifiers[0].verify_password("pw"));
        assert!(verifiers[1].verify_password(&live.token));
    }

    #[test]
    fn auth_reads_do_not_wait_for_a_write_in_progress() {
        use std::sync::{mpsc, Arc};
        use std::time::Duration;
        let store = Arc::new(store_with_alice());
        let token = store.issue_token("alice", None, None).unwrap().token;
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let writer = std::thread::spawn({
            let store = Arc::clone(&store);
            move || {
                store
                    .mutate(|d| {
                        entered_tx.send(()).unwrap();
                        // Stand-in for a slow PBKDF2 / fsync under the writer lock.
                        release_rx.recv().unwrap();
                        d.tokens.clear();
                        Ok(())
                    })
                    .unwrap();
            }
        });
        entered_rx.recv().unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn({
            let store = Arc::clone(&store);
            let token = token.clone();
            move || {
                done_tx
                    .send((
                        store.verify_token(&token),
                        store.scram_verifiers("alice").len(),
                        store.list_tokens("alice").len(),
                        store.user_count(),
                    ))
                    .unwrap();
            }
        });
        let seen = done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("auth reads must not block on a write in progress");
        assert_eq!(seen, (Some("alice".to_string()), 2, 1, 1));
        release_tx.send(()).unwrap();
        writer.join().unwrap();
        // The write is visible once it lands.
        assert_eq!(store.verify_token(&token), None);
    }
}
