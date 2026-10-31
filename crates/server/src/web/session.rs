//! In-memory Web sessions: bearer tokens `arneb_s_<43 base64url>` that the
//! Web port accepts in place of an API token.
//!
//! - A session token never satisfies
//!   [`is_token_shaped`](arneb_protocol::is_token_shaped), so pgwire and
//!   Trino never accept one.
//! - Only a SHA-256 of each token is kept, never the token itself, and the
//!   stored digest is compared in constant time.
//! - Sessions live [`SESSION_TTL`] and die with the process; expired ones
//!   are pruned lazily, on the next create or on a lookup that finds them.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aws_lc_rs::constant_time;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;

/// How long a session stays valid after it is created.
pub const SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);

/// Prefix of every session token.
pub const SESSION_PREFIX: &str = "arneb_s_";

/// Live sessions one user may hold; creating one more ends their oldest.
pub const MAX_SESSIONS_PER_USER: usize = 16;

/// Live sessions the whole process may hold; past it, creating one fails
/// with [`SessionLimit`].
pub const MAX_SESSIONS: usize = 10_000;

/// Random bytes in a session secret (43 base64url characters).
const SECRET_LEN: usize = 32;

type TokenDigest = [u8; 32];

struct Session {
    /// The full digest again, compared in constant time after the lookup.
    digest: TokenDigest,
    user: String,
    expires: Instant,
}

/// The process-wide session cap ([`MAX_SESSIONS`]) is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionLimit;

/// The live Web sessions. Cheap to clone; clones share the sessions.
#[derive(Clone)]
pub struct SessionStore {
    sessions: Arc<Mutex<HashMap<TokenDigest, Session>>>,
    max_sessions: usize,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::with_max_sessions(MAX_SESSIONS)
    }
}

impl fmt::Debug for SessionStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = self.lock().len();
        f.debug_struct("SessionStore")
            .field("sessions", &n)
            .finish()
    }
}

fn token_digest(token: &str) -> TokenDigest {
    arneb_state::sha256(token.as_bytes())
}

impl SessionStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty store with its own process-wide cap (tests use a small one).
    pub(crate) fn with_max_sessions(max_sessions: usize) -> Self {
        Self {
            sessions: Arc::default(),
            max_sessions,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<TokenDigest, Session>> {
        self.sessions.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Start a session for `user` and return its token, valid for
    /// [`SESSION_TTL`]. A user's [`MAX_SESSIONS_PER_USER`]-th session ends
    /// their oldest; with [`MAX_SESSIONS`] live in the process it fails.
    pub fn create(&self, user: &str) -> Result<String, SessionLimit> {
        self.create_at(user, Instant::now())
    }

    fn create_at(&self, user: &str, now: Instant) -> Result<String, SessionLimit> {
        let mut secret = [0u8; SECRET_LEN];
        // The system RNG failing is not recoverable for an auth service.
        aws_lc_rs::rand::fill(&mut secret).expect("system random number generator failed");
        let token = format!("{SESSION_PREFIX}{}", URL_SAFE_NO_PAD.encode(secret));
        let digest = token_digest(&token);
        let mut sessions = self.lock();
        sessions.retain(|_, s| s.expires > now);
        let mut mine: Vec<(Instant, TokenDigest)> = sessions
            .iter()
            .filter(|(_, s)| s.user == user)
            .map(|(d, s)| (s.expires, *d))
            .collect();
        // How many of the user's oldest sessions the new one replaces.
        let evict = (mine.len() + 1).saturating_sub(MAX_SESSIONS_PER_USER);
        // Check the global cap first (counting what the eviction frees), so
        // a refused sign-in never ends one of the user's live sessions.
        if sessions.len() - evict >= self.max_sessions {
            return Err(SessionLimit);
        }
        if evict > 0 {
            // Every session lives the same TTL, so the earliest expiry is
            // the oldest session.
            mine.sort();
            for (_, d) in &mine[..evict] {
                sessions.remove(d);
            }
        }
        sessions.insert(
            digest,
            Session {
                digest,
                user: user.to_owned(),
                expires: now + SESSION_TTL,
            },
        );
        Ok(token)
    }

    /// The user of the live session `token`, or `None`.
    pub fn resolve(&self, token: &str) -> Option<String> {
        self.resolve_at(token, Instant::now())
    }

    fn resolve_at(&self, token: &str, now: Instant) -> Option<String> {
        if !token.starts_with(SESSION_PREFIX) {
            return None;
        }
        let digest = token_digest(token);
        let mut sessions = self.lock();
        // The map lookup is on a SHA-256 of a 256-bit random secret, so its
        // timing reveals nothing usable; the stored digest is then compared
        // in constant time.
        let session = sessions.get(&digest)?;
        if constant_time::verify_slices_are_equal(&session.digest, &digest).is_err() {
            return None;
        }
        if session.expires <= now {
            sessions.remove(&digest);
            return None;
        }
        Some(session.user.clone())
    }

    /// End the session `token`; unknown tokens are ignored.
    pub fn revoke(&self, token: &str) {
        self.lock().remove(&token_digest(token));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sessions_resolve_until_revoked_or_expired() {
        let store = SessionStore::new();
        let token = store.create("alice").unwrap();
        let secret = token.strip_prefix(SESSION_PREFIX).unwrap();
        assert_eq!(secret.len(), 43);
        assert!(secret
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
        assert!(!arneb_protocol::is_token_shaped(&token));
        assert_eq!(store.resolve(&token).as_deref(), Some("alice"));
        assert_ne!(store.create("alice").unwrap(), token, "tokens are random");

        let mut wrong = token.clone();
        wrong.pop();
        wrong.push(if token.ends_with('A') { 'B' } else { 'A' });
        assert_eq!(store.resolve(&wrong), None);

        store.revoke(&token);
        assert_eq!(store.resolve(&token), None);

        let now = Instant::now();
        let token = store.create_at("bob", now).unwrap();
        assert_eq!(
            store.resolve_at(&token, now + SESSION_TTL / 2).as_deref(),
            Some("bob")
        );
        assert_eq!(store.resolve_at(&token, now + SESSION_TTL), None);
        assert_eq!(store.lock().len(), 1, "the expired session was pruned");
    }

    #[test]
    fn debug_does_not_show_tokens_or_users() {
        let store = SessionStore::new();
        let token = store.create("alice").unwrap();
        let shown = format!("{store:?}");
        assert!(
            !shown.contains(&token) && !shown.contains("alice"),
            "{shown}"
        );
    }

    #[test]
    fn a_user_keeps_at_most_sixteen_sessions_and_loses_the_oldest() {
        let store = SessionStore::new();
        let start = Instant::now();
        let tokens: Vec<String> = (0..MAX_SESSIONS_PER_USER)
            .map(|i| {
                store
                    .create_at("alice", start + Duration::from_secs(i as u64))
                    .unwrap()
            })
            .collect();
        let bob = store.create_at("bob", start).unwrap();
        let newest = store
            .create_at("alice", start + Duration::from_secs(100))
            .unwrap();
        let now = start + Duration::from_secs(101);
        assert_eq!(store.resolve_at(&tokens[0], now), None, "oldest evicted");
        assert_eq!(store.resolve_at(&tokens[1], now).as_deref(), Some("alice"));
        assert_eq!(store.resolve_at(&newest, now).as_deref(), Some("alice"));
        assert_eq!(store.resolve_at(&bob, now).as_deref(), Some("bob"));
        assert_eq!(store.lock().len(), MAX_SESSIONS_PER_USER + 1);
    }

    #[test]
    fn a_refused_sign_in_does_not_end_an_existing_session() {
        let store = SessionStore::with_max_sessions(MAX_SESSIONS_PER_USER + 1);
        let start = Instant::now();
        let alice: Vec<String> = (0..MAX_SESSIONS_PER_USER - 1)
            .map(|i| {
                store
                    .create_at("alice", start + Duration::from_secs(i as u64))
                    .unwrap()
            })
            .collect();
        store.create_at("bob", start).unwrap();
        store.create_at("carol", start).unwrap();
        // Full, and alice is under her own cap: refused, nothing evicted.
        let now = start + Duration::from_secs(100);
        assert_eq!(store.create_at("alice", now), Err(SessionLimit));
        assert_eq!(store.create_at("dave", now), Err(SessionLimit));
        assert_eq!(store.resolve_at(&alice[0], now).as_deref(), Some("alice"));
        assert_eq!(store.lock().len(), MAX_SESSIONS_PER_USER + 1);

        // A user at their own cap replaces their oldest even when the
        // process is full: the count does not grow.
        let store = SessionStore::with_max_sessions(MAX_SESSIONS_PER_USER);
        let mine: Vec<String> = (0..MAX_SESSIONS_PER_USER)
            .map(|i| {
                store
                    .create_at("alice", start + Duration::from_secs(i as u64))
                    .unwrap()
            })
            .collect();
        let newest = store.create_at("alice", now).unwrap();
        assert_eq!(store.resolve_at(&mine[0], now), None, "oldest replaced");
        assert_eq!(store.resolve_at(&mine[1], now).as_deref(), Some("alice"));
        assert_eq!(store.resolve_at(&newest, now).as_deref(), Some("alice"));
        assert_eq!(store.lock().len(), MAX_SESSIONS_PER_USER);
    }

    #[test]
    fn the_global_cap_refuses_new_sessions_until_one_ends() {
        let store = SessionStore::with_max_sessions(2);
        let a = store.create("alice").unwrap();
        store.create("bob").unwrap();
        assert_eq!(store.create("carol"), Err(SessionLimit));
        store.revoke(&a);
        assert!(store.create("carol").is_ok());
    }
}
