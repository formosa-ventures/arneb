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

use aws_lc_rs::{constant_time, digest};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;

/// How long a session stays valid after it is created.
// Sessions are created and revoked by `/api/v2/sessions`, mounted next; until
// then only the middleware's `resolve` is called outside tests.
#[allow(dead_code)]
pub const SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);

/// Prefix of every session token.
pub const SESSION_PREFIX: &str = "arneb_s_";

/// Random bytes in a session secret (43 base64url characters).
#[allow(dead_code)]
const SECRET_LEN: usize = 32;

type TokenDigest = [u8; 32];

struct Session {
    /// The full digest again, compared in constant time after the lookup.
    digest: TokenDigest,
    user: String,
    expires: Instant,
}

/// The live Web sessions. Cheap to clone; clones share the sessions.
#[derive(Clone, Default)]
pub struct SessionStore {
    sessions: Arc<Mutex<HashMap<TokenDigest, Session>>>,
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
    let mut out = [0u8; 32];
    out.copy_from_slice(digest::digest(&digest::SHA256, token.as_bytes()).as_ref());
    out
}

#[allow(dead_code)] // `create` / `revoke`: see `SESSION_TTL`.
impl SessionStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<TokenDigest, Session>> {
        self.sessions.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Start a session for `user` and return its token, valid for
    /// [`SESSION_TTL`].
    pub fn create(&self, user: &str) -> String {
        self.create_at(user, Instant::now())
    }

    fn create_at(&self, user: &str, now: Instant) -> String {
        let mut secret = [0u8; SECRET_LEN];
        // The system RNG failing is not recoverable for an auth service.
        aws_lc_rs::rand::fill(&mut secret).expect("system random number generator failed");
        let token = format!("{SESSION_PREFIX}{}", URL_SAFE_NO_PAD.encode(secret));
        let digest = token_digest(&token);
        let mut sessions = self.lock();
        sessions.retain(|_, s| s.expires > now);
        sessions.insert(
            digest,
            Session {
                digest,
                user: user.to_owned(),
                expires: now + SESSION_TTL,
            },
        );
        token
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
        let token = store.create("alice");
        let secret = token.strip_prefix(SESSION_PREFIX).unwrap();
        assert_eq!(secret.len(), 43);
        assert!(secret
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
        assert!(!arneb_protocol::is_token_shaped(&token));
        assert_eq!(store.resolve(&token).as_deref(), Some("alice"));
        assert_ne!(store.create("alice"), token, "tokens are random");

        let mut wrong = token.clone();
        wrong.pop();
        wrong.push(if token.ends_with('A') { 'B' } else { 'A' });
        assert_eq!(store.resolve(&wrong), None);

        store.revoke(&token);
        assert_eq!(store.resolve(&token), None);

        let now = Instant::now();
        let token = store.create_at("bob", now);
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
        let token = store.create("alice");
        let shown = format!("{store:?}");
        assert!(
            !shown.contains(&token) && !shown.contains("alice"),
            "{shown}"
        );
    }
}
