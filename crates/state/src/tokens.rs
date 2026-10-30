//! API tokens: issue, list, revoke and (crate-internal) verify.

use arneb_protocol::ScramVerifier;
use aws_lc_rs::{digest, rand};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

use crate::file::{StateError, StateStore, TokenRecord};

/// Public facts about a token; never contains the secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenInfo {
    /// 16 lowercase hex characters; also the middle segment of the token.
    pub id: String,
    /// The user the token authenticates as.
    pub user: String,
    /// Optional human-readable label.
    pub name: Option<String>,
    /// Creation time, milliseconds since the Unix epoch.
    pub created_ms: u64,
    /// Expiry time, milliseconds since the Unix epoch; `None` never expires.
    pub expires_ms: Option<u64>,
}

/// A freshly issued token. `token` is shown once and never stored.
#[derive(Clone)]
pub struct IssuedToken {
    /// Public facts about the token.
    pub info: TokenInfo,
    /// The full `arneb_<id>_<secret>` string.
    pub token: String,
}

impl std::fmt::Debug for IssuedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IssuedToken")
            .field("info", &self.info)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl StateStore {
    /// Issues a token for `user`, optionally named and expiring at
    /// `expires_at_ms`. The returned plaintext is the only copy.
    pub fn issue_token(
        &self,
        user: &str,
        name: Option<String>,
        expires_at_ms: Option<u64>,
    ) -> Result<IssuedToken, StateError> {
        let mut id_bytes = [0u8; 8];
        let mut secret_bytes = [0u8; 32];
        rand::fill(&mut id_bytes).map_err(|_| StateError::Random)?;
        rand::fill(&mut secret_bytes).map_err(|_| StateError::Random)?;
        let id: String = id_bytes.iter().map(|b| format!("{b:02x}")).collect();
        let secret = URL_SAFE_NO_PAD.encode(secret_bytes);
        let token = format!("arneb_{id}_{secret}");
        let secret_sha256 = sha256(secret.as_bytes());
        let info = TokenInfo {
            id,
            user: user.to_string(),
            name,
            created_ms: now_ms(),
            expires_ms: expires_at_ms,
        };
        let path = self.path().map(|p| p.to_path_buf()).unwrap_or_default();
        // PBKDF2 runs outside the writer lock, against the user's verifier
        // from a snapshot. If that verifier changed before the write (e.g. a
        // reseed), derive again once from the new one.
        for attempt in 0..2 {
            let user_scram = self
                .snapshot()
                .users
                .iter()
                .find(|u| u.name == user)
                .map(|u| u.scram.clone())
                .ok_or_else(|| StateError::UnknownUser(user.to_string()))?;
            let verifier: ScramVerifier = user_scram.parse().map_err(|e| StateError::Corrupt {
                path: path.clone(),
                message: format!("user {user:?} has an invalid verifier: {e}"),
            })?;
            let scram =
                ScramVerifier::from_password(&token, verifier.salt(), verifier.iterations())
                    .to_string();
            let written = self.mutate(|data| {
                match data.users.iter().find(|u| u.name == user) {
                    None => return Err(StateError::UnknownUser(user.to_string())),
                    Some(u) if u.scram != user_scram => return Ok(false),
                    Some(_) => {}
                }
                // 64 random bits: a collision is practically impossible, but
                // never let one shadow an existing token.
                if data.tokens.iter().any(|t| t.id == info.id) {
                    return Err(StateError::Random);
                }
                data.tokens.push(TokenRecord {
                    id: info.id.clone(),
                    user: info.user.clone(),
                    name: info.name.clone(),
                    created_ms: info.created_ms,
                    expires_ms: info.expires_ms,
                    secret_sha256,
                    scram: scram.clone(),
                });
                Ok(true)
            })?;
            if written {
                return Ok(IssuedToken { info, token });
            }
            tracing::debug!(user = %user, attempt, "user verifier changed while issuing a token; retrying");
        }
        Err(StateError::UnknownUser(user.to_string()))
    }

    /// The tokens owned by `user`, oldest first. Never includes secrets.
    pub fn list_tokens(&self, user: &str) -> Vec<TokenInfo> {
        self.snapshot()
            .tokens
            .iter()
            .filter(|t| t.user == user)
            .map(info_of)
            .collect()
    }

    /// Revokes `token_id` if it belongs to `user`; otherwise (unknown id, or
    /// another user's token) fails with [`StateError::TokenNotFound`].
    pub fn revoke_token(&self, user: &str, token_id: &str) -> Result<(), StateError> {
        self.mutate(|data| {
            let before = data.tokens.len();
            data.tokens
                .retain(|t| !(t.id == token_id && t.user == user));
            if data.tokens.len() == before {
                return Err(StateError::TokenNotFound(token_id.to_string()));
            }
            Ok(())
        })
    }

    /// Looks up `token` and checks its secret against the stored hash in
    /// constant time. Returns the token's facts only when it is live at
    /// `now_ms` (not expired). An unknown id still runs the comparison,
    /// against a fixed dummy hash.
    pub(crate) fn check_token_at(&self, token: &str, now_ms: u64) -> Option<TokenInfo> {
        if !arneb_protocol::is_token_shaped(token) {
            return None;
        }
        let (id, secret) = token["arneb_".len()..].split_once('_')?;
        let presented = sha256(secret.as_bytes());
        let guard = self.snapshot();
        let record = guard.tokens.iter().find(|t| t.id == id);
        let expected = record.map_or(&DUMMY_SECRET_SHA256, |r| &r.secret_sha256);
        let matches =
            aws_lc_rs::constant_time::verify_slices_are_equal(&presented, expected).is_ok();
        let record = record.filter(|r| matches && is_live(r, now_ms))?;
        // A token never outlives its user.
        guard
            .users
            .iter()
            .any(|u| u.name == record.user)
            .then(|| info_of(record))
    }

    /// [`StateStore::check_token_at`] at the current time.
    pub(crate) fn check_token(&self, token: &str) -> Option<TokenInfo> {
        self.check_token_at(token, now_ms())
    }
}

/// Compared against when a token id is unknown, so a miss costs the same
/// as a hit. No secret hashes to it in practice.
const DUMMY_SECRET_SHA256: [u8; 32] = [0u8; 32];

/// Whether `t` has not expired at `now_ms`.
pub(crate) fn is_live(t: &TokenRecord, now_ms: u64) -> bool {
    t.expires_ms.is_none_or(|e| now_ms < e)
}

fn info_of(t: &TokenRecord) -> TokenInfo {
    TokenInfo {
        id: t.id.clone(),
        user: t.user.clone(),
        name: t.name.clone(),
        created_ms: t.created_ms,
        expires_ms: t.expires_ms,
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(digest::digest(&digest::SHA256, bytes).as_ref());
    out
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_protocol::is_token_shaped;

    fn store_with_alice() -> StateStore {
        let store = StateStore::open(None).unwrap();
        let v = ScramVerifier::from_password("pw", b"0123456789abcdef", 4096);
        store.seed_users(&[("alice".into(), v)], false).unwrap();
        store
    }

    #[test]
    fn issued_token_has_the_documented_shape() {
        let store = store_with_alice();
        let t = store.issue_token("alice", Some("ci".into()), None).unwrap();
        assert!(is_token_shaped(&t.token), "{}", t.token);
        let (id, secret) = t.token["arneb_".len()..].split_once('_').unwrap();
        assert_eq!(id.len(), 16);
        assert!(id
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
        assert_eq!(secret.len(), 43);
        assert_eq!(t.info.id, id);
        assert_eq!(t.info.user, "alice");
        assert_eq!(t.info.name.as_deref(), Some("ci"));
        assert!(t.info.created_ms > 0);
        assert_eq!(t.info.expires_ms, None);
        let u = store.issue_token("alice", None, Some(5)).unwrap();
        assert_ne!(u.token, t.token);
        assert_eq!(u.info.expires_ms, Some(5));
    }

    #[test]
    fn debug_output_redacts_the_token() {
        let store = store_with_alice();
        let t = store.issue_token("alice", None, None).unwrap();
        let secret = t.token.rsplit_once('_').unwrap().1;
        let dbg = format!("{t:?}");
        assert!(!dbg.contains(&t.token) && !dbg.contains(secret), "{dbg}");
        assert!(dbg.contains(&t.info.id));
    }

    #[test]
    fn token_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        let v = ScramVerifier::from_password("pw", b"0123456789abcdef", 4096);
        store.seed_users(&[("alice".into(), v)], false).unwrap();
        let t = store.issue_token("alice", None, None).unwrap();
        drop(store);
        let store = StateStore::open(Some(&p)).unwrap();
        assert_eq!(store.list_tokens("alice"), vec![t.info.clone()]);
        assert_eq!(store.check_token(&t.token), Some(t.info));
    }

    #[test]
    fn plaintext_token_is_not_in_the_state_file() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        let store = StateStore::open(Some(&p)).unwrap();
        let v = ScramVerifier::from_password("pw", b"0123456789abcdef", 4096);
        store.seed_users(&[("alice".into(), v)], false).unwrap();
        let t = store.issue_token("alice", None, None).unwrap();
        let secret = t.token.rsplit_once('_').unwrap().1;
        let bytes = std::fs::read(&p).unwrap();
        let text = String::from_utf8_lossy(&bytes);
        assert!(!text.contains(secret));
        assert!(!text.contains(&t.token));
        assert!(text.contains(&t.info.id));
    }

    #[test]
    fn revoke_removes_only_that_token() {
        let store = store_with_alice();
        let a = store.issue_token("alice", None, None).unwrap();
        let b = store.issue_token("alice", None, None).unwrap();
        store.revoke_token("alice", &a.info.id).unwrap();
        assert_eq!(store.list_tokens("alice"), vec![b.info.clone()]);
        assert_eq!(store.check_token(&a.token), None);
        assert_eq!(store.check_token(&b.token), Some(b.info));
        assert!(matches!(
            store.revoke_token("alice", &a.info.id),
            Err(StateError::TokenNotFound(_))
        ));
    }

    #[test]
    fn revoking_someone_elses_token_is_not_found() {
        let store = store_with_alice();
        let v = ScramVerifier::from_password("pw", b"fedcba9876543210", 4096);
        store.seed_users(&[("bob".into(), v)], false).unwrap();
        let t = store.issue_token("alice", None, None).unwrap();
        assert!(matches!(
            store.revoke_token("bob", &t.info.id),
            Err(StateError::TokenNotFound(_))
        ));
        assert_eq!(store.list_tokens("alice").len(), 1);
        assert!(store.list_tokens("bob").is_empty());
    }

    #[test]
    fn issuing_for_an_unknown_user_fails() {
        let store = store_with_alice();
        assert!(matches!(
            store.issue_token("nobody", None, None),
            Err(StateError::UnknownUser(u)) if u == "nobody"
        ));
    }

    #[test]
    fn token_scram_verifier_shares_the_users_salt() {
        let store = store_with_alice();
        let t = store.issue_token("alice", None, None).unwrap();
        let (user_v, tok_v) = store
            .mutate(|d| {
                Ok((
                    d.users[0].scram.parse::<ScramVerifier>().unwrap(),
                    d.tokens[0].scram.parse::<ScramVerifier>().unwrap(),
                ))
            })
            .unwrap();
        assert_eq!(user_v.salt(), tok_v.salt());
        assert_eq!(user_v.iterations(), tok_v.iterations());
        assert!(tok_v.verify_password(&t.token));
        assert!(!user_v.verify_password(&t.token));
    }

    #[test]
    fn check_token_rejects_a_wrong_secret() {
        let store = store_with_alice();
        let t = store.issue_token("alice", None, None).unwrap();
        let mut bad = t.token.clone();
        let last = bad.pop().unwrap();
        bad.push(if last == 'A' { 'B' } else { 'A' });
        assert_eq!(store.check_token(&bad), None);
        assert_eq!(store.check_token("garbage"), None);
    }
}
