//! Client authentication for the PostgreSQL wire protocol.
//!
//! Two modes are supported:
//!
//! - [`AuthMethod::None`] (default): every connection is accepted without a
//!   password challenge, matching the historical behavior.
//! - [`AuthMethod::ScramSha256`]: clients must complete a SCRAM-SHA-256
//!   exchange (RFC 5802 / RFC 7677), the same mechanism PostgreSQL uses for
//!   `password_encryption = scram-sha-256`. The server stores only
//!   PostgreSQL-compatible SCRAM verifiers
//!   (`SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>`), never the
//!   plaintext password, and the password itself never crosses the wire.
//!
//! Every failure during the authentication phase is reported with severity
//! `FATAL` so the connection is closed; a wrong password or unknown user maps
//! to SQLSTATE `28P01` (`invalid_password`).

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use aws_lc_rs::{constant_time, digest, hmac, pbkdf2};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bytes::Bytes;
use futures::{Sink, SinkExt};
use pgwire::api::auth::{
    finish_authentication, save_startup_parameters_to_metadata, DefaultServerParameterProvider,
    StartupHandler,
};
use pgwire::api::{ClientInfo, PgWireConnectionState, METADATA_USER};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

/// SASL mechanism name advertised to clients.
pub const SCRAM_SHA_256: &str = "SCRAM-SHA-256";

/// Iteration count used by [`ScramVerifier::generate`]; matches PostgreSQL's
/// default `scram_iterations`.
pub const DEFAULT_SCRAM_ITERATIONS: u32 = 4096;

const SALT_LEN: usize = 16;
const KEY_LEN: usize = 32;

/// Errors raised while building an authentication configuration.
#[derive(Debug, thiserror::Error)]
pub enum AuthConfigError {
    /// The stored verifier string is not a valid SCRAM-SHA-256 verifier.
    #[error("invalid SCRAM-SHA-256 verifier: {0}")]
    InvalidVerifier(String),
    /// The same user name was configured twice.
    #[error("duplicate user '{0}' in auth configuration")]
    DuplicateUser(String),
    /// A user entry has an empty name.
    #[error("user name must not be empty")]
    EmptyUserName,
    /// Password authentication was requested without any users.
    #[error("password authentication requires at least one user")]
    NoUsers,
    /// The system random number generator failed.
    #[error("failed to generate random bytes")]
    Random,
}

/// A PostgreSQL-compatible SCRAM-SHA-256 password verifier.
///
/// Textual form (identical to `pg_authid.rolpassword`):
/// `SCRAM-SHA-256$<iterations>:<base64 salt>$<base64 StoredKey>:<base64 ServerKey>`.
#[derive(Clone, PartialEq, Eq)]
pub struct ScramVerifier {
    iterations: u32,
    salt: Vec<u8>,
    stored_key: [u8; KEY_LEN],
    server_key: [u8; KEY_LEN],
}

impl Debug for ScramVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramVerifier")
            .field("iterations", &self.iterations)
            .field("stored_key", &"<redacted>")
            .field("server_key", &"<redacted>")
            .finish()
    }
}

impl ScramVerifier {
    /// Derive a verifier from a plaintext password with an explicit salt and
    /// iteration count.
    pub fn from_password(password: &str, salt: &[u8], iterations: u32) -> Self {
        // SASLprep the password like PostgreSQL does; fall back to the raw
        // bytes when normalization fails (also PostgreSQL's behavior).
        let normalized = stringprep::saslprep(password)
            .map(|p| p.into_owned())
            .unwrap_or_else(|_| password.to_owned());
        let iterations = iterations.max(1);
        let mut salted = [0u8; KEY_LEN];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(iterations).expect("iterations clamped to >= 1"),
            salt,
            normalized.as_bytes(),
            &mut salted,
        );
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac_sha256(&salted, b"Server Key");
        Self {
            iterations,
            salt: salt.to_vec(),
            stored_key,
            server_key,
        }
    }

    /// Derive a verifier from a plaintext password using a fresh random salt
    /// and [`DEFAULT_SCRAM_ITERATIONS`].
    pub fn generate(password: &str) -> Result<Self, AuthConfigError> {
        let mut salt = [0u8; SALT_LEN];
        aws_lc_rs::rand::fill(&mut salt).map_err(|_| AuthConfigError::Random)?;
        Ok(Self::from_password(
            password,
            &salt,
            DEFAULT_SCRAM_ITERATIONS,
        ))
    }

    /// PBKDF2 iteration count of this verifier.
    pub fn iterations(&self) -> u32 {
        self.iterations
    }

    /// Check a client proof against this verifier (constant time).
    fn verify_client_proof(&self, auth_message: &[u8], client_proof: &[u8]) -> bool {
        if client_proof.len() != KEY_LEN {
            return false;
        }
        let client_signature = hmac_sha256(&self.stored_key, auth_message);
        let mut client_key = [0u8; KEY_LEN];
        for (i, b) in client_key.iter_mut().enumerate() {
            *b = client_proof[i] ^ client_signature[i];
        }
        constant_time::verify_slices_are_equal(&sha256(&client_key), &self.stored_key).is_ok()
    }

    fn server_signature(&self, auth_message: &[u8]) -> [u8; KEY_LEN] {
        hmac_sha256(&self.server_key, auth_message)
    }
}

impl fmt::Display for ScramVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{SCRAM_SHA_256}${}:{}${}:{}",
            self.iterations,
            STANDARD.encode(&self.salt),
            STANDARD.encode(self.stored_key),
            STANDARD.encode(self.server_key)
        )
    }
}

impl FromStr for ScramVerifier {
    type Err = AuthConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = |msg: &str| AuthConfigError::InvalidVerifier(msg.to_string());
        let mut parts = s.trim().splitn(3, '$');
        let (Some(method), Some(params), Some(keys)) = (parts.next(), parts.next(), parts.next())
        else {
            return Err(invalid(
                "expected SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>",
            ));
        };
        if method != SCRAM_SHA_256 {
            return Err(invalid("must start with 'SCRAM-SHA-256$'"));
        }
        let (iterations, salt) = params
            .split_once(':')
            .ok_or_else(|| invalid("missing ':' between iterations and salt"))?;
        let iterations: u32 = iterations
            .parse()
            .ok()
            .filter(|i| *i > 0)
            .ok_or_else(|| invalid("iterations must be a positive integer"))?;
        let salt = STANDARD
            .decode(salt)
            .map_err(|_| invalid("salt is not valid base64"))?;
        if salt.is_empty() {
            return Err(invalid("salt must not be empty"));
        }
        let (stored_key, server_key) = keys
            .split_once(':')
            .ok_or_else(|| invalid("missing ':' between StoredKey and ServerKey"))?;
        let decode_key = |k: &str, name: &str| -> Result<[u8; KEY_LEN], AuthConfigError> {
            STANDARD
                .decode(k)
                .ok()
                .and_then(|v| <[u8; KEY_LEN]>::try_from(v).ok())
                .ok_or_else(|| invalid(&format!("{name} must be 32 bytes of base64")))
        };
        Ok(Self {
            iterations,
            salt,
            stored_key: decode_key(stored_key, "StoredKey")?,
            server_key: decode_key(server_key, "ServerKey")?,
        })
    }
}

/// The set of users allowed to connect under password authentication.
pub struct UserCredentials {
    users: HashMap<String, ScramVerifier>,
    /// Per-process secret used to derive stable fake salts for unknown users,
    /// so probing does not reveal which user names exist.
    mock_secret: [u8; KEY_LEN],
}

impl Debug for UserCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names: Vec<&String> = self.users.keys().collect();
        names.sort();
        f.debug_struct("UserCredentials")
            .field("users", &names)
            .finish()
    }
}

impl UserCredentials {
    /// Build a credential store from `(user name, verifier)` pairs.
    ///
    /// Fails on an empty list, an empty user name, or a duplicate user name.
    pub fn new(
        users: impl IntoIterator<Item = (String, ScramVerifier)>,
    ) -> Result<Self, AuthConfigError> {
        let mut map = HashMap::new();
        for (name, verifier) in users {
            if name.is_empty() {
                return Err(AuthConfigError::EmptyUserName);
            }
            if map.insert(name.clone(), verifier).is_some() {
                return Err(AuthConfigError::DuplicateUser(name));
            }
        }
        if map.is_empty() {
            return Err(AuthConfigError::NoUsers);
        }
        let mut mock_secret = [0u8; KEY_LEN];
        aws_lc_rs::rand::fill(&mut mock_secret).map_err(|_| AuthConfigError::Random)?;
        Ok(Self {
            users: map,
            mock_secret,
        })
    }

    /// Number of configured users.
    pub fn len(&self) -> usize {
        self.users.len()
    }

    /// Whether no users are configured (never true for a constructed store).
    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }

    /// Verifier for `user`, or a deterministic decoy verifier (and `false`)
    /// when the user does not exist.
    fn lookup(&self, user: &str) -> (ScramVerifier, bool) {
        match self.users.get(user) {
            Some(v) => (v.clone(), true),
            None => {
                let salt = hmac_sha256(&self.mock_secret, user.as_bytes());
                let keys = hmac_sha256(&self.mock_secret, &salt);
                (
                    ScramVerifier {
                        iterations: DEFAULT_SCRAM_ITERATIONS,
                        salt: salt[..SALT_LEN].to_vec(),
                        stored_key: keys,
                        server_key: keys,
                    },
                    false,
                )
            }
        }
    }
}

/// How clients authenticate on the pgwire port.
#[derive(Debug, Clone, Default)]
pub enum AuthMethod {
    /// Accept every connection without a password (default).
    #[default]
    None,
    /// Require SCRAM-SHA-256 password authentication.
    ScramSha256(Arc<UserCredentials>),
}

impl AuthMethod {
    /// Short name of the mode for logging.
    pub fn name(&self) -> &'static str {
        match self {
            AuthMethod::None => "none",
            AuthMethod::ScramSha256(_) => "password (scram-sha-256)",
        }
    }
}

// ---------------------------------------------------------------------------
// SCRAM exchange
// ---------------------------------------------------------------------------

/// State kept between the client-first and client-final messages.
#[derive(Debug)]
struct ScramExchange {
    user: String,
    known_user: bool,
    verifier: ScramVerifier,
    gs2_header: String,
    client_first_bare: String,
    server_first: String,
    nonce: String,
}

#[derive(Debug)]
enum ScramState {
    /// Waiting for the StartupMessage.
    Start,
    /// AuthenticationSASL sent; waiting for SASLInitialResponse.
    AwaitingClientFirst,
    /// SASLContinue sent; waiting for SASLResponse.
    AwaitingClientFinal(Box<ScramExchange>),
    /// Exchange completed or aborted.
    Done,
}

fn protocol_violation(msg: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_owned(),
        "08P01".to_owned(),
        msg.into(),
    )))
}

fn invalid_password(user: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_owned(),
        "28P01".to_owned(),
        format!("password authentication failed for user \"{user}\""),
    )))
}

/// Force any error raised during authentication to `FATAL`. pgwire moves a
/// connection to `ReadyForQuery` after a non-fatal error, which during the
/// startup phase would skip authentication entirely.
fn ensure_fatal(err: PgWireError) -> PgWireError {
    let mut info: ErrorInfo = err.into();
    if !info.is_fatal() {
        info.severity = "FATAL".to_owned();
    }
    PgWireError::UserError(Box::new(info))
}

/// Parse a SASLInitialResponse payload (`gs2-header client-first-message-bare`).
///
/// Returns `(gs2_header, client_first_bare, client_nonce)`.
fn parse_client_first(data: &[u8]) -> PgWireResult<(String, String, String)> {
    let msg = std::str::from_utf8(data)
        .map_err(|_| protocol_violation("malformed SCRAM message: not valid UTF-8"))?;
    let mut parts = msg.splitn(3, ',');
    let (Some(cbind), Some(authzid), Some(bare)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(protocol_violation("malformed SCRAM client-first-message"));
    };
    match cbind {
        // "n": client does not support channel binding; "y": client supports
        // it but believes the server does not (we never advertise -PLUS).
        "n" | "y" => {}
        _ if cbind.starts_with("p=") => {
            return Err(protocol_violation(
                "SCRAM channel binding is not supported by this server",
            ))
        }
        _ => return Err(protocol_violation("malformed SCRAM gs2 header")),
    }
    if !authzid.is_empty() {
        return Err(protocol_violation(
            "SCRAM authorization identity is not supported",
        ));
    }
    let mut attrs = bare.split(',');
    // The user name inside the SCRAM message is ignored (like PostgreSQL);
    // the StartupMessage `user` parameter is authoritative.
    match attrs.next() {
        Some(a) if a.starts_with("n=") => {}
        _ => return Err(protocol_violation("malformed SCRAM client-first-message")),
    }
    let nonce = match attrs.next() {
        Some(a) if a.len() > 2 && a.starts_with("r=") => &a[2..],
        _ => return Err(protocol_violation("SCRAM client nonce missing")),
    };
    if !nonce
        .bytes()
        .all(|b| (0x21..=0x7e).contains(&b) && b != b',')
    {
        return Err(protocol_violation("SCRAM client nonce is not printable"));
    }
    Ok((
        format!("{cbind},{authzid},"),
        bare.to_owned(),
        nonce.to_owned(),
    ))
}

/// Parse a client-final-message. Returns `(without_proof, proof bytes)`.
fn parse_client_final(data: &[u8], exchange: &ScramExchange) -> PgWireResult<(String, Vec<u8>)> {
    let msg = std::str::from_utf8(data)
        .map_err(|_| protocol_violation("malformed SCRAM message: not valid UTF-8"))?;
    let (without_proof, proof) = msg
        .rsplit_once(",p=")
        .ok_or_else(|| protocol_violation("SCRAM client proof missing"))?;
    let mut attrs = without_proof.split(',');
    let channel_binding = attrs
        .next()
        .and_then(|a| a.strip_prefix("c="))
        .ok_or_else(|| protocol_violation("SCRAM channel binding attribute missing"))?;
    if channel_binding != STANDARD.encode(exchange.gs2_header.as_bytes()) {
        return Err(protocol_violation("SCRAM channel binding check failed"));
    }
    let nonce = attrs
        .next()
        .and_then(|a| a.strip_prefix("r="))
        .ok_or_else(|| protocol_violation("SCRAM nonce attribute missing"))?;
    if nonce != exchange.nonce {
        return Err(protocol_violation("SCRAM nonce mismatch"));
    }
    let proof = STANDARD
        .decode(proof)
        .map_err(|_| protocol_violation("SCRAM client proof is not valid base64"))?;
    Ok((without_proof.to_owned(), proof))
}

/// Per-connection startup handler that enforces the configured
/// [`AuthMethod`]. A fresh instance is created for every connection.
pub(crate) struct AuthStartupHandler {
    method: AuthMethod,
    state: Mutex<ScramState>,
}

impl AuthStartupHandler {
    pub(crate) fn new(method: AuthMethod) -> Self {
        Self {
            method,
            state: Mutex::new(ScramState::Start),
        }
    }

    fn take_state(&self) -> ScramState {
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::replace(&mut *guard, ScramState::Done)
    }

    fn set_state(&self, state: ScramState) {
        *self.state.lock().unwrap_or_else(|e| e.into_inner()) = state;
    }

    async fn scram_step<C>(
        &self,
        creds: &UserCredentials,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match (self.take_state(), message) {
            (ScramState::Start, PgWireFrontendMessage::Startup(ref startup)) => {
                save_startup_parameters_to_metadata(client, startup);
                if client.metadata().get(METADATA_USER).is_none() {
                    return Err(PgWireError::UserNameRequired);
                }
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                        vec![SCRAM_SHA_256.to_owned()],
                    )))
                    .await?;
                self.set_state(ScramState::AwaitingClientFirst);
                Ok(())
            }
            (
                ScramState::AwaitingClientFirst,
                PgWireFrontendMessage::PasswordMessageFamily(msg),
            ) => {
                let initial = msg.into_sasl_initial_response()?;
                if initial.auth_method != SCRAM_SHA_256 {
                    return Err(protocol_violation(format!(
                        "unsupported SASL mechanism \"{}\"",
                        initial.auth_method
                    )));
                }
                let data = initial
                    .data
                    .ok_or_else(|| protocol_violation("empty SCRAM client-first-message"))?;
                let (gs2_header, client_first_bare, client_nonce) = parse_client_first(&data)?;

                let user = client
                    .metadata()
                    .get(METADATA_USER)
                    .cloned()
                    .ok_or(PgWireError::UserNameRequired)?;
                let (verifier, known_user) = creds.lookup(&user);

                let mut server_nonce = [0u8; 18];
                aws_lc_rs::rand::fill(&mut server_nonce)
                    .map_err(|_| protocol_violation("failed to generate SCRAM nonce"))?;
                let nonce = format!("{client_nonce}{}", STANDARD.encode(server_nonce));
                let server_first = format!(
                    "r={nonce},s={},i={}",
                    STANDARD.encode(&verifier.salt),
                    verifier.iterations
                );
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::SASLContinue(Bytes::from(server_first.clone())),
                    ))
                    .await?;
                self.set_state(ScramState::AwaitingClientFinal(Box::new(ScramExchange {
                    user,
                    known_user,
                    verifier,
                    gs2_header,
                    client_first_bare,
                    server_first,
                    nonce,
                })));
                Ok(())
            }
            (
                ScramState::AwaitingClientFinal(exchange),
                PgWireFrontendMessage::PasswordMessageFamily(msg),
            ) => {
                let response = msg.into_sasl_response()?;
                let (without_proof, proof) = parse_client_final(&response.data, &exchange)?;
                let auth_message = format!(
                    "{},{},{}",
                    exchange.client_first_bare, exchange.server_first, without_proof
                );
                let proof_ok = exchange
                    .verifier
                    .verify_client_proof(auth_message.as_bytes(), &proof);
                if !(proof_ok && exchange.known_user) {
                    tracing::warn!(
                        user = %exchange.user,
                        peer = %client.socket_addr(),
                        "pgwire password authentication failed"
                    );
                    return Err(invalid_password(&exchange.user));
                }
                let signature = exchange.verifier.server_signature(auth_message.as_bytes());
                let server_final = format!("v={}", STANDARD.encode(signature));
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::SASLFinal(Bytes::from(server_final)),
                    ))
                    .await?;
                tracing::debug!(
                    user = %exchange.user,
                    peer = %client.socket_addr(),
                    "pgwire client authenticated"
                );
                finish_authentication(client, &DefaultServerParameterProvider::default()).await
            }
            (_, _) => Err(protocol_violation(
                "unexpected message during authentication",
            )),
        }
    }
}

#[async_trait]
impl StartupHandler for AuthStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match &self.method {
            AuthMethod::None => {
                if let PgWireFrontendMessage::Startup(ref startup) = message {
                    save_startup_parameters_to_metadata(client, startup);
                    finish_authentication(client, &DefaultServerParameterProvider::default())
                        .await?;
                }
                Ok(())
            }
            AuthMethod::ScramSha256(creds) => self
                .scram_step(creds, client, message)
                .await
                .map_err(ensure_fatal),
        }
    }
}

fn sha256(data: &[u8]) -> [u8; KEY_LEN] {
    let d = digest::digest(&digest::SHA256, data);
    let mut out = [0u8; KEY_LEN];
    out.copy_from_slice(d.as_ref());
    out
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; KEY_LEN] {
    let tag = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data);
    let mut out = [0u8; KEY_LEN];
    out.copy_from_slice(tag.as_ref());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Client side of SCRAM-SHA-256, used to drive the verifier in tests.
    fn client_proof(password: &str, salt: &[u8], iterations: u32, auth_message: &str) -> Vec<u8> {
        let mut salted = [0u8; KEY_LEN];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(iterations).unwrap(),
            salt,
            password.as_bytes(),
            &mut salted,
        );
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        client_key
            .iter()
            .zip(signature.iter())
            .map(|(a, b)| a ^ b)
            .collect()
    }

    #[test]
    fn rfc7677_test_vector() {
        // RFC 7677 section 3: user "user", password "pencil".
        let salt = STANDARD.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let verifier = ScramVerifier::from_password("pencil", &salt, 4096);
        let auth_message = "n=user,r=rOprNGfwEbeRWgbNEkqO,\
            r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
            s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,\
            c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let proof = STANDARD
            .decode("dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=")
            .unwrap();
        assert!(verifier.verify_client_proof(auth_message.as_bytes(), &proof));
        assert_eq!(
            STANDARD.encode(verifier.server_signature(auth_message.as_bytes())),
            "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );
    }

    #[test]
    fn verifier_round_trips_through_text() {
        let v = ScramVerifier::generate("s3cret").unwrap();
        let text = v.to_string();
        assert!(text.starts_with("SCRAM-SHA-256$4096:"));
        let parsed: ScramVerifier = text.parse().unwrap();
        assert_eq!(parsed, v);
    }

    #[test]
    fn correct_password_verifies_and_wrong_password_fails() {
        let salt = b"0123456789abcdef";
        let v = ScramVerifier::from_password("hunter2", salt, 4096);
        let auth = "n=,r=abc,r=abcdef,s=MDEyMzQ1Njc4OWFiY2RlZg==,i=4096,c=biws,r=abcdef";
        let good = client_proof("hunter2", salt, 4096, auth);
        let bad = client_proof("hunter3", salt, 4096, auth);
        assert!(v.verify_client_proof(auth.as_bytes(), &good));
        assert!(!v.verify_client_proof(auth.as_bytes(), &bad));
        assert!(!v.verify_client_proof(auth.as_bytes(), &good[..31]));
    }

    #[test]
    fn rejects_malformed_verifiers() {
        for bad in [
            "",
            "md5abcdef",
            "SCRAM-SHA-256$4096:c2FsdA==",
            "SCRAM-SHA-256$0:c2FsdA==$AAAA:BBBB",
            "SCRAM-SHA-256$4096:!!!$AAAA:BBBB",
            "SCRAM-SHA-256$4096:c2FsdA==$AAAA:BBBB",
            "SCRAM-SHA-1$4096:c2FsdA==$AAAA:BBBB",
        ] {
            assert!(
                bad.parse::<ScramVerifier>().is_err(),
                "expected '{bad}' to be rejected"
            );
        }
    }

    #[test]
    fn credentials_reject_duplicates_and_empty() {
        let v = ScramVerifier::generate("x").unwrap();
        assert!(matches!(
            UserCredentials::new(Vec::new()),
            Err(AuthConfigError::NoUsers)
        ));
        assert!(matches!(
            UserCredentials::new(vec![(String::new(), v.clone())]),
            Err(AuthConfigError::EmptyUserName)
        ));
        assert!(matches!(
            UserCredentials::new(vec![("a".into(), v.clone()), ("a".into(), v)]),
            Err(AuthConfigError::DuplicateUser(u)) if u == "a"
        ));
    }

    #[test]
    fn unknown_user_gets_stable_decoy_salt() {
        let creds = UserCredentials::new(vec![(
            "alice".into(),
            ScramVerifier::generate("pw").unwrap(),
        )])
        .unwrap();
        let (real, known) = creds.lookup("alice");
        assert!(known);
        assert_eq!(real.iterations(), DEFAULT_SCRAM_ITERATIONS);
        let (d1, k1) = creds.lookup("mallory");
        let (d2, _) = creds.lookup("mallory");
        assert!(!k1);
        assert_eq!(d1.salt, d2.salt, "decoy salt must be stable per user");
        assert_ne!(d1.salt, creds.lookup("eve").0.salt);
    }

    #[test]
    fn client_first_parsing() {
        let (gs2, bare, nonce) = parse_client_first(b"n,,n=,r=abcDEF123").unwrap();
        assert_eq!(gs2, "n,,");
        assert_eq!(bare, "n=,r=abcDEF123");
        assert_eq!(nonce, "abcDEF123");
        assert!(parse_client_first(b"y,,n=u,r=xyz").is_ok());
        assert!(parse_client_first(b"p=tls-server-end-point,,n=u,r=xyz").is_err());
        assert!(parse_client_first(b"n,a=admin,n=u,r=xyz").is_err());
        assert!(parse_client_first(b"n,,n=u").is_err());
        assert!(parse_client_first(b"garbage").is_err());
    }

    #[test]
    fn auth_errors_are_fatal() {
        assert!(ErrorInfo::from(invalid_password("bob")).is_fatal());
        let info = ErrorInfo::from(invalid_password("bob"));
        assert_eq!(info.code, "28P01");
        // A non-fatal pgwire error must be promoted so the connection closes.
        let promoted = ErrorInfo::from(ensure_fatal(PgWireError::InvalidSASLState));
        assert!(promoted.is_fatal());
    }
}
