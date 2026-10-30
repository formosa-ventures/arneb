//! An in-test [`CredentialSource`] with API tokens, for the pgwire and Trino
//! auth tests (`arneb-protocol` cannot depend on `arneb-state`). Tokens are
//! issued in the documented `arneb_<16 hex>_<43 base64url>` format, and their
//! SCRAM verifiers share the owning user's salt and iteration count.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use arneb_protocol::{is_token_shaped, CredentialSource, ScramVerifier};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

#[derive(Debug)]
pub struct TokenSource {
    users: HashMap<String, ScramVerifier>,
    /// `(token, owner, verifier of the full token)`.
    tokens: Mutex<Vec<(String, String, ScramVerifier)>>,
}

impl TokenSource {
    pub fn new(users: &[(&str, &str)]) -> Self {
        Self {
            users: users
                .iter()
                .map(|(u, p)| (u.to_string(), ScramVerifier::generate(p).unwrap()))
                .collect(),
            tokens: Mutex::new(Vec::new()),
        }
    }

    /// Issue a token for `user`; returns the plaintext.
    pub fn issue(&self, user: &str) -> String {
        let mut id = [0u8; 8];
        let mut secret = [0u8; 32];
        aws_lc_rs::rand::fill(&mut id).unwrap();
        aws_lc_rs::rand::fill(&mut secret).unwrap();
        let id: String = id.iter().map(|b| format!("{b:02x}")).collect();
        let token = format!("arneb_{id}_{}", URL_SAFE_NO_PAD.encode(secret));
        assert!(is_token_shaped(&token));
        let pw = &self.users[user];
        let v = ScramVerifier::from_password(&token, pw.salt(), pw.iterations());
        self.tokens
            .lock()
            .unwrap()
            .push((token.clone(), user.to_string(), v));
        token
    }

    pub fn revoke(&self, token: &str) {
        self.tokens.lock().unwrap().retain(|(t, _, _)| t != token);
    }
}

impl CredentialSource for TokenSource {
    fn scram_verifiers(&self, user: &str) -> Vec<ScramVerifier> {
        let Some(pw) = self.users.get(user) else {
            return Vec::new();
        };
        let mut out = vec![pw.clone()];
        out.extend(
            self.tokens
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, u, _)| u == user)
                .map(|(_, _, v)| v.clone()),
        );
        out
    }

    fn verify_password(&self, user: &str, password: &str) -> bool {
        if is_token_shaped(password) {
            return self.verify_token(password).as_deref() == Some(user);
        }
        self.users
            .get(user)
            .is_some_and(|v| v.verify_password(password))
    }

    fn verify_token(&self, token: &str) -> Option<String> {
        self.tokens
            .lock()
            .unwrap()
            .iter()
            .find(|(t, _, _)| t == token)
            .map(|(_, u, _)| u.clone())
    }

    fn user_count(&self) -> usize {
        self.users.len()
    }
}
