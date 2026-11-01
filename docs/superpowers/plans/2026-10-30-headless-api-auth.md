# H1 `headless-api-auth` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** H1 delivers the following:
- a durable coordinator state store, seeded from the config file
- per-user API tokens that work on every protocol
- authentication on every Web-port route
- a Web UI login
- the `/api/v2` skeleton with structured errors

**Architecture:**
- **State crate.** A new crate, `arneb-state`, owns a versioned JSON state file. It holds users and API tokens, and implements a new `CredentialSource` trait defined in `arneb-protocol`.
- **One credential source.** pgwire SCRAM, Trino Basic, and the Web port all authenticate against that one source.
- **Token verifiers.** A token is stored twice:
  - as a SHA-256 hash, for the HTTP fast path
  - as a SCRAM verifier derived with the owning user's salt, so a token also works as a pgwire password
- **Web port.** The Web port gets an axum auth middleware. It also gets a new `/api/v2` router whose errors use one JSON envelope.

**Tech Stack:** Rust (axum 0.8, serde_json, aws-lc-rs, thiserror, tracing), React + TypeScript (web/), Helm.

**Spec:** `docs/superpowers/specs/2026-10-30-headless-agent-interface-design.md` (stage H1). The executor reads the spec and this plan together.

## Global Constraints

- **Logging.** Use `tracing`, not `log`. Library errors use `thiserror`. Public APIs get doc comments.
- **Runtime knobs.** Every runtime knob has an `ARNEB_*` override. Its effective value is logged on `arneb::config` at startup. Precedence is CLI > env > file > defaults.
- **Coordinator only.** The state store lives on the coordinator or the standalone server. Workers never open it.
- **The config file is a seed.** It seeds state on first start only. Afterwards the state wins. A divergence logs a WARN on `arneb::config` that names the object.
  *Superseded in review (PR #215): config-seeded users now follow the config on every start, and a changed hash or removed user revokes their tokens. See D2 in `openspec/changes/headless-api-auth/design.md`. Task 2 below describes the original plan.*
- **Format and durability.** The state format is the builder's choice. Every write must be atomic and survive a crash.
- **Unchanged behavior under `[auth] type = "none"`.** pgwire and Trino behave as today, and the token endpoints return the structured error `auth_disabled`.
- **Unauthenticated routes stay as they are.** `/metrics` stays unauthenticated by default. `[web] metrics_auth = true` requires credentials for it.
- **`/api/v1` is not broken.** Its routes keep their paths and JSON shapes. Under password authentication they now require credentials.
- **Validation tiers.** Tier 1 means `cargo test --workspace`, `cargo fmt -- --check`, and `cargo clippy --workspace --all-targets -- -D warnings`. Tier 2 means the local distributed stack, used in Task 10. Each claim in the PR names the tier it rests on.
- **Housekeeping.** At the start, run `./scripts/target-housekeeping.sh`. Use `--sweep` at 🟡.

## Review Focus

1. **A revoked token must stop working immediately on every protocol.** The Trino Basic success cache (300 s TTL) must never cache a token-shaped password. Test: `revoked_token_is_refused_on_trino_basic_without_waiting_for_the_cache` (Task 4).
2. **A crash in the middle of a write must not lose the previous state.** A leftover temp file must not stop startup. Test: `leftover_temp_file_does_not_block_open` (Task 1).
3. **Rejections must not reveal which users or token ids exist.** An unknown user, a wrong password, and a well-formed token with an unknown id all get the same `unauthenticated` error. Test: `unknown_token_id_and_wrong_secret_are_indistinguishable` (Task 6).
4. **A config password change after the seed must not fail silently.** The WARN must name the user and say how to apply the change (`--reseed-state`). Test: `changed_config_password_warns_and_keeps_state` (Task 2).
5. **The Web UI must keep working under `none` with no login prompt.** Under `password` it must show the login. Test: `health_reports_auth_mode` (Task 7). Task 8 checks it by hand.

---

### Task 1: `arneb-state` crate with an atomic, versioned state file

**Files:**
- Create: `crates/state/Cargo.toml` (package `arneb-state`; dependencies: `arneb-protocol`, `serde`, `serde_json`, `thiserror`, `tracing`, `aws-lc-rs`, `base64`)
- Create: `crates/state/src/lib.rs`
- Create: `crates/state/src/file.rs`
- Modify: the workspace `Cargo.toml` `members`
- Test: `crates/state/src/file.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces:
  - `pub struct StateStore` (`Send + Sync`, shared as `Arc<StateStore>`)
  - `StateStore::open(path: Option<&Path>) -> Result<StateStore, StateError>`. `None` means in-memory.
  - `StateStore::is_persistent(&self) -> bool`
  - `StateStore::path(&self) -> Option<&Path>`
  - `pub enum StateError`, with these variants:
    - `Io { path: PathBuf, source: std::io::Error }`
    - `Corrupt { path: PathBuf, message: String }`
    - `UnsupportedVersion { path: PathBuf, found: u32 }`
    - `UnknownUser(String)`
    - `TokenNotFound(String)`
    - `Random`
  - Internally, a private `StateData { version: u32, users: Vec<UserRecord>, tokens: Vec<TokenRecord> }` is persisted as JSON with `version = 1`. All mutations go through `fn mutate<R>(&self, f: impl FnOnce(&mut StateData) -> Result<R, StateError>) -> Result<R, StateError>`. It holds a `Mutex`, applies `f` to a clone, persists the clone, and only then swaps it in.

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn open_missing_file_starts_empty_and_creates_nothing_until_first_write()
// open(Some(tmp/"state.json")) → Ok; file does not exist yet; is_persistent() == true
#[test] fn data_survives_reopen()
// mutate(push a UserRecord "alice") then drop; reopen same path → users == ["alice"]
#[test] fn leftover_temp_file_does_not_block_open()
// write garbage to tmp/"state.json.tmp", valid state to "state.json" → open Ok, data intact
#[test] fn corrupt_file_fails_naming_the_path()
// "state.json" = "{not json" → Err(StateError::Corrupt { path, .. }) with path == that file
#[test] fn newer_version_is_refused()
// {"version":2,...} → Err(StateError::UnsupportedVersion { found: 2, .. })
#[test] fn failed_persist_leaves_memory_unchanged()
// path in a directory made read-only after open → mutate returns Err(Io); in-memory users unchanged
#[cfg(unix)] #[test] fn state_file_is_owner_only()
// after a write, file mode & 0o777 == 0o600
```

- [ ] **Step 2: Run them and check that they fail.** Run `cargo test -p arneb-state`. Expected: the crate does not compile yet (`StateStore` is undefined).

- [ ] **Step 3: Implement `StateStore::open` and `mutate` in `file.rs`.**
  - Persist by writing `<path>.tmp` with mode 0600, calling `sync_all`, renaming it over `<path>`, then calling `sync_all` on the parent directory.
  - `open` ignores `<path>.tmp`.

- [ ] **Step 4: Run them and check that they pass.** Run `cargo test -p arneb-state`. Expected: all 7 tests pass.

- [ ] **Step 5: Commit.** `git commit -m "feat(state): add the coordinator state store"`

### Task 2: Seed users from the config, warn on divergence, `--reseed-state`

**Files:**
- Create: `crates/state/src/users.rs`
- Modify: `crates/protocol/src/auth.rs`. Make `ScramVerifier::salt(&self) -> &[u8]` public. Add `pub fn decoy_verifier(secret: &[u8; 32], user: &str) -> ScramVerifier`, moved out of `UserCredentials::lookup` so both stores share it.
- Test: `crates/state/src/users.rs`

**Interfaces:**
- Consumes: `StateStore::mutate` (from Task 1), `arneb_protocol::ScramVerifier`.
- Produces:
  - `StateStore::seed_users(&self, users: &[(String, ScramVerifier)], reseed: bool) -> Result<SeedReport, StateError>`
  - `pub struct SeedReport { pub first_start: bool, pub seeded: Vec<String>, pub diverged: Vec<String> }`
  - `UserRecord { name: String, scram: String /* ScramVerifier Display form */, from_config: bool }`

- **Seeding rules:**
  - **Empty state:** insert every config user (`first_start = true`).
  - **Non-empty state, `reseed = false`:**
    - Insert config users the state does not have yet, and list them in `seeded`.
    - Never overwrite an existing user.
    - Add a user to `diverged` when its stored verifier differs from the config's verifier, or when a `from_config` user has disappeared from the config.
  - **`reseed = true`:** replace every `from_config` user with the config's version, and drop `from_config` users that are no longer in the config. Their tokens are dropped too.

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn first_start_seeds_every_config_user()
#[test] fn new_config_user_is_added_on_a_later_start()
#[test] fn changed_config_password_warns_and_keeps_state()
// second seed with a different verifier for "alice" → diverged == ["alice"]; stored verifier unchanged;
// the WARN (captured with tracing-test or a test subscriber) contains "alice" and "--reseed-state"
#[test] fn reseed_replaces_config_users_and_drops_their_tokens()
#[test] fn removed_config_user_is_reported_not_deleted()
```

- [ ] **Step 2: Run them and check that they fail.** Run `cargo test -p arneb-state users`. Expected: `seed_users` is undefined.

- [ ] **Step 3: Implement `seed_users`.** It emits one WARN per diverged user. The WARN goes to target `arneb::config`, with field `user` and a message ending "…; restart with --reseed-state to apply the config file".

- [ ] **Step 4: Run them and check that they pass.** Run `cargo test -p arneb-state`. Also run `cargo test -p arneb-protocol auth`, which checks that the decoy refactor kept `unknown_user_gets_stable_decoy_salt` passing.

- [ ] **Step 5: Commit.** `git commit -m "feat(state): seed users from the config file"`

### Task 3: API tokens (issue, list, revoke, verify)

**Files:**
- Create: `crates/state/src/tokens.rs`
- Test: `crates/state/src/tokens.rs`

**Interfaces:**
- Consumes: `UserRecord` (from Task 2), `StateStore::mutate`.
- Produces:
  - `StateStore::issue_token(&self, user: &str, name: Option<String>, expires_at_ms: Option<u64>) -> Result<IssuedToken, StateError>`
  - `StateStore::list_tokens(&self, user: &str) -> Vec<TokenInfo>`
  - `StateStore::revoke_token(&self, user: &str, token_id: &str) -> Result<(), StateError>`
  - `pub struct TokenInfo { pub id: String, pub user: String, pub name: Option<String>, pub created_ms: u64, pub expires_ms: Option<u64> }`
  - `pub struct IssuedToken { pub info: TokenInfo, pub token: String }`
  - `pub fn is_token_shaped(s: &str) -> bool`. This lives in `arneb-protocol` (`auth.rs`), so the protocol crates can recognize a token without depending on `arneb-state`.

- **Token format:** `arneb_<id>_<secret>`.
  - `<id>` is 16 lowercase hex characters (8 random bytes).
  - `<secret>` is 43 characters of base64url without padding (32 random bytes).
  - `is_token_shaped` checks exactly this shape.
- **Stored record:** `TokenRecord { id, user, name, created_ms, expires_ms, secret_sha256: [u8; 32], scram: String }`.
  - `scram` is `ScramVerifier::from_password(full_token, user_salt, user_iterations)`. The user's salt and iteration count come from their stored verifier, so the user's SCRAM salt stays the same for both the password and every token.
  - The full token string is never stored or logged.
- **Revoking another user's token:** `revoke_token` with a user that does not own the token returns `TokenNotFound`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn issued_token_has_the_documented_shape()          // regex ^arneb_[0-9a-f]{16}_[A-Za-z0-9_-]{43}$
#[test] fn token_survives_reopen()
#[test] fn plaintext_token_is_not_in_the_state_file()        // file bytes do not contain the secret
#[test] fn revoke_removes_only_that_token()
#[test] fn revoking_someone_elses_token_is_not_found()
#[test] fn issuing_for_an_unknown_user_fails()               // Err(UnknownUser)
#[test] fn token_scram_verifier_shares_the_users_salt()
```

- [ ] **Step 2: Run them and check that they fail.** Run `cargo test -p arneb-state tokens`.

- [ ] **Step 3: Implement the token functions.** Use `aws_lc_rs::rand::fill` for randomness, `digest::SHA256` for the hash, and `constant_time::verify_slices_are_equal` for comparisons.

- [ ] **Step 4: Run them and check that they pass.** Run `cargo test -p arneb-state`.

- [ ] **Step 5: Commit.** `git commit -m "feat(state): issue, list and revoke API tokens"`

### Task 4: `CredentialSource` for pgwire SCRAM and Trino Basic

**Files:**
- Modify: `crates/protocol/src/auth.rs`:
  - Add the trait.
  - `AuthMethod::ScramSha256` now holds `Arc<dyn CredentialSource>`.
  - The SCRAM handler checks the client proof against every verifier and signs with the one that matched.
  - `UserCredentials` implements the trait.
- Modify: `crates/protocol/src/trino/auth.rs`:
  - Make the authenticator public as `pub struct BasicAuthenticator`, with `pub async fn check(&self, user: &str, password: &str) -> BasicVerdict` and `pub enum BasicVerdict { Valid, Invalid, Overloaded }`.
  - A token-shaped password bypasses the success cache and the PBKDF2 limiter.
- Modify: `crates/state/src/lib.rs` (`impl CredentialSource for StateStore`)
- Test: `crates/protocol/tests/auth.rs`, `crates/protocol/tests/trino_auth.rs`, `crates/state/src/lib.rs`

**Interfaces:**
- Consumes: `StateStore` token and user APIs (from Tasks 2–3), `is_token_shaped`.
- Produces (in `arneb_protocol`):

```rust
pub trait CredentialSource: Send + Sync + std::fmt::Debug {
    /// Verifiers for `user` — the password first, then each live token — all
    /// sharing one salt and iteration count. Empty when the user is unknown.
    fn scram_verifiers(&self, user: &str) -> Vec<ScramVerifier>;
    /// HTTP Basic: the user's password, or one of the user's live API tokens.
    fn verify_password(&self, user: &str, password: &str) -> bool;
    /// Bearer: the user owning this live token.
    fn verify_token(&self, token: &str) -> Option<String>;
    /// Number of users, for the startup log.
    fn user_count(&self) -> usize;
}
```

- **Expired tokens:** a token whose `expires_ms` has passed is not live.
- **Unknown users:** for an unknown user, `verify_password` still runs PBKDF2 against `decoy_verifier`, so timing does not reveal which users exist. An unknown token id still runs a SHA-256 comparison against a fixed dummy hash.

- [ ] **Step 1: Write the failing tests**

```rust
// crates/protocol/tests/auth.rs
#[tokio::test] async fn pgwire_accepts_an_api_token_as_the_password()
#[tokio::test] async fn pgwire_still_accepts_the_password_when_the_user_has_tokens()
#[tokio::test] async fn pgwire_refuses_a_revoked_token()
// crates/protocol/tests/trino_auth.rs
#[tokio::test] async fn trino_basic_accepts_an_api_token_as_the_password()
#[tokio::test] async fn revoked_token_is_refused_on_trino_basic_without_waiting_for_the_cache()
// crates/state/src/lib.rs
#[test] fn expired_token_is_not_live()
```

The protocol tests use a small in-test `CredentialSource`, because `arneb-protocol` cannot depend on `arneb-state`. Its tokens are issued in the documented format.

- [ ] **Step 2: Run them and check that they fail.** Run `cargo test -p arneb-protocol --test auth --test trino_auth`.

- [ ] **Step 3: Implement the trait and switch both handlers to it.** Update every `AuthMethod::ScramSha256(creds)` match in `crates/server/src/main.rs` to use `creds.user_count()`.

- [ ] **Step 4: Run them and check that they pass.** Run `cargo test -p arneb-protocol && cargo test -p arneb-state`. The existing tests in `auth.rs` and `trino_auth.rs` must still pass.

- [ ] **Step 5: Commit.** `git commit -m "feat(protocol): authenticate API tokens on pgwire and Trino"`

### Task 5: Config, CLI and startup wiring

**Files:**
- Modify: `crates/server/src/config.rs`:
  - Add `[state] path` (`StateConfig { path: Option<PathBuf> }`), overridden by `ARNEB_STATE_PATH`.
  - Add `[web] metrics_auth: bool` (default `false`), overridden by `ARNEB_WEB_METRICS_AUTH`.
  - Add `AuthConfig::user_verifiers(&self) -> Result<Vec<(String, ScramVerifier)>>`.
- Modify: `crates/server/src/main.rs`:
  - Add the `--reseed-state` CLI flag.
  - On a coordinator or standalone server with `[auth] type = "password"`:
    1. Call `StateStore::open(config.state.path)`.
    2. Call `seed_users(verifiers, reseed)`.
    3. Build `AuthMethod::ScramSha256(state.clone())`.
    4. Pass the same `Arc` to the Trino listener and to `WebState`.
- Modify: `crates/server/src/web/mod.rs`. `WebState` gains these fields:
  - `credentials: Option<Arc<dyn CredentialSource>>`
  - `state: Option<Arc<StateStore>>`
  - `access_rules: Option<Arc<AccessRules>>`
  - `metrics_auth: bool`
- Test: `crates/server/src/config.rs` tests

**Interfaces:**
- Consumes: `StateStore::open` and `seed_users` (from Tasks 1–2), `CredentialSource` (from Task 4).
- Produces: the `WebState` fields above, used by Tasks 6–7.

- **Startup log:** one INFO on `arneb::config` with `state_path` (or `in-memory`), `persistent`, `users`, `first_start`, `seeded`, `diverged` and `reseed`. With password authentication and no `[state] path`, it also logs a WARN: "no [state] path: API tokens are lost on restart".
- **Ignored flag:** `--reseed-state` with `[auth] type = "none"` is ignored, with an INFO.

- [ ] **Step 1: Write the failing tests**

```rust
#[test] fn state_path_from_file_env_and_default()       // unset → None; file; ARNEB_STATE_PATH wins over file
#[test] fn metrics_auth_defaults_off_and_env_overrides() // ARNEB_WEB_METRICS_AUTH=true → true
#[test] fn user_verifiers_parse_the_configured_hashes()
```

- [ ] **Step 2: Run them and check that they fail.** Run `cargo test -p arneb-server config`.

- [ ] **Step 3: Implement the config fields and the wiring.**

- [ ] **Step 4: Run them and check that they pass.** Run `cargo test -p arneb-server`, then start the server locally with a password user and `[state] path` in the scratch directory. Expected: the startup INFO lists `state_path`, `first_start=true` and `seeded=[<user>]`. On a second start, `first_start=false`.

- [ ] **Step 5: Commit.** `git commit -m "feat(server): open the state store at startup"`

### Task 6: Web auth middleware, the error envelope, and query rules on the Web API

**Files:**
- Create: `crates/server/src/web/auth.rs`
- Create: `crates/server/src/web/error.rs`
- Create: `crates/server/src/web/session.rs`
- Modify: `crates/server/src/web/mod.rs` (wrap `/api/v1`, `/api/v2` and, when `metrics_auth` is set, `/metrics` in the middleware)
- Modify: `crates/server/src/web/api.rs` (apply the query rules)
- Modify: `crates/catalog/src/access.rs` (`pub fn can_kill_query(&self, viewer: &str, owner: &str) -> bool`, with the same matching as `can_view_query` but checking `QueryAccess::Kill`; the owner may always kill their own query)
- Test: `crates/server/src/web/auth.rs`, `crates/catalog/src/access.rs`

**Interfaces:**
- Consumes: the `WebState` fields (from Task 5), `BasicAuthenticator` (from Task 4).
- Produces:
  - `pub struct Identity { pub user: String, pub via: AuthVia }`, inserted as a request extension. `pub enum AuthVia { Token, Session, Basic, Unauthenticated }`.
  - `pub struct ApiError { pub code: ErrorCode, pub message: String, pub hint: Option<String> }`. It implements `IntoResponse`, and the body is `{"error":{"code":"…","message":"…","hint":…}}`.
  - `pub enum ErrorCode`, serialized in snake_case. Each code maps to one HTTP status:

    | Code | Status |
    |---|---|
    | `unauthenticated` | 401, with `WWW-Authenticate: Bearer realm="arneb"` |
    | `forbidden` | 403 |
    | `not_found` | 404 |
    | `invalid_argument` | 400 |
    | `auth_disabled` | 409 |
    | `overloaded` | 503, with `Retry-After: 1` |
    | `internal` | 500 |

  - `SessionStore`:
    - `SessionStore::create(&self, user: &str) -> String` returns a token-shaped string with id prefix `s`.
    - `resolve(&self, token: &str) -> Option<String>`
    - `revoke(&self, token: &str)`
    - Sessions are in memory only, with an 8-hour TTL (`const SESSION_TTL`).

- **Under `password`:**
  - `Authorization: Bearer <api token or session>` is checked first, then `Authorization: Basic`, through `BasicAuthenticator`.
  - Anything else is rejected with `unauthenticated`. So is a bad credential, with no difference in the message.
  - Public routes are `/api/v1/info`, `/api/v2/health`, `POST /api/v2/sessions` and the static frontend.
- **Under `none`:**
  - The identity is `X-Arneb-User`, or `anonymous` when that header is absent, with `via = Unauthenticated`.
  - The query rules are not applied, which matches the Web UI today. *(This is Claude's choice, not the user's.)*
- **Query rules under `password`:**
  - `GET /api/v1/queries` and `GET /api/v1/queries/{id}` return only the queries `can_view_query` allows. A hidden query gets 404, not 403.
  - `DELETE /api/v1/queries/{id}` requires `can_kill_query`. A query that is visible but not killable gets 403.

- [ ] **Step 1: Write the failing tests** (they drive the router through `tower::ServiceExt::oneshot`, with an in-test `CredentialSource`)

```rust
#[tokio::test] async fn api_v1_requires_credentials_under_password_auth()   // GET /api/v1/queries → 401, envelope code "unauthenticated"
#[tokio::test] async fn info_and_health_stay_public()
#[tokio::test] async fn bearer_token_and_basic_both_authenticate()
#[tokio::test] async fn unknown_token_id_and_wrong_secret_are_indistinguishable() // identical status + body
#[tokio::test] async fn metrics_is_public_unless_metrics_auth()
#[tokio::test] async fn hidden_query_is_not_found_and_unkillable_query_is_forbidden()
#[tokio::test] async fn none_mode_keeps_the_api_open()
#[test] fn owner_can_always_kill_and_rules_grant_kill()                      // access.rs
```

- [ ] **Step 2: Run them and check that they fail.** Run `cargo test -p arneb-server web:: && cargo test -p arneb-catalog access`.

- [ ] **Step 3: Implement the middleware, `ApiError`, `SessionStore`, the rule checks and `can_kill_query`.** `/api/v1` keeps its old error bodies for 400 and 404, and only the new 401 and 403 responses use the envelope. That way the v1 shapes stay unchanged.

- [ ] **Step 4: Run them and check that they pass.** Run `cargo test -p arneb-server && cargo test -p arneb-catalog`.

- [ ] **Step 5: Commit.** `git commit -m "feat(web): authenticate the Web port and apply query rules"`

### Task 7: The `/api/v2` skeleton

**Files:**
- Create: `crates/server/src/web/v2.rs`
- Modify: `crates/server/src/web/mod.rs` (`.nest("/api/v2", v2::routes())`)
- Test: `crates/server/src/web/v2.rs`

**Interfaces:**
- Consumes: `Identity`, `ApiError`, `SessionStore` (from Task 6), `StateStore` token APIs (from Task 3).
- Produces these routes. Every error uses the envelope.

| Method and path | Auth | Response |
|---|---|---|
| `GET /health` | public | `{"status":"ok","version":"<pkg>","auth":"none"\|"password"}` |
| `POST /sessions` | public; body `{"user","password"}` | `201 {"token","expires_ms"}`, or `unauthenticated` |
| `DELETE /sessions/current` | session bearer | `204` |
| `GET /whoami` | any | `{"user","via":"token"\|"session"\|"basic"\|"unauthenticated"}` |
| `POST /tokens` | password auth; body `{"name"?, "expires_in_days"?}` | `201 {"token", "id", "name", "created_ms", "expires_ms"}`. The token is shown once. Under `none`: `auth_disabled`. |
| `GET /tokens` | password auth | `{"tokens":[TokenInfo…]}`, only the caller's own tokens |
| `DELETE /tokens/{id}` | password auth | `204`, or `not_found` |
| `GET /queries`, `GET /queries/{id}`, `DELETE /queries/{id}`, `GET /cluster`, `GET /cluster/workers`, `GET /info` | as in v1 | The same JSON as v1, with errors in the envelope |

- **Validation:** `expires_in_days` must be between 1 and 3650 inclusive. Any other value gets `invalid_argument`, with a hint that gives the range.
- **Session tokens and token management:** a session token may manage the caller's own tokens.
- **Persistence warning:** in-memory state adds `"persistent": false` to the `POST /tokens` response, together with a `hint` explaining that the token is lost on restart.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn health_reports_auth_mode()
#[tokio::test] async fn login_then_issue_list_and_revoke_a_token()
#[tokio::test] async fn issued_token_authenticates_the_next_request()
#[tokio::test] async fn tokens_are_auth_disabled_under_none()
#[tokio::test] async fn expires_in_days_out_of_range_is_invalid_argument_with_hint()
#[tokio::test] async fn cannot_list_or_revoke_another_users_tokens()
#[tokio::test] async fn v2_queries_matches_v1_shape()
```

- [ ] **Step 2: Run them and check that they fail.** Run `cargo test -p arneb-server web::v2`.

- [ ] **Step 3: Implement the routes.** Reuse the v1 handlers' response structs, and do not copy them.

- [ ] **Step 4: Run them and check that they pass.** Run `cargo test -p arneb-server`.

- [ ] **Step 5: Commit.** `git commit -m "feat(web): add the /api/v2 skeleton with tokens and sessions"`

### Task 8: Web UI login

**Files:**
- Modify: `web/src/lib/api.ts`. Send `Authorization: Bearer <sessionStorage["arneb.session"]>` when set. On a 401 whose envelope code is `unauthenticated`, clear the session and dispatch a `arneb:unauthenticated` window event.
- Create: `web/src/pages/Login.tsx`. It has a user field, a password field and a submit button. It calls `POST /api/v2/sessions` and stores the token.
- Modify: `web/src/App.tsx`. On load, read `/api/v2/health`. Under `password` with no session, or after `arneb:unauthenticated`, render `Login`. Add a sign-out item that calls `DELETE /api/v2/sessions/current`.
- Wrap `sessionStorage` reads and writes in `try/catch`.

- [ ] **Step 1: Build.** Run `cd web && pnpm install && pnpm lint && pnpm build`. Expected: both pass.
- [ ] **Step 2: Check by hand.** Start the server with password authentication, open `http://127.0.0.1:6432`, sign in, open Queries, sign out, and confirm the login page returns. With `[auth] type = "none"`, no login appears.
- [ ] **Step 3: Commit.** `git commit -m "feat(web): sign in to the Web UI under password auth"`

### Task 9: Helm persistence for the state file

**Files:**
- Create: `charts/arneb/templates/coordinator-pvc.yaml`
- Modify: `charts/arneb/values.yaml`:
  - `coordinator.state.persistence.enabled: true`
  - `size: 1Gi`
  - `storageClass: ""`
  - `existingClaim: ""`
- Modify: `charts/arneb/templates/coordinator.yaml`:
  - Mount the claim (or an `emptyDir` when persistence is disabled) at `/var/lib/arneb`.
  - Set `ARNEB_STATE_PATH=/var/lib/arneb/state.json`.
  - The strategy stays `Recreate`.
- Modify: `charts/arneb/templates/NOTES.txt`. Add one line about the state volume.

- [ ] **Step 1: Lint.** Run `helm lint charts/arneb -f charts/arneb/ci/kind-values.yaml`, then `helm template t charts/arneb | grep -A3 ARNEB_STATE_PATH`. Expected: lint passes, and the env var and the volume mount render.
- [ ] **Step 2: Disable persistence.** Run `helm template t charts/arneb --set coordinator.state.persistence.enabled=false | grep -B2 -A2 emptyDir`. Expected: an `emptyDir` is used.
- [ ] **Step 3: Commit.** `git commit -m "feat(chart): persist the coordinator state"`

### Task 10: Docs, OpenSpec change, Tier 2 check

**Files:**
- Create: `docs/guide/http-api.md`. It covers authentication (bearer, Basic, sessions), the token lifecycle, the error envelope, the route table from Task 7, and how to use a token as a pgwire or Trino password.
- Modify: `docs/guide/configuration.md`. Document `[state] path`, `[web] metrics_auth` and `--reseed-state`. Add an upgrade note: under password authentication, the Web API now requires credentials.
- Modify: `docs/guide/access-control.md`. The Web API now applies the `queries` view and kill rules.
- Modify: `docs/guide/kubernetes.md` (the state volume).
- Modify: `docs/.vitepress/config.ts` (add the sidebar entry for `http-api`).
- Create: `openspec/changes/headless-api-auth/` (`proposal.md`, `design.md`, `specs/…/spec.md`, `tasks.md`). Generate them with `openspec new change headless-api-auth`, filling them from the spec and this plan.

- [ ] **Step 1: Build the docs.** Run `cd docs && pnpm exec vitepress build .`. Expected: the build completes.
- [ ] **Step 2: Run Tier 2 on the local distributed stack.** Use a coordinator, two workers, `[auth] type = "password"` and `[state] path` set.
  1. Get a session with `POST /api/v2/sessions`, then issue a token with `POST /api/v2/tokens`.
  2. Run `psql "host=127.0.0.1 port=5432 user=<u> password=<token>" -c "select count(*) from lineitem"` and get the correct count.
  3. Run the same query through the Trino listener with Basic `<u>:<token>`.
  4. Restart the coordinator. The token still works.
  5. Revoke the token. pgwire, Trino and `/api/v2/whoami` all refuse it.

  Record the commands and outputs in the PR. This is the Tier 2 evidence.
- [ ] **Step 3: Run Tier 1 in full.** Run `cargo fmt -- --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`. Expected: everything is clean and passes.
- [ ] **Step 4: Commit.** `git commit -m "docs: document the HTTP API, tokens and the state store"`
