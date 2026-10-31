## Context

Stage H1 of the headless agent interface. Before this change, `[auth]` verified passwords from `[[auth.users]]` (an in-memory `CredentialSource`) for pgwire SCRAM and Trino Basic. The Web port (pgwire + 1000) served `/api/v1` and `/metrics` with no authentication and shipped no notion of a caller's identity, so the `queries` access rules could not apply there.

## Goals / Non-Goals

**Goals**
- A remote caller gets a revocable, expiring credential without shell or config access.
- One credential works on pgwire, Trino and the Web API; revocation is immediate everywhere.
- State survives a coordinator restart, including on Kubernetes.
- `/api/v1` keeps its paths and JSON; `[auth] type = "none"` behaves as before.

**Non-Goals**
- OAuth, MCP, and the operation APIs (later stages).
- Managing users or access rules over the API.
- Login lockout, token scopes, or cascading revocation.
- Persisting Web sessions.

## Decisions

**D1. A JSON state file, not an embedded database.** The data is small (users and tokens), read on the auth path and written rarely. A versioned JSON file written with write-temp, fsync, rename is atomic and crash-safe, needs no new dependency, and is readable by an operator. Alternative: SQLite or sled; rejected as heavier than the need.

**D2. Config users follow the config; state users are the state's.** Every user record carries `from_config`. On every start, `[[auth.users]]` is applied to the config-seeded users: new ones are added, a changed `password_hash` replaces the verifier and revokes all of the user's tokens, and a user gone from the config is deleted with their tokens. Each change logs on `arneb::config` naming the user. Users created through the state store (`from_config = false`) are never touched; a config user whose name collides with one is reported with a WARN and the state user is kept, unless `--reseed-state` lets the config overwrite it. Before the state store, a restart with an edited config revoked access; operators rely on that to respond to a leaked password, so keeping the state's verifier (the first draft of this design) was rejected. A collision warns rather than failing startup: keeping the state user never grants the config's credential any access, so it fails closed, and a hard failure would block restarts over a non-security mismatch.

**D3. Tokens are verified like SCRAM passwords.** The server stores a SCRAM verifier derived with the user's salt and iteration count, so a token works as a password on pgwire without a second mechanism. For Trino Basic and the Web API, a token-shaped credential (`is_token_shaped`) is checked by its id with a constant-time SHA-256 compare, bypassing the PBKDF2 cache so revocation is immediate. Alternative: a separate token table checked before SCRAM; rejected for pgwire, where the SCRAM exchange needs a verifier.

**D4. Sessions are separate from tokens.** Web sessions use `arneb_s_<43 base64url>`, live in memory (8 h, 16 per user, 10,000 total) and never satisfy `is_token_shaped`, so pgwire and Trino never accept them. A browser holds a short-lived session instead of a long-lived token; the login page keeps it in `sessionStorage`. Token-shaped passwords are refused at login so a session cannot be minted from a token.

**D5. A child token cannot outlive its parent.** A token minted by a token is clamped to the parent's expiry. Revocation does not cascade; this is documented rather than tracked, to keep tokens independent rows.

**D6. One error envelope for `/api/v2`.** Seven stable codes, each with one HTTP status, plus a `hint`. `/api/v1` keeps its historical bodies except for the new 401 and 403.

**D7. Auth is a single middleware with an explicit public list.** `requires_auth` is the one place that decides what is open, covering the nested v1 and v2 routers and the static UI. Under `none` the identity comes from `X-Arneb-User` or `anonymous`, and query rules are not applied.

**D8. Blocking work off the async workers.** PBKDF2 checks go through the shared `BasicAuthenticator` (bounded concurrency, `overloaded` when saturated); token issue and revoke run under `spawn_blocking`, with at most four issuances in flight.

**D9. Helm mounts a PVC at `/var/lib/arneb`.** Coordinator only, `ARNEB_STATE_PATH=/var/lib/arneb/state.json`. The chart's PVC is deleted on `helm uninstall`; `existingClaim` lets production keep it.

## Risks / Trade-offs

- [Revoking a token leaves tokens minted with it valid] -> documented; revoke children by id.
- [pgwire SCRAM timing varies with a user's live token count] -> reveals a count, not a secret; documented.
- [A config password shaped like a token works on pgwire but not on Trino Basic] -> documented.
- [No login lockout] -> PBKDF2 concurrency is bounded; operators rate-limit at a proxy.
- [Sessions are lost on restart] -> the UI shows the login page again.
- [In-memory state without `[state] path`] -> a startup WARN and a `persistent: false` hint on token issue.
- [The Web API now rejects unauthenticated `/api/v1` under password auth] -> an upgrade note in the configuration guide.

## Migration Plan

Under `type = "none"` there is nothing to do. Under `type = "password"`: scrapers of `/api/v1` need a token or Basic, and `/metrics` is unchanged unless `metrics_auth` is set. Set `[state] path` (or the Helm persistence value) before issuing tokens. Roll back by deploying the previous release; the state file is ignored by it.

## Open Questions

None for H1. OAuth and MCP authentication are designed in later stages.
