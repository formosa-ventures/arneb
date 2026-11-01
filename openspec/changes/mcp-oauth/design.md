## Context

Stage H6 of the headless agent interface, on top of H1 (identity, API tokens, state store), H3 (`/mcp`) and H4 (`via` in the audit log). `authenticate` in `web/auth.rs` already turns a credential into a user and a `via`; H6 adds one more credential kind, valid only on `/mcp`.

## Goals / Non-Goals

**Goals**
- A client that only speaks OAuth (the MCP authorization flow) can use `/mcp`.
- Arneb stays a resource server: no token issuing, login pages, client registration or token storage.
- Without `[oauth]` the behavior is byte-identical, headers included.

**Non-Goals**
- OAuth on `/api/*`, the Web UI login through OIDC, opaque-token introspection, auto-provisioning users.
- A Keycloak in the kind CI job (follows once the stack is on `main`).

## Decisions

**D1. `/mcp` only.** JWTs are accepted on `/mcp` and nowhere else; on `/api/*` and `/metrics` a JWT is just an invalid bearer (`401`). The spec scopes H6 to MCP. Widening is one condition in `authenticate` plus tests.

**D2. The mapped user must already exist** in the state store (config seed or `create_user`). No auto-provisioning. A valid token for an unknown user is `403 forbidden`, not `401`, which would loop the client's OAuth flow. Deleting the user revokes OAuth access on the next request. Cost: an admin creates each OAuth user once.

**D3. Default `user_claim = "sub"`.** `sub` is the only claim OIDC guarantees stable and unique; `preferred_username` and `email` are mutable at some IdPs and are opt-in, with a startup WARN. With `email`, the token's `email_verified` must be `true` (boolean, or the string `"true"`), else `403`: an unverified address can be registered by anyone. Cost: with the default, Arneb user names are IdP subject ids; an IdP that never sends `email_verified` cannot use `email`.

**D4. The audience defaults to the resource URL** (RFC 8707 and the MCP authorization spec: tokens must be issued for this resource); `aud` must contain it. Configurable for IdPs that can only emit a client id; in that client-id mode startup requires a non-empty `required_scopes`, because an OIDC ID token for the client carries the same `aud` and no scope.

**D4b. Access tokens, not ID tokens.** A header `typ` of `at+jwt` / `application/at+jwt` (RFC 9068) is accepted as an access token. Any other or no `typ` is accepted too, because Keycloak, Entra ID and Okta send `typ: JWT` on access tokens, but then a token carrying `nonce`, `at_hash` or a Keycloak payload `typ` of `ID`, `Refresh`, `Offline` or `Logout` is refused as not an access token. `require_access_token_typ = true` (opt-in) accepts only `at+jwt`. Cost: an IdP that puts `nonce` in access tokens without `at+jwt` (Keycloak before 25) is refused.

**D5. Asymmetric algorithms only:** RS256, RS384, RS512, PS256, ES256, ES384. `none` and `HS*` are always refused, so there is no algorithm confusion. Not configurable.

**D6. IdP unreachable is `503 auth_provider_unavailable`** (`Retry-After: 5`), not `401`, when no cached key can decide. API tokens, sessions and Basic on `/mcp` keep working regardless.

**D6b. Key cache.** A JWKS fetch runs in a detached task (single-flight; at most one start per `jwks_min_refresh_secs`); waiters share its outcome, so a dropped request can neither abort a fetch nor use up the throttle window without one. A drop guard in the task records a task that ends without a result (panic, runtime shutdown) as a failed attempt, so the flight is never left listed and refreshes resume. From 80% of `jwks_cache_secs` the set is refetched in the background while requests keep using the cached keys (stale-while-revalidate); while refetches fail, the last good keys decide until they are `jwks_max_stale_secs` old (default 86400, at least `jwks_cache_secs`). A token whose `kid` is unknown waits for a refetch. Every key under the `kid` that fits the token's `alg` is tried. A `jwks_uri` from the discovery document of a non-loopback issuer must be `https`. Cost: a key the IdP withdraws while it is unreachable stays accepted for up to `jwks_max_stale_secs`.

**D7. An OAuth identity has the same rights as the user**, including `create_token` (minting an API token), whose expiry is clamped to the access token's `exp` so a child never outlives its parent. Revoking the user at the IdP does not revoke Arneb API tokens already minted; documented. Refusing `create_token` for `via = oauth` would be one check.

**D8. `[oauth]` is startup-only config** (like ports and TLS), not state-store data; changing it needs a restart. It requires `[auth] type = "password"` and `[mcp] enabled`; otherwise startup fails with a named error.

**D9. Plain `http` issuer or resource only for loopback hosts** (local testing); anything else must be `https`.

**D10. One shape for every `401`.** Under `[oauth]` only the `WWW-Authenticate` header on `/mcp` changes; the body is the same `unauthenticated` envelope. A request without credentials gets the bare challenge; a refused JWT adds `error="invalid_token"` and a fixed `error_description` (RFC 6750 section 3.1) so a client refreshes instead of restarting its flow; a missing scope is `403` with `error="insufficient_scope"`. The specific failure reason is never observable (it is logged at DEBUG as a reason code on `arneb::oauth`, never the token). The challenge strings are computed once at startup.

**D11. Metadata at one path.** The RFC 9728 document is served only at `/.well-known/oauth-protected-resource` plus the path of `resource` (the bare well-known path only for a path-less resource, which `resource` ending in `/mcp` rules out); any other path under the prefix is a plain `404`.

## Risks / Trade-offs

- A `user_claim` other than `sub` trusts the IdP's rules on who may edit that claim.
- JWKS fetches go to the network from the coordinator; a slow IdP is bounded by a 5 s timeout, one fetch in flight, and the minimum refresh gap.
- Tier 2 runs against Keycloak with plain `http` on loopback; a real deployment terminates TLS in front of the Web port (see the Kubernetes guide).
