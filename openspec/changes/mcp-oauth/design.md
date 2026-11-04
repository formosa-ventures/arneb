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

**D3. Default `user_claim = "sub"`.** `sub` is the only claim OIDC guarantees stable and unique; `preferred_username` and `email` are mutable at some IdPs and are opt-in, with a startup WARN. With `email`, the token's `email_verified` must be `true` (boolean, or the string `"true"`), else `403`: an unverified address can be registered by anyone. With any other claim whose value contains `@` (`sub` included, such as an Okta login; `preferred_username` with Keycloak's "email as username", Entra `upn`, an Auth0 namespaced claim), `email_verified` vouches only for the token's `email`, so `email` must equal the value (ASCII case-insensitive) and be verified; a different `email` is a distinct `403` (a self-chosen username can be someone else's address). `sub` is not exempt: "never reassigned" is not "not chosen by the registrant", and Okta puts the self-chosen login there without `email_verified`. `allow_missing_email_verified` (opt-in, `ARNEB_OAUTH_ALLOW_MISSING_EMAIL_VERIFIED`, WARN at startup) accepts a token without `email` or `email_verified` for such a claim; a non-`true` value or a different `email` is refused regardless. A value without `@` is not checked: email verification says nothing about it. Cost: with the default, Arneb user names are IdP subject ids; an IdP that never sends `email_verified` cannot use `email`, and needs the opt-in for an address under another claim; an IdP whose username address legitimately differs from its `email` cannot map that username.

**D4. The audience defaults to the resource URL** (RFC 8707 and the MCP authorization spec: tokens must be issued for this resource); `aud` must contain it. Configurable for IdPs that name the API differently (Entra `api://…`, an Auth0 API identifier) or can only emit a client id. For any `audience` other than the resource URL (a hierarchical URL with a host, equal to `resource` up to URL normalization and a trailing slash), startup requires a non-empty `required_scopes`, because a client id can be any string (`arneb:mcp` and `localhost:8080` parse as URLs; MCP client-metadata-document ids and Keycloak URL clients are URLs), and an OIDC ID token for that client carries it as `aud` and no scope. Cost: an Entra `api://…` URI, an Auth0 API identifier or a URN audience needs `required_scopes` too, even though it cannot be a client id at that IdP.

**D4b. Access tokens, not ID tokens.** The first rule that applies decides: a header `typ` of `at+jwt` / `application/at+jwt` (RFC 9068) is an access token; a payload `typ` of `Bearer` (case-insensitive; Keycloak) is an access token even with a `nonce`, which Keycloak before 24 (and later with its "nonce backwards compatible" mapper, and RH-SSO) copies into access tokens; a payload `typ` of `ID`, `Refresh`, `Offline` or `Logout` is refused. Any other token (header `typ: JWT` from Entra ID, Okta and most others) may be an ID token when it carries `nonce` or `at_hash`, or neither `scope` nor `scp` (an ID token from a refresh grant has no `nonce`, and `at_hash` is optional): with an empty `required_scopes` it is refused, otherwise it passes only with every required scope, which ID tokens never carry. `require_access_token_typ = true` (opt-in) accepts only `at+jwt`. Cost: an IdP whose access tokens have no type marker and no scope claim needs `required_scopes` its tokens carry; an unmarked ID token sent with required scopes gets `403 insufficient_scope` rather than `401`; app-only tokens without a scope (Entra client credentials with `roles` only, Auth0 M2M) are refused; and payload `typ: Bearer` is trusted from every IdP, so only an admin-defined mapper putting it in an ID token would let that ID token through.

**D4c. Sender-constrained tokens are refused.** A token with a `cnf` claim (RFC 7800: DPoP `jkt`, mTLS `x5t#S256`) or a DPoP type (payload `typ: DPoP` from Keycloak, header `typ` `dpop+jwt`) is `401 invalid_token`, checked before the access-token rules. Arneb verifies no DPoP proof or client certificate, so accepting such a token as a plain bearer would let a stolen one be replayed. Cost: a client whose IdP binds its tokens cannot use `/mcp` until binding is turned off for it; verifying DPoP proofs is a follow-up.

**D5. Asymmetric algorithms only:** RS256, RS384, RS512, PS256, ES256, ES384. `none` and `HS*` are always refused, so there is no algorithm confusion. Not configurable.

**D6. IdP unreachable is `503 auth_provider_unavailable`** (`Retry-After: 5`), not `401`, when no cached key can decide. API tokens, sessions and Basic on `/mcp` keep working regardless.

**D6b. Key cache.** A JWKS fetch runs in a detached task (single-flight; at most one start per `jwks_min_refresh_secs`); waiters share its outcome, so a dropped request can neither abort a fetch nor use up the throttle window without one. A drop guard in the task records a task that ends without a result (panic, runtime shutdown) as a failed attempt, so the flight is never left listed and refreshes resume. From 80% of `jwks_cache_secs` the set is refetched in the background while requests keep using the cached keys (stale-while-revalidate); while refetches fail, the last good keys decide until they are `jwks_max_stale_secs` old (default 86400, at least `jwks_cache_secs`). A token whose `kid` is unknown waits for a refetch. Every key under the `kid` that fits the token's `alg` is tried. A `jwks_uri` from the discovery document of a non-loopback issuer must be `https`. Cost: a key the IdP withdraws while it is unreachable stays accepted for up to `jwks_max_stale_secs`.

**D7. An OAuth identity has the same rights as the user**, including `create_token` (minting an API token), whose expiry is clamped to the access token's `exp` so a child never outlives its parent. Revoking the user at the IdP does not revoke Arneb API tokens already minted; documented. Refusing `create_token` for `via = oauth` would be one check.

**D8. `[oauth]` is startup-only config** (like ports and TLS), not state-store data; changing it needs a restart. It requires `[auth] type = "password"` and `[mcp] enabled`; otherwise startup fails with a named error.

**D9. Plain `http` issuer or resource only for loopback hosts** (local testing); anything else must be `https`.

**D10. One shape for every `401`.** Under `[oauth]` only the `WWW-Authenticate` header on `/mcp` changes; the body is the same `unauthenticated` envelope. A request without credentials gets the bare challenge; any refused non-empty bearer (a JWT, an opaque token, an expired or revoked Arneb token; an empty `Bearer` counts as no credentials) adds `error="invalid_token"`, a fixed `error_description` (RFC 6750 section 3.1) and `scope` when `required_scopes` is set, so a client refreshes or re-authorizes with the right scope instead of restarting its flow; a missing scope is `403` with `error="insufficient_scope"`. The specific failure reason is never observable (it is logged at DEBUG as a reason code on `arneb::oauth`, never the token). The challenge strings are computed once at startup.

**D11. Metadata at the derived path, and the root only for a root `/mcp`.** The RFC 9728 document is served at `/.well-known/oauth-protected-resource` plus the path of `resource`. When that path is exactly `/mcp` it is also served at the bare well-known path, as a fallback for SDKs and proxies (`mcp-remote`) that fetch only the root. Behind a path prefix the bare path is `404`: the host is shared, and answering the host-wide path would point every other protected resource's clients at Arneb's issuer. Any other path under the prefix is a plain `404`. Cost: the bare copy's `resource` is not the root URL, so a strict RFC 9728 section 3.3 client discards it (and uses the derived path); a root-only client behind a prefix cannot discover Arneb; and an operator who routes the host-wide bare path to a root-`/mcp` Arneb on an ingress shared with other resources still exposes them (documented).

## Risks / Trade-offs

- A `user_claim` other than `sub` trusts the IdP's rules on who may edit that claim.
- JWKS fetches go to the network from the coordinator; a slow IdP is bounded by a 5 s timeout, one fetch in flight, and the minimum refresh gap.
- Tier 2 runs against Keycloak with plain `http` on loopback; a real deployment terminates TLS in front of the Web port (see the Kubernetes guide).
