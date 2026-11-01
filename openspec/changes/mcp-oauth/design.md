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

**D3. Default `user_claim = "sub"`.** `sub` is the only claim OIDC guarantees stable and unique; `preferred_username` and `email` are mutable at some IdPs and are opt-in, with a startup WARN. Cost: with the default, Arneb user names are IdP subject ids.

**D4. The audience defaults to the resource URL** (RFC 8707 and the MCP authorization spec: tokens must be issued for this resource); `aud` must contain it. Configurable for IdPs that can only emit a client id.

**D5. Asymmetric algorithms only:** RS256, RS384, RS512, PS256, ES256, ES384. `none` and `HS*` are always refused, so there is no algorithm confusion. Not configurable.

**D6. IdP unreachable is `503 auth_provider_unavailable`** (`Retry-After: 5`), not `401`, when no cached key can decide. API tokens, sessions and Basic on `/mcp` keep working regardless.

**D7. An OAuth identity has the same rights as the user**, including `create_token` (minting an API token). Revoking the user at the IdP does not revoke Arneb API tokens already minted; documented. Refusing `create_token` for `via = oauth` would be one check.

**D8. `[oauth]` is startup-only config** (like ports and TLS), not state-store data; changing it needs a restart. It requires `[auth] type = "password"` and `[mcp] enabled`; otherwise startup fails with a named error.

**D9. Plain `http` issuer or resource only for loopback hosts** (local testing); anything else must be `https`.

**D10. One shape for every `401`.** Under `[oauth]` only the `WWW-Authenticate` header on `/mcp` changes; the body is the same `unauthenticated` envelope, so a token's failure reason is never observable (it is logged at DEBUG as a reason code on `arneb::oauth`, never the token).

## Risks / Trade-offs

- A `user_claim` other than `sub` trusts the IdP's rules on who may edit that claim.
- JWKS fetches go to the network from the coordinator; a slow IdP is bounded by a 5 s timeout, one fetch in flight, and the minimum refresh gap.
- Tier 2 runs against Keycloak with plain `http` on loopback; a real deployment terminates TLS in front of the Web port (see the Kubernetes guide).
