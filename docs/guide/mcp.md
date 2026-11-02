# MCP Server

The Web port (pgwire port + 1000, for example `6432`) serves a
[Model Context Protocol](https://modelcontextprotocol.io) endpoint at
`/mcp`. An MCP client such as Claude Code, Cursor or the MCP Inspector can
explore the catalog, plan and run SQL, and read results as tools, without a
driver and without a shell on the server.

Every tool is the MCP form of an [HTTP API](/guide/http-api) route. A tool
runs as the authenticated user, with the same visibility, access rules,
admission control and limits as the matching `/api/v2` route. The
coordinator and standalone nodes serve `/mcp`; a worker has no Web port.

## Connect

Mint an [API token](/guide/http-api#api-tokens), then register the server.
With Claude Code:

```bash
claude mcp add --transport http arneb https://<host>/mcp \
  --header "Authorization: Bearer $ARNEB_TOKEN"
```

Any client that speaks streamable HTTP and can send a header works the same
way: point it at `https://<host>/mcp` and set
`Authorization: Bearer arneb_…`. Basic credentials and Web UI session tokens
are accepted too.

- **The Web port is plain HTTP.** The bearer token crosses the network in
  the clear. For anything beyond the same machine, put a TLS-terminating
  proxy in front of the Web port and connect to its `https://` address.
- **OAuth-only clients** (claude.ai custom connectors, for example) need
  `[oauth]` configured; see [OAuth](#oauth).

Under `[auth] type = "none"` there is nothing to prove: the caller is the
`X-Arneb-User` header or `anonymous`, and `/mcp` accepts only loopback
`Host` values by default (see [Host allow-list](#host-allow-list)).

## OAuth

Clients that insist on an OAuth flow cannot send an API token. With an
`[oauth]` section, `/mcp` also accepts OAuth 2.0 access tokens (JWTs) from an
external OpenID Connect identity provider (IdP) such as Keycloak, Auth0,
Okta or Entra ID. Arneb is only the resource server: it never issues tokens,
serves a login page or registers clients. The client discovers the IdP from
Arneb, signs the user in there, and sends the resulting token.

```toml
[auth]
type = "password"          # required

[oauth]
issuer     = "https://idp.example/realms/arneb"
resource   = "https://db.example/mcp"     # the public URL of /mcp
user_claim = "sub"                        # default; see below
```

Every key, its env var and its default is in
[Configuration](/guide/configuration#oauth). Setting `issuer` turns OAuth
on; without it nothing changes. `[oauth]` is read at startup only; changing
it needs a restart, and a bad value stops startup with a message naming it.

### What the server does

- The protected-resource metadata document
  ([RFC 9728](https://www.rfc-editor.org/rfc/rfc9728)) answers without
  credentials with the `resource`, the `authorization_servers` (the issuer)
  and the supported scopes. It is served only at the path derived from
  `resource`, `/.well-known/oauth-protected-resource` followed by the
  resource's path (`/.well-known/oauth-protected-resource/mcp` for
  `https://db.example/mcp`); the bare well-known path and any other suffix
  answer a plain `404`.
- A `/mcp` request without credentials gets `401` with
  `WWW-Authenticate: Bearer realm="arneb", resource_metadata="…"`, which is
  where an OAuth client starts. A JWT that is presented but refused
  (expired, wrong audience, bad signature, an ID token, ...) gets the same
  `401` body with `error="invalid_token"` added to the challenge
  ([RFC 6750](https://www.rfc-editor.org/rfc/rfc6750#section-3.1)), so a
  client can refresh its token instead of starting over; the reason itself
  is only logged at DEBUG on `arneb::oauth`. The `401` body is the same as
  everywhere else.
- A bearer that looks like a JWT (three dot-separated parts, not starting
  with `arneb_`) is validated: the signature against the issuer's JWKS,
  `iss`, `aud`, `exp` / `nbf`, that it is an access token, and
  `required_scopes`. Every key published under the token's `kid` that fits
  its algorithm is tried, so an algorithm migration (say RS256 and PS256
  under one `kid`) keeps working. Only asymmetric algorithms are accepted
  (RS256/384/512, PS256, ES256/384); `none` and `HS*` are always refused.
- **Access tokens only.** A header `typ` of `at+jwt` (RFC 9068) is an access
  token. Many IdPs (Keycloak, Entra ID, Okta) send `typ: JWT` instead, so
  that is accepted too, but such a token is refused when it carries a claim
  only OIDC ID tokens have (`nonce`, `at_hash`), or a Keycloak payload
  `typ` other than an access token's (`ID`, `Refresh`, `Offline`, `Logout`).
  Set `require_access_token_typ = true` when your IdP sends `at+jwt`, to
  accept nothing else.
- **Key cache.** The JWKS comes from the discovery document (a remote
  issuer's discovered `jwks_uri` must be `https`) and is cached for
  `jwks_cache_secs`. From 80% of that age it is refetched in the
  background, so requests never wait for a routine refresh. If the IdP is
  down then, the last good keys keep deciding until they are
  `jwks_max_stale_secs` old (default 24 h). A token with an unknown `kid`
  waits for a refetch, at most one per `jwks_min_refresh_secs`. A fetch
  runs on its own: a client that disconnects never cancels it.
- The token's `user_claim` is the Arneb user. That user runs the tools with
  their own access rules, and `whoami` reports `via: "oauth"`. Audit entries
  of manage actions carry `via: "oauth"`.
- API tokens, sessions and Basic credentials keep working on `/mcp` as before.

Design decisions you should know about:

- **`/mcp` only.** A JWT on `/api/*`, `/metrics` or the Web UI is just an
  invalid bearer (`401`).
- **The user must already exist.** Arneb does not create users from tokens. A
  valid token for a name with no Arneb user is `403 forbidden` (not `401`,
  which would send the client round its login flow again). Create the user
  once with `POST /api/v2/users` or `[[auth.users]]`; the password is only
  for pgwire and Basic, an OAuth-only user never uses it. Deleting the user
  ends their OAuth access on the next request.
- **`sub` is the default `user_claim`** because it is the only claim OIDC
  guarantees stable and unique. `preferred_username` and `email` can be
  edited by users at some IdPs; only use them when the IdP forbids that, or a
  user could take over another Arneb user. The server logs a WARN at startup
  when the claim is not `sub`. With `sub` the Arneb user names are the IdP's
  subject ids.
- **`user_claim = "email"` needs a verified address.** The token's
  `email_verified` must be `true` (or the string `"true"`); otherwise the
  answer is `403 forbidden`, "the identity's email address is not verified".
  An IdP that does not send `email_verified` (Entra ID, for one) cannot be
  used with `email`.
- **The audience is the resource URL.** Tokens must be issued for this
  resource (RFC 8707): `aud` has to contain `audience`, which defaults to
  `resource`. Set `audience` only for an IdP that can emit nothing but a
  client id. In that client-id mode an OIDC ID token for the same client
  carries the same `aud`, so startup refuses an `audience` other than
  `resource` unless `required_scopes` is set too (ID tokens carry no
  scope).
- **If the IdP is unreachable** and no cached key can decide, `/mcp` answers
  `503 auth_provider_unavailable` with `Retry-After: 5`, not `401`. API
  tokens, sessions and Basic keep working.
- **An OAuth identity has the same rights as the user**, including
  `create_token`. A token minted this way never outlives the access token
  that minted it: its expiry is clamped to the JWT's `exp` and the result's
  `hint` says so. Revoking the user at the IdP does not revoke Arneb API
  tokens they minted earlier (until that expiry); revoke those through the
  API, or delete the Arneb user.
- **Plain `http`** is accepted for `issuer` and `resource` only on a
  loopback host (local testing). Anything else must be `https`, and
  `resource` must end in `/mcp`.

### Identity provider checklist

1. Create an OIDC realm / tenant whose access tokens are signed JWTs with an
   asymmetric key (not opaque tokens, not HS256).
2. Make the access token's `aud` contain exactly the `resource` URL. In
   Keycloak that is an Audience mapper on the client with *Included Custom
   Audience* set to the URL; in other IdPs an API / resource identifier.
3. Make sure the claim named by `user_claim` is in the access token (not
   only in the ID token), and that it equals an Arneb user name. A user
   name is 1 to 64 characters of `A-Z a-z 0-9 _ . @ -`; a value outside that
   set (Auth0 subjects such as `auth0|64f...` contain `|`) answers `403`
   with "the identity's 'sub' claim is not a valid Arneb user name". With
   Auth0, point `user_claim` at a custom claim (added by an Action) holding a
   valid name, or seed the user in `[[auth.users]]` under a valid name that
   the claim carries.
4. Create the matching Arneb users.
5. Allow the MCP client's redirect URI. MCP clients are public clients
   using the authorization-code flow with PKCE; what a given client needs
   (a registered client id, or dynamic client registration) is up to the
   IdP and the client.
6. Optionally set `required_scopes` to a scope the IdP grants only to
   permitted clients.

`docker/keycloak/arneb-realm.json` is a dev realm that does this (users
`alice`, `bob`, `carol` with public, dev-only passwords; client
`arneb-smoke` with the audience mapper, client `arneb-other` without). Start
it with `docker compose --profile oauth up -d keycloak` and point `[oauth]`
at `http://127.0.0.1:8180/realms/arneb`; `scripts/mcp-oauth-smoke.sh` runs
the whole scenario against it.

### Connecting claude.ai

Custom connectors need a public `https` URL, so put a TLS-terminating proxy
in front of the Web port (see [Kubernetes](/guide/kubernetes#oauth-for-mcp))
and set `resource` to its address plus `/mcp`. In claude.ai, add a custom
connector with that URL. claude.ai reads the metadata document, sends the
user to the IdP to sign in, and calls `/mcp` with the token. The signed-in
user must exist in Arneb, or every call is `403`.

## Tools

Forty-four tools. The server also sends `instructions` on connect, which tell
the agent the intended order: discover, describe or sample, explain, submit,
poll, fetch, cancel.

| Tool | Arguments | Returns | Same as |
|---|---|---|---|
| `whoami` | none | `{user, via, version, role, auth, privileges}` | `GET /whoami`, `/info`, `/health` |
| `list_catalogs` | `prefix?`, `limit?` | names, `total`, `truncated` | `GET /catalogs` |
| `list_schemas` | `catalog`, `prefix?`, `limit?` | same | `GET /catalogs/{c}/schemas` |
| `list_tables` | `catalog`, `schema`, `prefix?`, `limit?` | same | `GET …/tables` |
| `describe_table` | `catalog`, `schema`, `table`, `limit?` | columns, `truncated`, row count, size | `GET …/tables/{t}` |
| `sample_table` | `catalog`, `schema`, `table`, `rows?` | a finished statement with a first page | `GET …/sample` |
| `explain_query` | `sql`, `catalog?`, `schema?` | plan text, `plan_truncated`, scan estimates | `POST /explain` |
| `submit_query` | `sql`, `catalog?`, `schema?`, `max_rows?`, `wait_ms?` | statement status | `POST /statements` |
| `get_query_status` | `id`, `wait_ms?` | statement status | `GET /statements/{id}` |
| `fetch_results` | `id`, `cursor?`, `max_bytes?` | a page of rows and `next_cursor` | `GET /statements/{id}/results` |
| `cancel_query` | `id`, `dry_run?` | your own statement: its status after cancelling (an ended one is released, no rows); any other id, or a dry run: the manage envelope | `DELETE /statements/{id}` or `DELETE /queries/{id}` |
| `list_queries` | `state?`, `user?`, `min_elapsed_ms?`, `limit?` | `{queries, truncated}` | `GET /queries`, bounded |
| `get_query` | `id` | one tracked query | `GET /queries/{id}` |
| `get_cluster` | none | role, workers, admission counters | `GET /cluster` |
| `list_workers` | `limit?` | `{workers, truncated}`, sorted by id (default 100, at most 1000) | `GET /workers` |
| `get_worker` | `id` | one worker with its `scheduling` | `GET /workers/{id}` |
| `cordon_worker` | `id`, `dry_run?` | manage envelope | `POST /workers/{id}/cordon` |
| `drain_worker` | `id`, `dry_run?` | manage envelope | `POST /workers/{id}/drain` |
| `uncordon_worker` | `id`, `dry_run?` | manage envelope | `POST /workers/{id}/uncordon` |
| `get_admission` | none | limits, running and queued counts, `source` | `GET /admission` |
| `set_admission_limits` | `max_running_queries?`, `max_queued_queries?`, `dry_run?` | manage envelope | `PUT /admission` |
| `reset_admission_limits` | `dry_run?` | manage envelope | `DELETE /admission` |
| `list_audit_log` | `limit?`, `cursor?`, `user?`, `action?` | `{entries, next_cursor, truncated, scope, ...}` (default 50, at most 500; without manage, only your own actions) | `GET /audit` |
| `list_tokens` | none | your live tokens, never the secret | `GET /tokens` |
| `create_token` | `name?`, `expires_in_days?` | the new token, shown once | `POST /tokens` |
| `revoke_token` | `id` | `{revoked: id}` | `DELETE /tokens/{id}` |
| `get_access_rules` | none | `{rules, from_config}` | `GET /access-rules` |
| `set_access_rules` | `rules`, `dry_run?` | manage envelope | `PUT /access-rules` |
| `clear_access_rules` | `dry_run?` | manage envelope | `DELETE /access-rules` |
| `list_users` | `limit?` | `{users, truncated}` (default 100, at most 1000) | `GET /users` |
| `create_user` | `name`, `password`, `dry_run?` | manage envelope | `POST /users` |
| `set_user_password` | `name`, `password`, `dry_run?` | manage envelope | `PATCH /users/{name}` |
| `delete_user` | `name`, `dry_run?` | manage envelope | `DELETE /users/{name}` |
| `list_user_tokens` | `name`, `limit?` | `{user, tokens, truncated}` | `GET /users/{name}/tokens` |
| `issue_user_token` | `name`, `token_name?`, `expires_in_days?`, `dry_run?` | manage envelope; the token is shown once | `POST /users/{name}/tokens` |
| `revoke_user_token` | `name`, `id`, `dry_run?` | manage envelope | `DELETE /users/{name}/tokens/{id}` |
| `list_catalog_configs` | `limit?` | `{generation, catalogs, truncated}`, secrets redacted | `GET /catalog-configs` |
| `get_catalog_config` | `name` | one definition, secrets redacted | `GET /catalog-configs/{name}` |
| `create_catalog` | `spec`, `dry_run?` | manage envelope | `POST /catalog-configs` |
| `update_catalog` | `name`, `spec`, `dry_run?` | manage envelope | `PUT /catalog-configs/{name}` |
| `drop_catalog` | `name`, `dry_run?` | manage envelope | `DELETE /catalog-configs/{name}` |
| `get_settings` | none | `{slow_query_threshold_ms, source}` | `GET /settings` |
| `update_settings` | `slow_query_threshold_ms`, `dry_run?` | manage envelope | `PATCH /settings` |
| `reseed_state` | `dry_run?` | manage envelope | `POST /state/reseed` |

The result of a tool is its `structuredContent`: the same JSON body as the
matching route (an array is wrapped in an object). Every argument has a
description in the tool's input schema.

Annotations tell a client which calls are safe to auto-approve. All tools set
`openWorldHint = false`. `whoami`, the `list_*` and `get_*` tools,
`describe_table`, `sample_table`, `explain_query` and `fetch_results` are
read-only. `revoke_token`, `cordon_worker`, `drain_worker`,
`set_admission_limits`, `reset_admission_limits`, `set_access_rules`,
`clear_access_rules`, `set_user_password`, `delete_user`,
`revoke_user_token`, `update_catalog`, `drop_catalog`, `update_settings` and
`reseed_state` are destructive and idempotent; `uncordon_worker` is
idempotent and not destructive; `create_user`, `issue_user_token` and
`create_catalog` are neither. `cancel_query` is destructive but not
idempotent: the first call cancels a running statement of yours and keeps
it, a call on an ended one releases it with its rows, and a call after that
only finds the tracked query that already ended (`changed: false`).
`submit_query` and `create_token` carry no read-only hint.

### Typical session

1. `list_catalogs`, `list_schemas`, `list_tables` to find the table.
2. `describe_table` for columns, `sample_table` for a few real rows.
3. `explain_query` to check the SQL and see how much it would scan.
4. `submit_query` with `wait_ms` (up to 10000). A short query comes back
   finished with its first page.
5. If it is `queued` or `running`, `get_query_status` with `wait_ms` until it
   is `finished`, `failed` or `cancelled`.
6. A finished status carries `first_page`. Start `fetch_results` from
   `first_page.next_cursor` and repeat with each page's `next_cursor` until
   it is null (a null `first_page.next_cursor` means you already have every
   row).
7. `cancel_query` once you are done with a statement: it stops a running
   one, and frees the retained rows of one that has ended.

A statement that fails after it was submitted, including a SQL syntax error,
is a normal result whose status has `state: "failed"` and an `error` object.
Only a bad request is an error result.

## Managing the cluster

The manage tools (`cordon_worker`, `drain_worker`, `uncordon_worker`,
`set_admission_limits`, `reset_admission_limits`, and `cancel_query` on
another user's query) call the
same functions as their `/api/v2` routes, with the same checks. `whoami`
reports `privileges: {observe, manage}`. The observe tools (`list_workers`,
`get_worker`, `get_admission`, `list_audit_log`) need no extra privilege under
password auth unless a `system_information` rule withholds `read`. Manage
needs the user in `[access_control] admin_users` or a `system_information`
rule that allows `write`; under `[auth] type = "none"` every manage call is
`manage_requires_auth`.

Every manage tool takes `dry_run`. A dry run is authorized and validated like
the real call, changes nothing and is not audited; the result is the envelope
`{dry_run, action, target, summary, changed, warning, result}`. Preview first,
then repeat without `dry_run`. Real actions are written to the audit log,
which `list_audit_log` reads. `set_admission_limits` takes non-negative
integers; the server refuses a negative value or an empty call with
`invalid_argument`, as `PUT /api/v2/admission` does. An argument of the wrong
type (`1.5`, a string, a number above the 64-bit range) comes back as a text
error naming the value and the expected type, not the `{code}` envelope.

### Runtime configuration

The access rules, users, user tokens, catalogs, settings and re-seed tools
change the [runtime configuration](/guide/http-api#runtime-configuration)
exactly as their routes do, with the same checks, dry runs and audit
entries. All of them need manage, except `get_settings` (observe).

- `set_access_rules` takes the whole rules file as `rules` (a JSON object). A
  change that would remove your own manage privilege is refused with
  `conflict`, also as a dry run.
- `create_user` and `set_user_password` take a password. It is not logged or
  audited; the tool descriptions tell the agent not to repeat it back to the
  user. `issue_user_token` shows the new token once.
- `create_catalog` and `update_catalog` take the spec as `spec` (the
  `[[catalogs]]` fields). The coordinator connects to the backend first (at
  most 10 s); every result carries the spec with its secrets as
  `"<redacted>"`. To change a catalog, read it with `get_catalog_config`,
  edit the fields and send it back: a field left as `"<redacted>"` keeps the
  stored secret, unless the change moves where it is sent, which needs the
  secret again.
- `reseed_state` replaces catalogs, rules, settings and config users with
  the config file's. Preview it with `dry_run: true` first.

## Limits

Results are bounded the way the [HTTP API](/guide/http-api#limits-and-truncation)
bounds them, and a value above a ceiling is refused with `invalid_argument`,
never clamped. The ceilings come from `[agent_api]`.

| Argument | Default | Ceiling | Source |
|---|---|---|---|
| `limit` of `list_catalogs`, `list_schemas`, `list_tables` | 1000 | 10000 | `[agent_api] default_max_rows`, `max_rows` |
| `limit` of `describe_table` (columns) | 1000 | 10000 | `[agent_api] default_max_rows`, `max_rows` |
| plan text of `explain_query` | 32768 bytes, cut and flagged `plan_truncated` | none | `[mcp] default_page_bytes` |
| `rows` of `sample_table` | 10 | 100 | fixed |
| `max_rows` of `submit_query` | 1000 | 10000 | `[agent_api] default_max_rows`, `max_rows` |
| `wait_ms` | 0 | 10000 | ceiling is `[agent_api] max_wait_ms` |
| `max_bytes` of `fetch_results` | 32768 | 4194304 | default is `[mcp] default_page_bytes`, ceiling is `[agent_api] max_page_bytes` |
| `limit` of `list_queries` | 50 | 500 (0 is refused) | fixed |

The tool descriptions, their argument descriptions and the server
instructions quote the configured numbers: they are filled from
`[agent_api]` and `[mcp] default_page_bytes` when the server starts, so
what an agent reads is what the server enforces. The table shows the
defaults.

Differences from the HTTP routes:

- **Smaller pages.** An agent pays for every byte of a result in its
  context, so the default page is `[mcp] default_page_bytes` (32768 bytes)
  rather than the 256 KiB of `/api/v2`. It applies to `fetch_results` when
  `max_bytes` is omitted, and to the inline first page of `submit_query`,
  `get_query_status` and `sample_table` (`cancel_query` returns no rows). Ask for more with `max_bytes`, up to
  the `[agent_api] max_page_bytes` ceiling. The budget counts the bytes of
  the rows only. The status fields, the `columns` list and the text copy of
  the JSON that the MCP library adds to every result come on top, so a full
  page roughly doubles in what the agent receives.
- **Plans fit the page budget.** `explain_query` cuts the plan at the same
  `[mcp] default_page_bytes` budget (`/api/v2/explain` keeps
  `[agent_api] default_page_bytes`). Like every tool result, the text block
  repeats the structured JSON, so clients that only pass text content to the
  model still see the plan.
- **Bounded history.** `list_queries` returns the newest queries first, at
  most `limit`, and sets `truncated` when more matched. Each query's `state`
  is lowercase (`queued`, `running`, `finished`, ...), unlike
  `/api/v2/queries`, which keeps `Finished`. The `state` filter is one of
  the eight states the list reports, `queued`, `planning`, `starting`,
  `running`, `finishing`, `finished`, `failed` or `cancelled` (any case), and
  keeps exactly that state: `running` does not include `planning`,
  `starting` or `finishing`. Another value is `invalid_argument`. `user` keeps one owner's queries and
  `min_elapsed_ms` keeps only slow ones. The `sql` of a query in this tool and
  in `get_query` is cut at `max_cell_bytes` and flagged `sql_truncated`.
- **Cancel.** `cancel_query` with the id of your own statement from
  `submit_query` works like `DELETE /api/v2/statements/{id}` and returns the
  statement status: a running statement is cancelled and kept, an ended one
  is released with its rows. Any other id (a query id from `list_queries`,
  or a statement already released or evicted), or any call with `dry_run`,
  works like `DELETE /api/v2/queries/{id}` and returns the manage envelope;
  killing another user's query needs the manage privilege or an explicit
  `queries` rule that allows `kill` (with no `queries` rules, only the owner
  or a manager may kill).
- **Truncation and eviction.** The statement status is the `/api/v2` one, so
  a truncated result carries `truncated_by` (which limit cut it, including
  `user_budget` for your share of retained results); `truncated_hint` (what
  to do) is set only when no row could be kept. An ended statement whose
  rows were evicted to make room answers `not_found` from `get_query_status`
  and `fetch_results` with a message saying so and a hint to resubmit; see
  [eviction](/guide/http-api#limits-and-truncation).
- **Tokens need password auth.** `create_token`, `list_tokens` and
  `revoke_token` answer `auth_disabled` under `type = "none"`. A token that
  mints a token cannot outlive its parent: the expiry is clamped and the
  result's `hint` says so.

## Errors

A failed call is a tool result with `isError: true` whose `structuredContent`
is the `/api/v2` [error envelope](/guide/http-api#errors), with the same
codes:

```json
{"error":{"code":"column_not_found","message":"…","hint":"…",
          "location":{"line":1,"column":8},"candidates":["l_quantity"]}}
```

`hint` says what to do next, and `candidates` lists the nearest existing
names for a misspelled table or column, so an agent can fix the call and
retry.

JSON-RPC errors are only for protocol faults, such as an unknown method or a
malformed JSON-RPC message. Bad tool arguments (a missing required field, a
wrong type) also come back as a tool result with `isError: true`, with a
text message that names the field.

## Configuration

`[mcp]` is on by default. See the
[configuration guide](/guide/configuration#mcp) for the full table.

```toml
[mcp]
enabled            = true
allowed_hosts      = ["db.internal"]   # omit for the default
allowed_origins    = ["https://app.example:443"]   # omit to refuse any Origin
default_page_bytes = 32768
```

With `enabled = false` (or `ARNEB_MCP_ENABLED=false`) `/mcp` is not served
and answers `404`, under password auth too (it asks for no credentials). The effective values are logged once on `arneb::config`
at startup.

### Host allow-list

`/mcp` checks the request's `Host` header against `allowed_hosts` to stop
DNS-rebinding attacks from a browser page.

- Under password auth the default is any host: a cross-site page cannot
  attach the bearer token, so rebinding gains nothing.
- Under `type = "none"` the default is loopback only (`localhost`,
  `127.0.0.1`, `::1`). A remote client then needs `allowed_hosts` set.
- Entries are `host` or `host:port`. A `*` anywhere in the list means any
  host. An empty list is a startup error.

### Origin check

`/mcp` also checks the browser `Origin` header, as the MCP transport
requires. A request without `Origin` (a CLI or desktop MCP client) always
passes. By default every request that carries an `Origin` is refused with
`403`, so a web page cannot drive `/mcp` from a visitor's browser. To allow a
browser-based client, list its origins in `allowed_origins`: each entry has a
scheme, as in `https://app.example:443`, `http://localhost:*` (any port) or
`null`. An entry without a port currently matches any port; give the port
explicitly.

## Security

- **Tool results are kept out of debug logs.** The MCP library logs every
  tool result at `debug`, and a result can hold rows or a new API token from
  `create_token`, and it logs three `info` lines for every request. Arneb
  therefore caps the `rmcp` log target at `warn`, even under
  `RUST_LOG=debug`; its warnings (a rejected `Origin` or `Host`) still log.
  A directive on an rmcp module, such as `RUST_LOG=debug,rmcp::transport=warn`,
  keeps the cap for the rest of rmcp and still applies to that module. Only
  a directive whose target is exactly `rmcp` (for example
  `RUST_LOG=info,rmcp=debug`) lifts the cap. That is an explicit opt-in:
  at `debug` rmcp logs tool results, new API tokens included, as does
  naming its `rmcp::service` module at `debug`. Do either only on a
  machine whose logs are as private as the tokens.
- **Host and Origin checks** stop DNS rebinding and cross-site calls; see
  [Host allow-list](#host-allow-list) and [Origin check](#origin-check).
- **Plain HTTP.** Put TLS in front of the Web port for remote clients (see
  [Connect](#connect)).

## Stateless behavior

The endpoint keeps no MCP session. Every `POST /mcp` is independent and
answers with a JSON body, so a coordinator restart or a load balancer never
strands a client, and no session id is issued. There is no
server-sent stream: `GET /mcp` answers `405 Method Not Allowed`
(`Allow: POST`) whatever its `Accept` header says, which the streamable HTTP
transport permits. The server never pushes notifications.

Statements created through `submit_query` do live in coordinator memory,
exactly as with the HTTP API: they are lost on restart, and an idle one is
reaped after `[agent_api] idle_timeout_secs`.

A request with no credentials, or with a bad one, gets HTTP `401` with
`WWW-Authenticate: Bearer realm="arneb"` before any MCP handling.
