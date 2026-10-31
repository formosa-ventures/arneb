# HTTP API

The Web port (pgwire port + 1000, for example `6432`) serves a JSON HTTP API
next to the Web UI. It is how a script or an agent operates a running server
without a browser or a shell: sign in, mint an API token, then list and
cancel queries and read cluster state.

- `/api/v2` is the stable, headless API. Every error uses one
  [error envelope](#errors).
- `/api/v1` is what the Web UI uses. Its paths and JSON shapes are unchanged;
  under password auth it now needs credentials too.

The same API token also works as the password for
[pgwire and the Trino listener](#tokens-as-passwords), so one credential
covers every protocol.

## Authentication

Authentication follows `[auth] type` (see
[Authentication](/guide/configuration#authentication)).

Under `type = "password"`, every route needs credentials except:

- `GET /api/v1/info`
- `GET /api/v2/health`
- `POST /api/v2/sessions`
- the static Web UI files (the page itself asks you to sign in)

`/metrics` also stays open unless `[web] metrics_auth = true` (or
`ARNEB_WEB_METRICS_AUTH=true`); see [Web UI](/guide/configuration#web-ui).

Send one of these on each request:

| Credential | Header | Notes |
|---|---|---|
| API token | `Authorization: Bearer arneb_…` | For scripts and agents. Revocable, optional expiry. |
| Session token | `Authorization: Bearer arneb_s_…` | For the Web UI. Lives 8 hours in the server's memory. |
| Password or API token | `Authorization: Basic <base64 user:password>` | The password may be an API token. |

A missing or invalid credential gets `401` with
`WWW-Authenticate: Bearer realm="arneb"`.

Under `type = "none"` there is nothing to prove. The caller is the user in
the `X-Arneb-User` header, or `anonymous`. The `queries` access rules are
not applied, and the session and token routes answer `auth_disabled`.

Use TLS in front of the Web port when it is reachable beyond the machine:
bearer tokens and Basic credentials are sent in the clear over plain HTTP.

### Sessions

A session is a short-lived login for browsers. Exchange a user and password
(not an API token) for a session token:

```bash
curl -s -X POST http://127.0.0.1:6432/api/v2/sessions \
  -H 'Content-Type: application/json' \
  -d '{"user":"alice","password":"secret"}'
# {"token":"arneb_s_…","expires_ms":1767000000000}
```

Sessions are kept in memory, last 8 hours, and are lost when the coordinator
restarts. A user has at most 16 (a new one ends their oldest), and the
server holds at most 10,000. `DELETE /api/v2/sessions/current` with the
session token ends it. Session tokens never work on pgwire or Trino. A
token-shaped password at login is refused like a wrong password.

## API tokens

An API token is a credential for automation. It looks like
`arneb_<16 hex>_<43 base64url>`: the 16 hex characters are its id and the
rest is the secret.

```bash
# Mint a token with a session (or with Basic, or another token)
curl -s -X POST http://127.0.0.1:6432/api/v2/tokens \
  -H "Authorization: Bearer $SESSION" \
  -H 'Content-Type: application/json' \
  -d '{"name":"ci","expires_in_days":30}'
# {"token":"arneb_0123456789abcdef_…","id":"0123456789abcdef","name":"ci",
#  "created_ms":…,"expires_ms":…}
```

- The plaintext token is in this response only. The server stores a salted
  hash (the same SCRAM verifier format as a password), so it cannot show it
  again.
- `name` (at most 128 characters) and `expires_in_days` (an integer from 1
  to 3650) are optional. Without an expiry the token lives until revoked.
- A user can hold at most 100 live tokens. Revoke unused ones.
- A token minted with another token (or with Basic using a token) cannot
  outlive it: its expiry is clamped to the parent's, and the response says so
  in `hint`.
- Tokens belong to the caller. `GET /api/v2/tokens` lists only your own
  (id, name, created and expiry times, never the secret), and
  `DELETE /api/v2/tokens/{id}` revokes one of yours. Another user's id is
  `404`.
- Revocation is immediate on every protocol: pgwire, Trino and the Web API
  all refuse the token on the next request or connection. An already
  established pgwire connection is not closed: new connections and new
  requests are refused.
- Changing a user's `password_hash` in `[[auth.users]]`, or removing the
  user, revokes all of their tokens on the next start (see
  [State](/guide/configuration#state)).
- Tokens are stored in the [state file](/guide/configuration#state). With no
  `[state] path` they live in memory, the response carries
  `"persistent": false` and a hint, and they are lost when the coordinator
  restarts.

### Tokens as passwords

A token is accepted wherever the user's password is:

```bash
psql "host=127.0.0.1 port=5432 user=alice password=$TOKEN" -c 'select 1'
trino --server https://coordinator:8080 --user alice --password   # paste the token
curl -u "alice:$TOKEN" https://coordinator:8080/v1/info
```

On pgwire the token goes through the normal SCRAM exchange, so it never
crosses the network. On Trino the token is sent as a Basic password, so
password auth without `[tls]` is a startup error; disable the listener with
`--no-trino` if you only need pgwire (see
[Trino clients](/guide/trino-clients)).

## Errors

`/api/v2` errors share one body:

```json
{"error":{"code":"unauthenticated","message":"…","hint":"…"}}
```

`hint` says what to do next and is `null` when there is nothing to add.

| Code | HTTP | Meaning |
|---|---|---|
| `unauthenticated` | 401 | No valid credentials, or a wrong login. |
| `forbidden` | 403 | Authenticated, but the access rules deny it (killing a query). |
| `not_found` | 404 | No such route, query or token, or one you may not see. |
| `invalid_argument` | 400 (405 for a wrong method) | A malformed request. |
| `auth_disabled` | 409 | The route needs `[auth] type = "password"`. |
| `overloaded` | 503 | Too many pending password checks or sessions; retry (`Retry-After: 1`). |
| `internal` | 500 | An unexpected failure; the server log has the detail. |

`/api/v1` keeps its historical `400` and `404` bodies; its new `401` and
`403` responses use the envelope.

## Routes

All `/api/v2` routes need credentials, except `health` and `POST /sessions`.

| Method and path | Description |
|---|---|
| `GET /api/v2/health` | `{"status","version","auth"}`; `auth` is `none` or `password`. Open. |
| `POST /api/v2/sessions` | Body `{"user","password"}`. Returns `201 {"token","expires_ms"}`. Open. |
| `DELETE /api/v2/sessions/current` | Ends the session whose token made the request (`204`). |
| `GET /api/v2/whoami` | `{"user","via"}`; `via` is `token`, `session`, `basic` or `unauthenticated`. |
| `POST /api/v2/tokens` | Mint a token (see above). `201`. |
| `GET /api/v2/tokens` | `{"tokens":[{"id","name","created_ms","expires_ms"}]}` for the caller. |
| `DELETE /api/v2/tokens/{id}` | Revoke the caller's token (`204`). |
| `GET /api/v2/queries` | Queries the caller may view. Same parameters and body as `/api/v1/queries`. |
| `GET /api/v2/queries/{id}` | One query; `404` if missing or hidden. |
| `DELETE /api/v2/queries/{id}` | Cancel a query (`204`); `403` if the caller may view but not kill it. |
| `GET /api/v2/cluster` | Cluster overview. |
| `GET /api/v2/cluster/workers` | Workers and their state. |
| `GET /api/v2/info` | Server version and role. |

The `queries`, `cluster` and `info` bodies are the `/api/v1` JSON; only the
errors differ. The `/api/v1` routes are `/api/v1/queries`,
`/api/v1/queries/{id}` (GET, DELETE), `/api/v1/cluster`,
`/api/v1/cluster/workers` and `/api/v1/info`.

Both versions apply the [`queries` access rules](/guide/access-control):
a query the caller may not `view` is hidden (`404`), one they may view but
not `kill` answers `403`, and a user may always kill their own.

## Security notes

- **Revocation does not cascade.** Revoking a token does not revoke tokens
  that were minted with it. Revoke those by id too.
- **Timing can hint at a token count.** A pgwire SCRAM exchange tries the
  password and each live token, so its timing can reveal roughly how many
  tokens a user has. It does not reveal any secret.
- **Token-shaped config passwords.** A password in `[[auth.users]]` that
  looks like a token (`arneb_` plus 16 hex, `_`, 43 base64url characters)
  works on pgwire but not as Trino Basic, which treats token-shaped
  passwords as tokens. Do not choose such a password.
- **No login lockout.** Repeated wrong passwords are not locked out.
  Concurrent PBKDF2 checks are bounded (a burst gets `overloaded`), which
  limits the cost, not the guessing. Use long passwords, or tokens, and
  rate-limit at your proxy.
- **Plaintext tokens** appear only in the issue response. The server never
  logs them.
- **The Web port is plain HTTP.** Terminate TLS in front of it when it
  leaves the host.
