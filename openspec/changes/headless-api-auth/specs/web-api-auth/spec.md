## ADDED Requirements

### Requirement: Web port authentication
Under `[auth] type = "password"`, every Web route SHALL require `Authorization: Bearer <API token or session>` or `Authorization: Basic`, except `GET /api/v1/info`, `GET /api/v2/health`, `POST /api/v2/sessions` and the static UI files. `/metrics` SHALL stay open unless `[web] metrics_auth = true` (`ARNEB_WEB_METRICS_AUTH`). A failure SHALL be `401` with `WWW-Authenticate: Bearer realm="arneb"`. Under `none`, the caller SHALL be the `X-Arneb-User` header or `anonymous`, and the `queries` rules SHALL NOT be applied.

#### Scenario: No credentials
- **WHEN** password auth is on and `GET /api/v1/queries` has no Authorization header
- **THEN** the response is `401`

#### Scenario: Public routes
- **WHEN** password auth is on and `GET /api/v2/health` has no credentials
- **THEN** the response is `200` with `"auth":"password"`

#### Scenario: Metrics auth
- **WHEN** `metrics_auth = true` and `GET /metrics` has no credentials
- **THEN** the response is `401`

### Requirement: Sessions
`POST /api/v2/sessions` with a valid user and password SHALL return a session token `arneb_s_<43 base64url>` valid for 8 hours. Sessions SHALL be kept in memory, at most 16 per user (a new one ends the oldest) and 10,000 in total. A token-shaped password SHALL be refused like a wrong password. `DELETE /api/v2/sessions/current` SHALL end the session that made the request.

#### Scenario: Login and logout
- **WHEN** alice logs in and later calls `DELETE /api/v2/sessions/current` with the session
- **THEN** the session token is refused afterwards

### Requirement: Error envelope
`/api/v2` errors SHALL be `{"error":{"code","message","hint"}}` with `code` one of `unauthenticated` (401), `forbidden` (403), `not_found` (404), `invalid_argument` (400), `auth_disabled` (409), `overloaded` (503) and `internal` (500). Under `none`, the session and token routes SHALL answer `auth_disabled`.

#### Scenario: Auth disabled
- **WHEN** auth is `none` and a caller posts to `/api/v2/tokens`
- **THEN** the response is `409` with code `auth_disabled`

### Requirement: Query rules on the Web API
`/api/v1/queries*` and `/api/v2/queries*` SHALL apply the `queries` view and kill rules under password auth. A query the caller may not view SHALL be hidden (`404`). A query the caller may view but not kill SHALL answer `403` to a cancel. A user SHALL always be able to kill their own query.

#### Scenario: Hidden query
- **WHEN** a rule denies `view` of another user's query
- **THEN** it is absent from the list and its detail is `404`

#### Scenario: Visible, not killable
- **WHEN** a rule allows `view` but not `kill`
- **THEN** `DELETE /api/v2/queries/{id}` answers `403`
