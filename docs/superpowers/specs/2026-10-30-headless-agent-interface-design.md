# Headless agent interface (v0.2.0, first priority)

**Date:** 2026-10-30 · **Status:** design, not yet planned · **Release:** v0.2.0

## Intent

Today Arneb is operated through surfaces built for people:
- SQL over pgwire or the Trino REST protocol
- a Web UI that is mostly read-only
- a TOML config file, read once at startup
- a rules file whose changes need a restart

An operator has to sit at a terminal or a browser, and some changes need
shell access to the coordinator.

v0.2.0 makes Arneb **headless**. Every operation a person does today can be
done remotely by an AI agent, over the network. The agent needs no browser,
no shell and no config-file edits. It is an external client, like a browser
hitting the Web UI; the only change is that the operator is an agent instead
of a person.

**Success, in the user's words:** "I can use an agent remotely to complete
every operation." Concretely, v0.2.0 ships when a remote agent can run the
[acceptance scenario](#acceptance) end to end against a kind cluster. The
agent gets only a URL and a credential.

This work joins v0.2.0's existing items: G6 stage 2b, the P1 SF30
validation and the production checklist. It is done **first**. The
production checklist gains a section for the agent interface.

## Goals

1. **API first.** Every operation is a stable, versioned HTTP API. The MCP
   server and the Web UI are both clients of that API. That rules out, by
   construction, anything a person can do that an agent cannot.
2. **Remote MCP.** The coordinator serves a Model Context Protocol endpoint
   over streamable HTTP and exposes the API as tools. It must work with
   clients that use a bearer token, such as Claude Code, and with clients
   that require OAuth, such as Claude's web and desktop custom connectors.
3. **Stable for an agent.** An agent follows instructions literally and has a
   finite context window, so the interface must be stricter than one built
   for people:
   - **Bounded results.** Every result has a row and byte limit. A
     truncated result says so, and gives the full count when it is known.
   - **No blocking calls.** Long queries never hold a call open: the agent
     submits, polls, fetches and cancels.
   - **Structured errors.** An error has a stable code, the position in the
     SQL when there is one, and a hint the agent can act on. For example,
     an unknown column comes with the nearest column names.
   - **Cheap discovery.** The agent can list catalogs, schemas, tables,
     columns and types, and take a bounded sample, without writing SQL
     against `information_schema`.
   - **Preview before running.** A query can be previewed with `EXPLAIN`
     before it runs, with the estimated scan size where the connector
     knows it.
4. **Full operation, in three layers.**
   - **Query:** discover, sample, explain, run, and manage one's own queries.
   - **Observe:** everything the Web UI shows today (queries, query detail,
     cluster, workers, admission counters), plus query history, the
     slow-query log, the metrics behind the Prometheus endpoint, and the
     audit log.
   - **Manage:** all of these, without a restart:
     - cancel any query
     - cordon, drain and uncordon a worker
     - change the admission limits
     - edit the access rules
     - add, change and remove catalogs
     - create, change and remove users and their API tokens
5. **Safe by default.**
   - Every call is authenticated and authorized.
   - Write privileges (DDL and DML) are enforced from the rules for every
     protocol (pgwire, Trino, API and MCP), not only for agents. This
     changes behavior in v0.2.0, so it goes in the upgrade notes.
   - Manage is a separate privilege.
   - Every manage action supports a dry run that reports what it would do,
     for example "cancels these 3 queries".
   - Every manage action that ran is written to a durable audit log.

## Non-goals

- **Infrastructure operations.** Helm and the platform keep these jobs:
  installing or upgrading binaries, scaling the worker count, issuing TLS
  certificates, and Kubernetes changes. Arneb can cordon and drain a worker,
  but it does not create or delete pods.
- **A new Web UI.** The Web UI moves onto the new API and gains only a
  minimal login. It does not need write features in v0.2.0.
- **An agent inside Arneb.** Arneb is the system an agent operates. It does
  not host a model, plan queries from natural language, or call an LLM.
- **An OAuth authorization server.** Arneb validates tokens from an external
  OIDC identity provider. It does not register clients, show consent
  screens, or issue OAuth tokens itself.
- **Multiple coordinators.** The chart runs one coordinator, and the state
  store assumes one writer.

## Anti-goals (would be a failure even if it technically works)

- **Unequal rights.** An agent can do something a person with the same
  identity could not, or the other way round. This holds for every protocol.
- **Unbounded results.** One call can return an unbounded result into the
  agent's context.
- **Untraceable actions.** A manage action cannot be traced back to an
  identity, or its trace is lost when the coordinator restarts.
- **Wider unauthenticated access.** The agent interface widens what an
  unauthenticated network peer can do. Today the Web UI API has no
  authentication, including query cancel. It is protected only by binding
  to loopback under password authentication.
- **Silent runtime changes.** A runtime change silently disappears, for
  example after a restart, or is silently overridden by the config file.

## Current state (2026-10-30)

- **Web UI API** (`crates/server/src/web/api.rs`, on `port + 1000`):
  - Routes: `GET /api/v1/queries`, `GET|DELETE /api/v1/queries/{id}`,
    `GET /api/v1/cluster`, `GET /api/v1/cluster/workers` and
    `GET /api/v1/info`.
  - It has no authentication. Under `[auth] type = "password"` it binds to
    loopback unless `[web] bind_address` is set.
  - `/metrics` is served on the same port.
- **Authentication:** `[auth]` is `none` or `password`.
  - Users and their SCRAM hashes live in the config file.
  - pgwire uses SCRAM. The Trino REST protocol uses `Authorization: Basic`
    (`crates/protocol/src/trino/auth.rs`).
- **Authorization:** a Trino-format rules file, set with `[access_control]
  rules_file` (`docs/guide/access-control.md`).
  - It has `catalogs`, `schemas`, `tables` and `queries` sections.
  - It controls reads only.
  - If the file or a section is missing, everything is allowed.
  - It is read at startup, so a change needs a restart.
- **Config:** `AppConfig` (`crates/server/src/config.rs`) is read once at
  startup. Workers build their own catalogs and connectors from their own
  copy of the config (`crates/server/src/main.rs`).
- **Drain:** a worker drains on SIGTERM and then exits. Under Helm, the
  Deployment replaces the pod.
- **Query execution over HTTP:** the Trino REST protocol (`/v1/statement`)
  already pages results and supports cancel. It is shaped for Trino clients,
  not agents: it has no row or byte limits, no structured hints and no
  discovery shortcuts.

## Design decisions

The user decided three things:
- the state store
- OAuth through an external identity provider, required in v0.2.0
- write privileges enforced on every protocol

Items marked *(my call)* were proposed by Claude and have not been confirmed
by the user.

### Runtime state

- The coordinator keeps runtime state in a **local state file on a volume**,
  with no new external dependency. The Helm chart adds a PersistentVolumeClaim
  for it. This holds catalogs, users, API tokens, access rules, mutable
  settings such as the admission limits, and the audit log.
- **The config file is a seed.** On first start, with no state file, the
  coordinator copies the config file's catalogs, users, rules file and
  mutable settings into the state. From then on the state is the source of
  truth for those objects.
- **Divergence is visible.** At every start, the coordinator compares the
  config file with the state. For each seeded object whose config entry now
  differs, it logs a WARN on `arneb::config` naming the object, so a
  ConfigMap edit is never silently ignored. *(my call)*
  *For users (H1), a WARN was not enough: an edited `password_hash` or a
  removed user must revoke access on restart, as it did before the state
  store. Config-seeded users therefore follow the config on every start;
  objects created at runtime are never touched by it. See D2 in
  `openspec/changes/headless-api-auth/design.md`.*
- **Re-seeding is explicit.** An explicit re-seed action replaces the state
  from the config file. It is a manage action, so it has a dry run and is
  audited. *(my call)*
- **Not in the state:** startup-only settings such as ports, bind addresses
  and TLS paths stay in the config file. Changing them still needs a restart
  and is out of scope.
- **Format is the builder's choice,** as long as writes are atomic and
  survive a crash.
- **Workers receive catalogs from the coordinator.** They no longer build
  catalogs from their own config. The coordinator pushes catalog definitions
  over Flight RPC on registration and on every change. *(my call)*
- **Catalog changes affect new queries only.** A running query keeps the
  catalog snapshot it was planned with. A removed catalog fails only new
  queries. *(my call)*

### Credentials and identity

- **Users:**
  - Users live in the state.
  - A user has a SCRAM password for pgwire and Trino Basic authentication,
    and any number of API tokens.
  - Users created at runtime work on every protocol.
- **API tokens:**
  - Tokens are issued per user, shown once, stored hashed and revocable.
  - A token is accepted as a bearer token on the API and MCP.
  - A token is also accepted as a password on pgwire and Trino Basic, so
    the same identity has the same access everywhere. *(my call)*
- **OAuth (required for v0.2.0):**
  - Arneb is an OAuth resource server behind an external OIDC identity
    provider, such as Keycloak, Okta or Google.
  - It validates the provider's JWTs and maps a configured claim to an
    Arneb user.
  - It serves the protected-resource metadata that MCP clients need to
    discover the provider.
- **Bootstrap without a shell:**
  - The first manager is a user from the config seed.
  - A new `[access_control] admin_users` list grants manage when no rule
    grants it. *(my call)*
  - That user obtains its first token by calling the token endpoint with
    Basic authentication.
- **Under `[auth] type = "none"`** (the default):
  - The API and MCP accept a user name the way pgwire does today.
  - Startup logs a WARN.
  - The manage layer is refused with a structured error, because an
    unauthenticated identity must not manage. *(my call)*

### Authorization

- **One policy system.** The existing Trino-format rules extend to cover
  all of it; there is no second policy system.
- **Query layer:**
  - It uses the current `catalogs`, `schemas` and `tables` rules.
  - Table rules gain Trino's write privileges, enforced on every protocol.
- **Observe layer:**
  - Query visibility follows the existing `queries` rules (view).
  - Cluster, metrics and audit views follow Trino's `system_information`
    `read`.
- **Manage layer:**
  - Killing queries follows the `queries` rules (kill).
  - Everything else needs `system_information` `write`, or a new section
    for actions that have no Trino counterpart.
- **Defaults:**
  - The current "a missing section allows everything" convention stays for
    the query layer, so existing setups keep working.
  - Observe is allowed to any authenticated user, like the Web UI today.
  - Manage is **denied unless granted**, by a rule or by `admin_users`.
    This breaks the convention on purpose. *(my call)*

### Surfaces

- **One versioned management API** is served on the coordinator's Web port,
  under a new version prefix. The current `/api/v1` routes keep working
  until the Web UI moves over, and they gain authentication.
  *(my call: a new prefix rather than breaking `/api/v1`)*
- **The Web UI** gains a minimal login that exchanges a user name and
  password for a session token. *(my call)*
- **`/metrics`** stays unauthenticated by default so existing Prometheus
  scrapes keep working. A setting can require a token. *(my call)*
- **MCP is built into the coordinator,** not run as a separate service, so
  it shares authentication, access control, the query tracker and the audit
  log. It uses the official Rust MCP SDK (`rmcp`) on the existing axum
  server, over streamable HTTP. *(my call)*
- **Worker states:**
  - **Cordon:** the worker stays up and gets no new tasks.
  - **Drain:** cordon, then wait for the worker's running tasks to finish.
  - **Uncordon:** the worker gets new tasks again.
  - Drain-then-exit stays the SIGTERM path used by Helm.
  - Cordon state is coordinator-side scheduler state and is kept in the
    state store. *(my call)*

### Stages

Each stage is its own OpenSpec change, done in this order:

| Stage | Change | Content |
|---|---|---|
| H1 | `headless-api-auth` | The state store and seeding; users and API tokens on every protocol; authentication on every Web-port route, including the existing ones; the Web UI login; the versioned API skeleton; structured errors. |
| H2 | `agent-query-api` | Discovery, sample, `EXPLAIN` with estimates, async submit / poll / fetch / cancel, row and byte limits with truncation reporting. |
| H3 | `remote-mcp` | The MCP endpoint exposing H1–H2 and the observe layer as tools, with tool descriptions written for agents. |
| H4 | `manage-api` | The privilege layers; write-privilege enforcement on every protocol; cancel any query; cordon, drain and uncordon; admission limits; dry runs; the durable audit log. |
| H5 | `runtime-config` | Runtime edits to catalogs (pushed to the workers), users, tokens, access rules and mutable settings; re-seeding. |
| H6 | `mcp-oauth` | OAuth resource server for MCP against an external OIDC provider. |

## Acceptance

The headless goal is met when a remote agent completes the scenario below.
It connects with only a URL and a credential, and has no shell, browser or
file access. It runs against a kind cluster with two workers, a Hive
catalog and a PostgreSQL catalog.

1. Discovers the catalogs, finds the TPC-H tables, and samples one.
2. Previews a TPC-H query with `EXPLAIN`, runs it, and gets a correct
   bounded result. A large `SELECT` comes back truncated, and the response
   says so.
3. Recovers from a wrong column name by using the error's hint.
4. Starts a long query, polls it, and cancels it.
5. Reads the cluster state, the query history and the slow queries.
6. Cordons a worker, checks that new queries do not use it, drains it, and
   uncordons it.
7. Adds a new catalog and queries it from both workers. Changes an access
   rule so another user loses access, and verifies the denial. No restart is
   allowed.
8. Creates a user and an API token. Connects as that user over the API and
   over pgwire, then revokes the token and is refused.
9. Restarts the coordinator through Helm, outside the agent. The catalog,
   user and rule changes and the audit log are all still there.
10. Every manage action above appears in the audit log with its identity.

**Negative checks:**
- A second identity without manage privileges attempts steps 6–8 and is
  refused with a structured error.
- An identity without write privileges is refused DDL over pgwire and over
  the API.

**OAuth:** an MCP client that requires OAuth connects through a test OIDC
provider (for example Keycloak in kind) and runs steps 1–2.

**Validation** follows CLAUDE.md:
- Tier 1 covers each API and tool.
- Tier 2 runs the scenario on the local distributed stack.
- The kind job in CI runs the scenario as a script, without an LLM.
- One manual run uses a real agent: Claude Code connected to the MCP
  endpoint.

## Left to the builder

- Exact route paths, JSON shapes, error codes, and the MCP tool names and
  descriptions.
- The state-file format and schema, and the retention of the audit log.
  Retention is a runtime knob.
- The default and maximum row and byte limits, and the poll and
  result-retention timeouts. Each is a runtime knob with an `ARNEB_*`
  override and a logged effective value.
- How the Web UI migrates to the new API, and when `/api/v1` is removed.
- How `EXPLAIN` estimates are surfaced per connector. "Unknown" is an
  acceptable answer.
- Whether the async query API reuses the Trino protocol's paging internals.
- The exact Trino write-privilege subset to support, and how it maps onto
  DDL and DML.
