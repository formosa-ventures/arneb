# Phase 3 roadmap — production readiness + federation

**Date:** 2026-10-16
**Status:** Approved design; each work item becomes its own OpenSpec change.

**Progress (2026-10-18, v0.1.3):** P2 admission control (`admission-control`, #138)
and P3 security (`security-tls-auth`, #136) are done, with review follow-ups in
#149–#152. P1's code is done (scan accounting, #137 / #148); its SF30
validation tasks (tracked-vs-RSS ratio, q21 SF30 distributed, 3-run stability)
and the SF100 spill smoke remain. #126 traced to the seed's file layout and is
fixed by #135. M2–M4 not started.

**Progress (2026-10-20, v0.1.4):** M2 F1 + F2 landed in #158
(`rdbms-postgresql-connector`, archived). F4 is `cross-source-join`: a mixed
TPC-H layout (dimensions in PostgreSQL, facts in Hive), dynamic-filter pushdown
into PostgreSQL with `IN`-list compaction, and the mixed-layout correctness gate.

**Progress (2026-10-22, v0.1.5):** M2 is done: F4 landed in #167
(`cross-source-join`; mixed-layout gate 22/22 at SF10, standalone and
distributed). M3: P4 stage 1 landed in #171 (`fault-tolerance-fail-clean`;
stage 2, query-level retry, is `query-retry`). P5 landed in #172
(`observability-metrics`: `/metrics`, slow-query log, Grafana dashboard) and
#173 (`query-history`: bounded history, `system.runtime.queries`, and the
Web UI's query list reads the same history). P6 is
`kubernetes-deployment` (GHCR images, `charts/arneb`, kind acceptance).

**Plan after v0.1.5 (2026-10-23):** see "Path to production" at the end of
this document. It adds gaps the original milestones did not cover (G1–G6)
and sequences the remaining work into releases ending at v0.2.0.

## Intent

Phase 1–2.5 made Arneb a correct, fast single-node and distributed engine:
TPC-H 22/22 cell-identical to Trino, faster on all 22 at SF10 and SF30 cold,
at ~0.14× Trino's peak memory. Phase 3 turns that engine into something a team
can run, and returns to the project's stated purpose — federated queries.

Phase 3 is done when **both** of these scenarios actually run:

1. **Replace an internal Trino cluster.** A team of ~5–10 people points
   BI / ad-hoc tooling (Superset, Metabase, dbt) at Arneb. Concurrent and
   oversized queries do not take the process down, unauthenticated users
   cannot connect, a lost worker does not wedge the cluster, and operators
   can see what is happening.
2. **Lakehouse + OLTP federation.** One SQL statement joins Hive/Iceberg
   tables with PostgreSQL (and later MySQL) tables, correctly, with filters
   and projections pushed into the remote database.

## Structure

Four milestones, alternating between the two tracks so that every milestone
ends with a scenario that can be demonstrated.

| Milestone | Theme | Work items | Depends on |
|---|---|---|---|
| M1 | Safe to share with a team | P1 memory accounting + spill, P2 admission control, P3 security | — |
| M2 | Federation MVP | F1 RDBMS connector framework, F2 PostgreSQL connector, F4 cross-source join | M1/P3 |
| M3 | Survives failure, observable | P4 fault tolerance, P5 observability, P6 deployment | M1 |
| M4 | Wider federation | F3 MySQL connector, deeper pushdown | M2 |

M1 comes first because federation exposes operational databases: Arneb must
authenticate users and bound memory before it is pointed at one.

Every work item follows the project's validation tiers (`CLAUDE.md` →
"Performance validation workflow"): Tier 1 local `cargo`, Tier 2 local
distributed plumbing, Tier 3 remote SF30 for final numbers only. Runtime
knobs follow `docs/guide/configuration.md` — an `ARNEB_*` variable read,
applied and logged by the binary.

---

## M1 — Safe to share with a team

### P1 — Finish memory accounting and spill

Completes the open `exec-memory-accounting` change (14/23 tasks done at the time of writing).

- Bring Parquet scan decode buffers under `MemoryReservation`, after first
  verifying the produced `RecordBatch` is not double-counted downstream.
- Measure SF30 worker tracked-bytes vs RSS and record the converged ratio.
- Investigate and fix issue #126 (q18 SF10 peak memory 2.0–4.7 GB across
  identical runs).

**Acceptance**
- q21 SF30 completes in distributed mode on the constrained host.
- SF30 22/22 stable across 3 repeated runs: no OOM, no bistable outliers.
- SF1 trino-diff stays cell-identical (1e-9).

### P2 — Admission control

- Per-query memory limit (`query_max_memory`); exceeding it fails the query
  with a clear error instead of exhausting the host.
- Wire the existing `ResourceGroupManager` into dispatch: a concurrency cap
  with a queue for queries over the cap.
- Statement timeout: `SET statement_timeout` on pgwire,
  `query_max_execution_time` session property on the Trino protocol.
- Defaults, caps and timeouts are runtime knobs (`ARNEB_*`, logged at start).

**Acceptance**
- 20 concurrent SF10 queries: the process survives and queuing is observable.
- A query over its memory limit fails with a `53200`-class error.
- A timed-out query is cancelled and its memory and tasks are released.

### P3 — Security

- TLS on pgwire (`sslmode=require` / `verify-full`).
- HTTPS on the Trino listener with HTTP Basic auth against the shared
  `[auth]` user store (issue #85).
- Coordinator ↔ worker Flight RPC authenticated with a shared secret;
  TLS optional.

**Acceptance**
- `psql "sslmode=verify-full"` connects; `trino --password` works over https.
- A worker presenting a wrong secret is rejected at registration.
- With none of these configured, behavior is unchanged.

**Order:** P1 before P2 (the memory limit depends on accurate tracking).
P3 is independent and can run in parallel.

---

## M2 — Federation MVP

### F1 — RDBMS connector framework

A new crate (working name `crates/rdbms`) shared by all remote-SQL connectors.

- A `Dialect` trait owning type mapping, translation of pushed-down
  expressions into remote SQL, and identifier quoting. A new database is one
  dialect implementation.
- Pushdown: projection, filter (comparisons, `IN`, `IS NULL`, `LIKE`), limit.
  Untranslatable expressions stay in Arneb; they never fail the query.
- Connection pooling, and splits by numeric primary-key range so several
  workers read in parallel.
- Built on the existing `ConnectorFactory` + `DataSource` traits. If the
  pushdown interface is insufficient, the change's `design.md` states exactly
  what is extended and why.

### F2 — PostgreSQL connector

- `tokio-postgres`; configured as `[[catalogs]] type = "postgresql"`, with
  schema and table discovery.
- Read-only. Writes and DDL are out of scope.
- Types: everything TPC-H uses, plus `timestamp`, `timestamptz`, `uuid`,
  `text`, `numeric`. Unsupported types fail with a clear error.

F1 and F2 are designed together (a concrete dialect keeps the framework
honest) but land as two OpenSpec changes.

### F4 — Cross-source join and validation

- Add PostgreSQL to `docker-compose.yml`; give the bundled Trino a
  `postgresql` catalog over the same database.
- Mixed TPC-H layout: dimensions (`nation`, `region`, `customer`,
  `supplier`, `part`, `partsupp`) in PostgreSQL, facts (`orders`,
  `lineitem`) in Hive.
- Dynamic filters reach PostgreSQL: build-side keys pushed as `IN (...)` or
  a range predicate.

**Acceptance**
- Mixed-layout TPC-H 22/22 cell-identical to Trino on the same layout.
- PostgreSQL's query log shows filters and projections pushed down.
- Mixed-layout SF10 latency and memory reported next to Trino. No win
  threshold — M2 gates on correctness and pushdown behavior.

---

## M3 — Survives failure, observable

### P4 — Fault tolerance

- **Stage 1, fail cleanly.** When a worker dies mid-query, the coordinator
  detects it (heartbeat timeout and Flight stream errors) and fails the query
  with a clear error. Its tasks on the other workers are cancelled and their
  resources released — no zombie tasks. A worker that re-registers accepts new
  tasks normally.
- **Stage 2, query-level retry.** The equivalent of Trino's
  `retry_policy = QUERY`: a failed read-only query re-runs on the remaining
  workers. Off by default.
- Task-level retry (needs exchange spooling) is out of scope.

**Acceptance**
- Killing a worker during SF10 q09 fails the query within 30 s, or completes
  it with retry enabled; the next queries run normally.
- Covered by an automated Tier 2 (local distributed) test.

### P5 — Observability

- Prometheus `/metrics` on the Web UI port: query counts by state, latency
  histogram, memory pool usage, queue depth, worker liveness.
- Bounded query history, shown in the Web UI and exposed as
  `system.runtime.queries` over both pgwire and the Trino protocol.
- A slow-query log on its own `tracing` target, threshold via `ARNEB_*`.
- An example Grafana dashboard JSON.

**Acceptance**
- Metrics are scraped with correct values from the Tier 2 stack (the
  `CLAUDE.md` rule for new instrumentation).

### P6 — Deployment

- Multi-arch images pushed to GHCR on every release.
- Helm chart: coordinator and workers as separate workloads, config in a
  ConfigMap, `[auth]` and TLS material in Secrets.
- `docs/guide/kubernetes.md`.

**Acceptance**
- CI runs `helm install` on kind (1 coordinator + 2 workers) and passes a
  TPC-H SF1 smoke test.

**Order:** P4 stage 1 before P5 (worker liveness metrics reuse its
detection). P6 last, so the chart covers M1's TLS and auth settings.

---

## M4 — Wider federation

### F3 — MySQL connector

- `mysql_async`, implemented as a new F1 dialect: type mapping, backtick
  quoting, MySQL `LIKE` / collation semantics.
- Any change F3 forces on the F1 framework is recorded in its `design.md` —
  it signals that the F1 abstraction was cut in the wrong place.

**Acceptance**
- The mixed TPC-H layout with MySQL in place of PostgreSQL is 22/22
  cell-identical to Trino on the same layout.

### Deeper pushdown

All gated per dialect capability, falling back to Arneb-side evaluation.

- Aggregate pushdown: `GROUP BY` with `COUNT` / `SUM` / `MIN` / `MAX`;
  `AVG` decomposed into `SUM` + `COUNT`; partial aggregate remote and final
  aggregate in Arneb when a table is read as multiple splits.
- TopN pushdown: `ORDER BY ... LIMIT n`.
- Join pushdown when both sides are in the same PostgreSQL/MySQL catalog.

**Acceptance**
- `EXPLAIN` shows what was pushed; the remote query log shows the SQL.
- Mixed-layout correctness unchanged.
- Latency change M2 → M4 for affected queries, labeled with its tier.

---

## Out of scope for Phase 3

Candidates for a later phase:

- Iceberg delete files and Iceberg writes
- Delta Lake
- Writes to RDBMS catalogs (`INSERT` into PostgreSQL)
- Task-level retry and exchange spooling
- Other connectors (ClickHouse, Elasticsearch, …)

## Already in flight

These open changes are not part of Phase 3's milestones. Each is finished
or parked on its own merit:

- `dist-adaptive-partition` (11/19 tasks)
- `selective-dim-first-gating` (proposal only, uncommitted)
- Issue #116 — return `orc-rust` to crates.io once it supports arrow 60

---

## Path to production (added 2026-10-23, after v0.1.5)

Remaining Phase 3 items at v0.1.5: P1's SF30 validation, P4 stage 2, P6, and
M4. Checking scenario 1 ("replace an internal Trino cluster") against the
current code turned up gaps that none of the milestones cover. Each becomes
its own OpenSpec change.

### Gaps

- **G1 — Concurrent exchange failures (#139).** Under ~20 concurrent SF10
  queries on a local cluster, about one query per run fails with an h2
  `BrokenPipe` / `ConnectionReset` on a worker-to-worker Flight stream.
  Scenario 1 is exactly this workload. It is fixed before P4 stage 2, so
  retry does not hide it.
- **G2 — `cancel_query` vs a held partitioned-buffer lock (#156, item 3).**
  A cancel that loses the `try_lock` race leaves the buffer unflagged, which
  weakens P2's "a timed-out query releases its memory and tasks". Lands with
  G1.
- **G3 — Authorization.** Arneb authenticates users but does not authorize
  them: any authenticated user can read every catalog, including federated
  PostgreSQL. Minimum: file-based rules on catalog and schema read access,
  modeled on Trino's file-based access control.
- **G4 — Worker graceful drain.** On SIGTERM, a worker stops accepting
  tasks, finishes the running ones (bounded by a timeout), and deregisters.
  Without it, every Helm rolling upgrade fails in-flight queries.
- **G5 — Version handshake.** Workers report their version at registration
  and the coordinator rejects an incompatible one. This is also the
  compatibility plan #156 item 1 (scheme-less peer addresses) waits on.
- **G6 — Scenario 1 acceptance with real clients.** A Tier 2 concurrency
  soak (20 concurrent queries × N rounds, zero failures) in CI, plus smoke
  tests from Superset, Metabase and dbt.

### Releases

| Release | Theme | Items | Validation |
|---|---|---|---|
| v0.1.6 | Concurrency reliability | G1, G2, G6 soak test; G4 and G5 landed early (#178, #179) | Tier 1 + 2 |
| v0.1.7 | Deployable | P6 | Tier 1 + 2 + kind |
| v0.1.8 | Safe to share | G3, P4 stage 2 (off by default) | Tier 1 + 2 |
| in parallel | Large-scale memory | P1 SF30 validation, `dist-adaptive-partition` SF30 sweep (q18 SF30 still OOMs) | Tier 3 |
| v0.2.0 | Production-ready | Scenario 1 checklist green, BI client smoke (G6), `docs/guide/production-checklist.md` | — |

M4 follows v0.2.0.

**Order:** G1 first, because it makes the soak test and retry results
unreliable. G4 and G5 before P6 ships, so the chart's rolling upgrade does
not fail queries. G3 before v0.2.0 is announced.
