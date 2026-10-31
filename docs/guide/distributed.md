# Distributed Mode

Arneb supports distributed query execution using a coordinator-worker architecture. The coordinator accepts SQL queries, plans execution, and dispatches tasks to workers via Apache Arrow Flight RPC.

## Roles

| Role | pgwire | Web UI | Flight RPC | Description |
|------|--------|--------|------------|-------------|
| `standalone` | yes | yes | yes | Single process, all-in-one (default) |
| `coordinator` | yes | yes | yes | Accepts SQL, plans queries, dispatches to workers |
| `worker` | no | no | yes | Executes plan fragments, serves data |

## Coordinator Setup

Create `coordinator.toml`:

```toml
bind_address = "0.0.0.0"
port = 5432

[[tables]]
name = "lineitem"
path = "/data/lineitem.parquet"
format = "parquet"
```

Start the coordinator:

```bash
cargo run --bin arneb -- --config coordinator.toml --role coordinator
```

The coordinator listens on:
- Port `5432` — pgwire (SQL clients)
- Port `6432` — Web UI
- Port `9090` — Flight RPC (worker communication)

## Worker Setup

Create `worker.toml`:

```toml
bind_address = "0.0.0.0"

[[tables]]
name = "lineitem"
path = "/data/lineitem.parquet"
format = "parquet"

[cluster]
rpc_port = 9091
coordinator_address = "127.0.0.1:9090"
worker_id = "worker-1"
```

Start the worker:

```bash
cargo run --bin arneb -- --config worker.toml --role worker
```

Workers do not expose pgwire or Web UI ports. They communicate with the coordinator via Flight RPC only.

::: tip
Workers need the same table definitions as the coordinator. Each worker should have access to the same data files or object store paths.
:::

## Adding More Workers

Each worker needs a unique `worker_id` and `rpc_port`:

```toml
# worker-2.toml
bind_address = "0.0.0.0"

[[tables]]
name = "lineitem"
path = "/data/lineitem.parquet"
format = "parquet"

[cluster]
rpc_port = 9092
coordinator_address = "127.0.0.1:9090"
worker_id = "worker-2"
```

Workers register with the coordinator automatically via a heartbeat protocol on startup.

### Versions

Every node in a cluster must run the same Arneb release. Each heartbeat
carries the worker's version, and the coordinator refuses a worker on any
other release, so no query is ever scheduled on it. The coordinator logs
`worker rejected: version mismatch` once per worker and version, and the
worker logs `heartbeat rejected` with both versions at error level on every
attempt. A refused worker keeps retrying, so replacing the coordinator with
the worker's release lets it join without a restart. The Cluster page and
`GET /api/v1/cluster/workers` show each worker's version.

During an upgrade, workers still on the old release drop out of the cluster
as soon as the new coordinator starts, and rejoin as they are replaced.
Releases before 0.1.6 have no version in their heartbeat and cannot talk to
0.1.6 or later in either direction, so upgrade the coordinator and every
worker together when moving from 0.1.5.

## Securing the Cluster

Out of the box, the Flight RPC port trusts every peer, and the client-facing
ports are plaintext. For anything beyond a trusted single host, turn on both
cluster settings on **every node**, plus client TLS and passwords on the
coordinator:

```toml
# every node (coordinator and workers)
[cluster]
secret = "…"        # or ARNEB_CLUSTER_SECRET — same value on every node, >= 32 chars

[cluster.tls]
cert_path = "/etc/arneb/node.crt"
key_path  = "/etc/arneb/node.key"
ca_path   = "/etc/arneb/ca.crt"

# coordinator only: client-facing TLS and passwords
[tls]
cert_path = "/etc/arneb/server.crt"
key_path  = "/etc/arneb/server.key"

[auth]
type = "password"
[[auth.users]]
name = "alice"
password_hash = "SCRAM-SHA-256$…"   # arneb hash-password
```

- The **secret** authenticates every Flight call: heartbeat registration,
  task submission, data exchange and status. A worker with a wrong or
  missing secret is rejected and never registered; it logs an ERROR naming
  `[cluster] secret` and keeps retrying. The coordinator logs the rejection
  on `arneb::cluster`.
- **Cluster TLS** encrypts exchange traffic and keeps the secret off the
  wire. Without it, the secret is sent in the clear, and startup logs a
  WARN on non-loopback binds. Each node's certificate must name the host its
  peers dial (the `advertised_address` host for workers, the
  `coordinator_address` host for the coordinator). Keep `host:port` in those
  settings; nodes switch to `https://` automatically.
- Cluster TLS does not authenticate peers; the secret does. A node with
  `[cluster.tls]` but no secret refuses to start on a non-loopback bind.
- Every node must use the same mode: a plaintext node cannot talk to a TLS
  node, and the failure shows up as heartbeat errors on the worker.
- With password auth, the coordinator's Web UI binds to `127.0.0.1` unless
  `[web] bind_address` is set.
- Per-user read permissions on catalogs, schemas and tables come from an
  [access rules file](/guide/access-control) on the coordinator.

`./scripts/dev-certs.sh` creates a throwaway CA and a `localhost`
certificate that every node on one host can share. See
[Cluster security](/guide/configuration#cluster-security) and
[TLS](/guide/configuration#tls) for every setting.

## When a Worker Fails

When a worker is lost, the queries running on it fail cleanly and the
cluster keeps serving (unless [query retry](#query-retry) is on):

- **The worker crashes or its connection breaks.** Its queries fail at once
  with `worker '<id>' (<address>) failed: …`. The coordinator marks the
  worker dead, so the next query runs on the remaining workers.
- **The worker hangs** (stuck process, network partition). It stops sending
  heartbeats. Once `heartbeat_timeout_secs` passes (default 10 s, with a
  heartbeat every `heartbeat_interval_secs`, default 2 s), its queries fail
  with `worker '<id>' (<address>) lost: no heartbeat for <n> s`.
- In both cases the coordinator cancels the query's tasks on every other
  worker, so they release their memory right away.
- **A worker that comes back** (restarted or unfrozen) rejoins with its next
  heartbeat. The coordinator logs `worker re-registered`, and new queries use
  it again.

### Query retry

With `retry_policy = "query"` in `[cluster]` (or `ARNEB_RETRY_POLICY=query`)
on the coordinator, a query that fails because one of its workers was lost
runs again from the start on the remaining workers, like Trino's
`retry_policy=QUERY`. It is off by default.

- Only read-only queries are re-run. `INSERT`, `CREATE TABLE AS` and other
  writes fail as before.
- Only a loss of one of the query's workers triggers a re-run: a crash, a
  broken connection, a missed heartbeat deadline, or a worker that left the
  cluster. A query that fails for any other reason (a SQL error, the memory
  limit, a timeout, a cancel) is not re-run.
- A query is re-run at most `query_retry_attempts` times (default 4). The
  first re-run waits 1 s, and each later one waits twice as long, up to
  10 s. With no worker left, the query fails with the last error.
- The client sees nothing of the failed runs: results are collected in full
  on the coordinator before any row is sent, so no row arrives twice. The
  [`statement_timeout`](/guide/configuration#admission-control) covers
  all of a query's runs.
- Each re-run logs `retrying query after worker loss` on `arneb::cluster`,
  with the lost worker and the error, and counts in
  `arneb_query_retries_total` on `/metrics`. Startup logs the effective
  policy on `arneb::config`.

A hung worker is detected only after `heartbeat_timeout_secs`, so a re-run
after one starts about 10 s after the hang, plus the delay.

`benchmarks/tpch/scripts/fault_injection.sh` checks all of this on the local
bench cluster by killing and pausing a worker during SF10 q09. With
`RETRY=1` it runs the coordinator with `retry_policy = "query"` and checks
that q09 completes instead, with the same rows as a run without a fault.

`benchmarks/tpch/scripts/concurrency_soak.sh [rounds]` runs rounds of 20
concurrent TPC-H queries against a running cluster and exits non-zero if any
fails. With `LOG_DIR` pointing at the nodes' log files, it also counts the
[exchange events](/guide/observability#exchange-events) each node logged.
With the default admission cap of 8, the rest of each round waits in the
queue. Lifting it (`ARNEB_MAX_RUNNING_QUERIES=0`) runs all 20 at once, but
with an unbounded memory pool that needs the memory for 20 concurrent SF10
queries: on a 16 GB Docker Desktop VM it gets a worker or RustFS OOM-killed
within a few rounds.

## Stopping a Worker

Send a worker SIGTERM (or press Ctrl-C) to take it out of the cluster
without failing queries, as Kubernetes does on a rolling upgrade:

1. The worker logs `worker draining` and tells the coordinator at once. The
   coordinator schedules no new query on it; the Cluster page shows it as
   *Draining*, and `GET /api/v1/cluster/workers` reports
   `state: "draining"`. Queries already running on it keep running.
2. The worker exits once the coordinator has finished every query it holds,
   logging `worker drained`, and deregisters (`worker deregistered` on the
   coordinator). It waits at least one heartbeat interval first, so a query
   that was being planned when the drain began is covered too.
3. If queries are still running after `drain_timeout_secs` (default 120 s),
   it logs `drain timed out` with the number left and exits; those queries
   fail as for a lost worker. A second SIGTERM or Ctrl-C exits at once.

Give the process manager more time than `drain_timeout_secs` before it
force-kills the worker. On Kubernetes, set `terminationGracePeriodSeconds`
above it; the default 30 s is shorter than the default drain timeout.

When a query finishes, the coordinator sends each worker that ran part of it
`release_query`, and they drop the query's tasks and exchange buffers right
away. That is how a draining worker knows it is idle, and it also frees
broadcast buffers that used to stay in memory for up to 600 s.

The coordinator and standalone server stop on SIGTERM or Ctrl-C at once;
they do not drain.

To take a worker out of scheduling without stopping it, cordon or drain it
from the coordinator's [HTTP API](/guide/http-api#workers)
(`POST /api/v2/workers/{id}/cordon|drain|uncordon`, manage privilege). That
is coordinator-side: the worker keeps running and heartbeating, its
lifecycle `state` stays `active`, and the API reports a separate
`scheduling` field (`cordoned`, `draining`, `drained`). A drained worker
stays in the cluster until it is uncordoned or stopped. The cordon is kept
in the state store across coordinator restarts. SIGTERM drain-then-exit,
above, is unchanged and works the same on a cordoned worker.

## How It Works

1. A SQL query arrives at the coordinator via pgwire
2. The coordinator parses, plans, and optimizes the query into a `LogicalPlan`
3. The `PlanFragmenter` splits the plan into fragments suitable for distributed execution
4. The `NodeScheduler` assigns fragments to available workers
5. Workers execute their fragments and return results via Flight RPC
6. The coordinator assembles the final result and sends it to the client

## Multi-Node Example

Start a coordinator and two workers on a single machine:

**Terminal 1 — Coordinator:**
```bash
cargo run --bin arneb -- --config coordinator.toml --port 5432 --role coordinator
```

**Terminal 2 — Worker 1:**
```bash
cargo run --bin arneb -- --config worker-1.toml --role worker
```

**Terminal 3 — Worker 2:**
```bash
cargo run --bin arneb -- --config worker-2.toml --role worker
```

**Terminal 4 — Query:**
```bash
psql -h 127.0.0.1 -p 5432 -c "SELECT count(*) FROM lineitem;"
```
