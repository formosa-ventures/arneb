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
- Every node must use the same mode: a plaintext node cannot talk to a TLS
  node, and the failure shows up as heartbeat errors on the worker.
- With password auth, the coordinator's Web UI binds to `127.0.0.1` unless
  `[web] bind_address` is set.

`./scripts/dev-certs.sh` creates a throwaway CA and a `localhost`
certificate that every node on one host can share. See
[Cluster security](/guide/configuration#cluster-security) and
[TLS](/guide/configuration#tls) for every setting.

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
