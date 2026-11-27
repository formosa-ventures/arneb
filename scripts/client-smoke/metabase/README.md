# Metabase against Arneb (Tier 2, by hand)

A fresh Metabase syncs an Arneb cluster over pgwire with its PostgreSQL
driver and answers a fixed set of questions. The same probe runs against a
PostgreSQL 17 that holds the same DDL, which is the reference, and whose
statement log is the source of the Metabase corpus that CI replays
(`crates/protocol/tests/metabase_corpus.rs`). This is not run in CI: the
Metabase image is about 1.4 GB and a run takes minutes of JVM start-up and
sync. What works and what does not is in `docs/guide/clients.md`
("Metabase").

Pinned versions: Metabase `metabase/metabase:v0.64.8.3` (LTS) and
`postgres:17`, in `compose.yml`.

| File | What it is |
|---|---|
| `compose.yml` | `metabase` on `127.0.0.1:13300`, `pgcap` (PostgreSQL with `log_statement=all`) on `127.0.0.1:15499` |
| `seed_pgcap.sql` | The TPC-H DDL in schema `default`, all of `nation`, 2 `orders` and 1 `customer`, for `pgcap` |
| `probe.py` | Sets Metabase up, adds the database, waits for the sync, prints the synced fields, runs the questions and an x-ray (standard library only) |
| `capture.py` | Turns the `pgcap` log into a corpus file and optionally replays it |

Always use compose project `arneb-mb`, so these containers never clash with
another project.

## Run

```bash
# 1. The 1+2 cluster on TPC-H SF1 (scripts/k8s-smoke/gen_tpch.py output):
#    pgwire 127.0.0.1:15432, metrics http://127.0.0.1:16432/metrics
cargo build --release --bin arneb
scripts/client-smoke/cluster.sh start /tmp/tpch /tmp/arneb-mb-run

# 2. Metabase and the capture PostgreSQL; seed the reference once
docker compose -p arneb-mb -f scripts/client-smoke/metabase/compose.yml up -d
until pg_isready -h 127.0.0.1 -p 15499 -q; do sleep 1; done
psql -h 127.0.0.1 -p 15499 -U smoke -d arneb -v ON_ERROR_STOP=1 \
    -f scripts/client-smoke/metabase/seed_pgcap.sql

# 3. The probe against Arneb. Metabase runs in Docker, so it reaches the
#    host's cluster as host.docker.internal.
python3 scripts/client-smoke/metabase/probe.py http://127.0.0.1:13300 \
    host.docker.internal 15432 arneb \
    --expect-tables 8 --expect-fields 61 \
    --metrics http://127.0.0.1:16432/metrics \
    --json /tmp/mb-arneb.json | tee /tmp/mb-arneb.txt

# 4. The same probe against PostgreSQL (pgcap, inside the compose network)
python3 scripts/client-smoke/metabase/probe.py http://127.0.0.1:13300 \
    pgcap 5432 arneb --expect-tables 8 --expect-fields 61 | tee /tmp/mb-pg.txt
```

The probe waits for Metabase to become healthy (a minute or two on the
first start). Expected:
- against Arneb, `PROBE ok=38 fail=0 tables=8 fields=61`, with
  `OK   pg_catalog fallbacks delta=0`;
- against PostgreSQL, `PROBE ok=37 fail=0 tables=8 fields=61` (no metrics
  check);
- the same `base_type` and `database_type` for every column on both:
  `diff <(grep ^FIELD /tmp/mb-pg.txt) <(grep ^FIELD /tmp/mb-arneb.txt)`
  prints nothing.

The data results differ between the two (PostgreSQL holds a few rows);
compare the Arneb ones in `/tmp/mb-arneb.json` with DuckDB on the same SF1
data instead.

The probe uses pgJDBC's default settings. pgJDBC asks for binary results
once a statement has run 5 times on a connection, and Arneb answers in the
formats it asks for. Before binary results were supported, the sync lost
the fields of two tables (`fields=53`) unless the database had
`--jdbc-options binaryTransfer=false`.

The probe sets Metabase up with the admin `probe@example.com` and a random
password, which it keeps in `.probe-password` next to `probe.py`
(git-ignored), so later runs against the same Metabase log in with it. Each
run deletes and re-adds its Metabase database (named `arneb@HOST:PORT`), so
every run syncs from scratch.

## Recapture the corpus

After a probe run against `pgcap`:

```bash
docker compose -p arneb-mb -f scripts/client-smoke/metabase/compose.yml \
    logs --no-log-prefix pgcap > /tmp/pgcap.log
python3 scripts/client-smoke/metabase/capture.py /tmp/pgcap.log /tmp/metabase-captured.sql \
    --replay 127.0.0.1:15432
```

`capture.py` keeps the statements of Metabase's connections (the seeding is
dropped), inlines bind parameters, masks the `application_name` and the
`queryHash` comments, and keeps each statement once. `--replay` runs each
one on Arneb over the simple-query protocol and needs psycopg2
(`scripts/client-smoke/requirements.txt`). Metabase's fingerprint queries
carry generated aliases (`"substring2074"`), which differ between runs.

To change the corpus CI replays, replace
`docs/superpowers/specs/2026-11-21-g6-stage-2b2/metabase-captured.sql` and
regenerate `metabase-pg17-expected.json` from `pgcap`: the golden rows are
indexed by statement position.

## Tear down

```bash
docker compose -p arneb-mb -f scripts/client-smoke/metabase/compose.yml down -v
scripts/client-smoke/cluster.sh stop /tmp/arneb-mb-run
rm -f scripts/client-smoke/metabase/.probe-password
```

Metabase keeps its own settings in the container, so after `down` the next
`up` is a fresh, un-set-up Metabase.
