# TPC-H Comparison Tutorial

This tutorial takes you from a fresh clone of the Arneb repository to a
side-by-side comparison of **Arneb**, **Trino**, and **Apache DataFusion**
on TPC-H SF1, with a Markdown report you can share.

The whole flow takes ~10 minutes after the first build (mostly Trino
container startup and TPC-H data seeding).

Everything runs in a container — the three engines *and* the benchmark
runner. That last part matters: the DataFusion adapter executes
in-process inside the runner binary, so a runner on the host would mean
a native DataFusion competing against containerized rivals, no matter
what isolation the other engines got. Containerizing the runner is what
puts all three under the same CPU and memory limits.

Arneb runs in its **default configuration** here. No tuning options are
enabled, so the numbers you get are the numbers a stock build produces —
which is the whole point of a comparison you can check yourself.

## 1. Prerequisites

You need:

- **Docker** with Docker Compose v2. Everything else runs inside it.
  ~4 GB of free RAM while the stack is up (six engine containers plus
  MinIO and Hive Metastore).
- **About 6 GB of free disk** — TPC-H SF1 data, container images, and
  the Rust build cache used by the image builds.
- **Free TCP ports on localhost**: 8080 (Trino), 9000 and 9001 (MinIO),
  9083 (Hive Metastore). Arneb is *not* published to the host — the
  runner reaches it over the Compose network — so a local PostgreSQL on
  5432 will not collide with this benchmark.

No Rust toolchain is required for the containerized path; the images
build it. You only need one for the native mode described in §4.

> **Which MinIO endpoint applies to you.** The two run modes reach MinIO
> at different addresses, and mixing them produces a connection failure
> with no obvious cause:
>
> | Mode | MinIO endpoint |
> |---|---|
> | Containerized (this tutorial, and where official numbers come from) | `http://minio:9000` on the Compose network |
> | Native (§4, local iteration only) | `http://127.0.0.1:9000` from the host |
>
> **The native mode has a credential trap.** DataFusion's S3 client
> reads `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the
> environment via `AmazonS3Builder::from_env()`. If you have **real AWS
> credentials** exported in that shell, DataFusion silently uses them
> and fails to read MinIO — or worse, reaches real AWS. Before running
> natively:
>
> ```bash
> unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
> ```
>
> The containerized path is structurally immune to this: a container
> only sees the environment explicitly passed to it, and your shell's
> AWS variables are not.

## 2. Reproduce the comparison

One command runs the whole thing from the repository root:

```bash
./benchmarks/tpch/scripts/run_benchmark.sh
```

It starts the engine stack (building images on first run), seeds TPC-H
SF1, runs all three engines from inside the runner container, and writes
the comparison report. The first run takes ~15 minutes, mostly compiling
the Arneb and runner images; later runs reuse the BuildKit cache.

Useful variations:

```bash
# Two engines only.
./benchmarks/tpch/scripts/run_benchmark.sh --engines=arneb,trino

# A couple of queries, fewer runs — handy while iterating.
./benchmarks/tpch/scripts/run_benchmark.sh --queries=1,6 --runs=4

# Reuse data already seeded by a previous run.
SKIP_SEED=1 ./benchmarks/tpch/scripts/run_benchmark.sh
```

Expected output excerpt while it's running:

```
=== Engine: arneb ===
q01: www..... p50=42.3ms p95=51.0ms
q02: www..... p50=18.7ms p95=22.4ms
...
=== Engine: trino ===
q01: www..... p50=128.4ms p95=152.0ms
...
=== Engine: datafusion ===
q01: www..... p50=39.8ms p95=44.6ms
...
```

`w` is a warmup run, `.` is a measured run. The defaults are 3 warmups
and 5 measured runs per query (`--num-runs 8 --warm-up 3`).

When it finishes, you'll have:

```
benchmarks/tpch/results/
├── arneb_20260430_123456.json
├── trino_20260430_123456.json
├── datafusion_20260430_123456.json
└── comparison.md
```

### What the script does, step by step

If you'd rather drive it yourself, these are the same steps:

```bash
COMPOSE="docker compose -f docker-compose.yml \
                       -f docker/tpch-bench/docker-compose.official.yml"

# 1. Start the six engine containers (1 coordinator + 2 workers each).
$COMPOSE up -d --build --wait \
    arneb arneb-worker-1 arneb-worker-2 \
    trino trino-worker-1 trino-worker-2

# 2. Seed ~1 GB of TPC-H SF1 Parquet into MinIO and register it in Hive
#    Metastore. Takes ~2 minutes the first time.
TPCH_SF=sf1 $COMPOSE run --rm tpch-seed

# 3. Run all three engines from inside the runner container.
$COMPOSE run --rm tpch-bench \
    --engines arneb,trino,datafusion \
    --arneb-host arneb --trino-host trino \
    --catalog hive --schema tpch \
    --minio-endpoint http://minio:9000 \
    --queries-dir /queries --output-dir /results

# 4. Render the report.
$COMPOSE run --rm tpch-bench report \
    --dir /results --output /results/comparison.md
```

Verify the seed landed before benchmarking:

```bash
docker compose exec trino trino --execute \
    "SELECT COUNT(*) FROM hive.tpch.lineitem"
```

You should see `"6001215"` (SF1 lineitem row count).

## 3. Read the report

`comparison.md` looks like this (trimmed to 6 representative queries):

```markdown
# TPC-H Comparison Report

**Engines:** arneb, trino, datafusion
**Generated:** 2026-04-30T12:35:01+00:00
**Run plan:** 3 warmup + 5 measurement runs per query

> **Note.** p95/p99 are heuristic at this sample count (5 measurement
> runs). Increase `--num-runs` for a tighter estimate.

## Per-query latency (p50)

| Query | Status | arneb (ms) | trino (ms) | datafusion (ms) | arneb→trino | arneb→datafusion | trino→datafusion |
|---|---|---:|---:|---:|---:|---:|---:|
| q01 | ok | 42.3 | 128.4 | 39.8 | 0.33x | 1.06x | 3.23x |
| q03 | ok | 81.5 | 195.1 | 76.0 | 0.42x | 1.07x | 2.57x |
| q06 | ok | 12.1 | 38.4 | 11.8 | 0.32x | 1.03x | 3.25x |
| q15 | ok/skipped/ok | - | 51.2 | 18.0 | - | - | 2.84x |
| q17 | ok/skipped/ok | - | 412.0 | 102.4 | - | - | 4.02x |
| q21 | ok/skipped/ok | - | 904.1 | 198.7 | - | - | 4.55x |

## Suite summary

| Engine | OK | Failed | Skipped | Geomean p50 (ms) |
|---|---:|---:|---:|---:|
| arneb | 17 | 0 | 5 | 38.7 |
| trino | 22 | 0 | 0 | 144.2 |
| datafusion | 22 | 0 | 0 | 41.1 |

### Pairwise geomean speedup

| Pair | Geomean |
|---|---:|
| arneb → trino | 0.36x |
| arneb → datafusion | 1.05x |
| trino → datafusion | 3.51x |
```

How to read it:

- **Per-query table.** One row per query. The `Status` column shows
  each engine's status if any disagree (e.g., `ok/skipped/ok` means
  Arneb skipped, Trino and DataFusion succeeded). Each `<engine> (ms)`
  column is that engine's **p50 (median)** wall-clock latency over the
  measurement runs. Speedup columns show `left/right` ratios — for
  example, `arneb→datafusion = 1.06x` means DataFusion's p50 was 1.06×
  the value of Arneb's p50, i.e., DataFusion was 6% slower than Arneb
  on that query.
- **Suite summary.** OK/Failed/Skipped counts plus the geometric mean
  of p50s across queries that engine ran successfully. The `Skipped`
  count for Arneb in the example reflects queries the harness declares
  unsupported (correlated subqueries / EXISTS / views) — see
  `benchmarks/tpch/src/skip.rs` for the live list.
- **Pairwise geomean speedup.** Computed only across queries where
  *both* engines in the pair completed successfully. With Arneb's 17
  passes vs Trino's 22, the geomean uses just those 17.
- **Correctness divergences.** When two engines produce
  canonically-different result sets for the same query, that query
  shows up in a separate section with truncated SHA-256 digests and
  per-engine row counts. No section means everyone agrees up to the
  documented float-rounding tolerance (6 decimals).
- **The p95/p99 caveat.** With 5 measured runs, p95 and p99 are
  approximately the max — the report header warns about this. To get
  meaningful tail percentiles, raise `--num-runs` to 20+ at the cost of
  a longer benchmark wall-clock.

## 4. Going further

- **Run a single query** to debug or A/B against a code change:

  ```bash
  cargo run --release -p tpch-bench -- run \
      --engines arneb,datafusion --queries 6
  ```

- **Higher scale factor.** Reseed at SF10 and rerun:

  ```bash
  docker compose down -v   # wipe the SF1 data
  TPCH_SF=sf10 docker compose run --rm tpch-seed
  ./benchmarks/tpch/scripts/run_benchmark.sh
  ```

  Expect Arneb and DataFusion runs to take ~10× longer; Trino's
  per-query overhead dominates at SF1 so the ratio is smaller.

- **Skip an engine** (e.g., when comparing only against DataFusion):

  ```bash
  ./benchmarks/tpch/scripts/run_benchmark.sh --engines=arneb,datafusion
  ```

- **Re-render the report from previous results** without rerunning
  queries:

  ```bash
  docker compose -f docker-compose.yml \
                 -f docker/tpch-bench/docker-compose.official.yml \
                 run --rm tpch-bench report \
                 --dir /results --output /results/comparison.md
  ```

- **Point at a remote Trino.** Override the hostname and port:

  ```bash
  docker compose -f docker-compose.yml \
                 -f docker/tpch-bench/docker-compose.official.yml \
                 run --rm tpch-bench \
                 --engines trino \
                 --trino-host trino.internal --trino-port 8080 \
                 --catalog hive --schema tpch \
                 --queries-dir /queries --output-dir /results
  ```

- **Tighter percentiles.** At the cost of a longer benchmark:

  ```bash
  ./benchmarks/tpch/scripts/run_benchmark.sh --runs=25
  ```

### Running the harness natively (local iteration only)

Official numbers do **not** come from this path — it runs the harness on
the host while Trino stays in a container, so DataFusion and Arneb get
native performance that Trino does not. It is faster to iterate on,
which is the only reason it is documented.

```bash
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

docker compose up -d                       # infra + Trino only
docker compose run --rm tpch-seed

cargo build --release --bin arneb
./target/release/arneb --config benchmarks/tpch/tpch-hive.toml &

# tpch-bench is excluded from the workspace, so build it from its own
# directory — `cargo build -p tpch-bench` from the repo root will not
# resolve it.
cd benchmarks/tpch && cargo build --release && cd ../..

./benchmarks/tpch/target/release/tpch-bench \
    --engines arneb,trino,datafusion \
    --catalog hive --schema tpch \
    --minio-endpoint http://127.0.0.1:9000 \
    --queries-dir benchmarks/tpch/queries \
    --output-dir benchmarks/tpch/results
```

Note the host-oriented MinIO endpoint and the `unset` — both are
specific to this mode. See the callout in §1.

When you're done, tear the stack down to free the RAM:

```bash
docker compose down
```
