# TPC-H Benchmark

Performance comparison of arneb against Trino using TPC-H queries.
Both engines read the same Parquet data from MinIO via Hive Metastore.

## Quickstart (scenario — the single entry point)

A **scenario** declares a scale factor, a run plan, and the host floors below
which the run is refused. One command takes it end to end: preflight the host,
bring up the infrastructure, seed the declared scale, measure both engines under
identical container isolation, and print the comparison with the scale it was
measured at in the header.

```bash
./benchmarks/tpch/scripts/run_benchmark.sh --scenario=sf10
```

Declared scenarios live in [`scenarios/`](scenarios/) — currently `sf1` and
`sf10`. Naming one that does not exist aborts before any container starts,
listing the ones that do.

| Flag / variable | Effect |
|---|---|
| `--scenario=<name>` | Load `scenarios/<name>.sh` |
| `--queries="q01 q06"` | Measure a subset instead of all 22 |
| `--skip-trino` | arneb only |
| `--runs=N`, `--sf=<sf>` | Override the scenario's run plan or scale |
| `BENCH_WARMUP=1` | One untimed warm-up per query per engine before the timed run, applied identically to both engines |
| `SKIP_SEED=1` | Measure the resident dataset; refused if it is not this scale's data |
| `SKIP_PREFLIGHT=1` | Proceed although a host floor is unmet; the run is recorded as bypassed |

Precedence, highest first: **explicit CLI flag > environment variable > scenario
> built-in default.**

Each run writes three things into `results/`: the raw
`memory_total_<timestamp>.csv`, a `memory_total_<timestamp>.provenance.json`
recording the scale, scenario, warm-up state and bypass marker, and the rendered
`comparison.md`. The scale travels with the measurement — a CSV without a
sidecar still renders, but its scale prints as `unrecorded` rather than being
assumed.

### Host floors

`sf10` declares 15 GiB of container-runtime memory, 6 CPUs, and 40 GB free disk.
The memory figure is an **observed-working** value, not a measured minimum: full
22-query suites have completed at 15.66 GiB, and no smaller host has been tried.
The 40 GB sizes a *first* run, which builds the engine images from source
(~18 GB of BuildKit cache); the seeded SF10 dataset itself is 2.0 GiB of
Snappy Parquet.

## Results — SF10, 2026-09-20

Host: 10 CPUs, 15.66 GiB container-runtime memory, 2.7 CPUs per engine node.
Both engines as coordinator + 2 workers, rotated so only the measured engine is
resident, cluster restarted before every query. Dataset: `lineitem` 59,986,052
rows, 2.0 GiB Snappy Parquet on MinIO via HMS.

Latency, cold vs warm (warm = one untimed run before the timed one, applied to
both engines):

| Query | cold arneb | cold Trino | cold | warm arneb | warm Trino | warm |
|-------|----------:|----------:|-----:|----------:|----------:|-----:|
| q01 | 1894 | 6283 | 3.3x | 1818 | 3039 | 1.7x |
| q02 | 671 | 5991 | 8.9x | 506 | 2122 | 4.2x |
| q03 | 2543 | 9047 | 3.6x | 2184 | 3832 | 1.8x |
| q04 | 1989 | 7402 | 3.7x | 1952 | 2781 | 1.4x |
| q05 | 2270 | 8887 | 3.9x | 2414 | 4302 | 1.8x |
| q06 | 724 | 4629 | 6.4x | 656 | 1883 | 2.9x |
| q07 | 2028 | 8433 | 4.2x | 1942 | 3858 | 2.0x |
| q08 | 3075 | 9835 | 3.2x | 3124 | 4902 | 1.6x |
| q09 | 4265 | 11779 | 2.8x | 4487 | 6443 | 1.4x |
| q10 | 2056 | 9983 | 4.9x | 2175 | 4183 | 1.9x |
| q11 | 689 | 5218 | 7.6x | 737 | 1942 | 2.6x |
| q12 | 1216 | 6619 | 5.4x | 1081 | 2508 | 2.3x |
| q13 | 1815 | 7738 | 4.3x | 1632 | 4394 | 2.7x |
| q14 | 1195 | 7078 | 5.9x | 1074 | 2879 | 2.7x |
| q15 | 1037 | 6975 | 6.7x | 997 | 3314 | 3.3x |
| q16 | 1089 | 7956 | 7.3x | 1079 | 2616 | 2.4x |
| q17 | 3337 | 11353 | 3.4x | 3032 | 5300 | 1.7x |
| q18 | 4566 | 10798 | 2.4x | 4331 | 5145 | 1.2x |
| q19 | 1453 | 7419 | 5.1x | 1279 | 3239 | 2.5x |
| q20 | 3037 | 8465 | 2.8x | 2939 | 3942 | 1.3x |
| **q21** | 7936 | 15185 | 1.9x | **8262** | **6442** | **0.8x** |
| q22 | 962 | 5659 | 5.9x | 946 | 2136 | 2.3x |

**Geomean: cold 4.4x, warm 2.0x.** All times in milliseconds.

Peak memory (simultaneous cluster RSS, warm run):

| Query | arneb MB | Trino MB | ratio | | Query | arneb MB | Trino MB | ratio |
|---|--:|--:|--:|---|---|--:|--:|--:|
| q01 | 366 | 5468 | 0.07x | | q12 | 330 | 5054 | 0.07x |
| q02 | 261 | 3806 | 0.07x | | q13 | 414 | 6478 | 0.06x |
| q03 | 1918 | 5409 | 0.35x | | q14 | 691 | 4904 | 0.14x |
| q04 | 1704 | 6923 | 0.25x | | q15 | 446 | 7093 | 0.06x |
| q05 | 1048 | 6559 | 0.16x | | q16 | 414 | 4533 | 0.09x |
| q06 | 349 | 4670 | 0.07x | | q17 | 1586 | 7859 | 0.20x |
| q07 | 1217 | 5537 | 0.22x | | q18 | 3827 | 9180 | 0.42x |
| q08 | 1558 | 7380 | 0.21x | | q19 | 642 | 5514 | 0.12x |
| q09 | 1849 | 9338 | 0.20x | | q20 | 1123 | 6683 | 0.17x |
| q10 | 1009 | 5593 | 0.18x | | q21 | 1994 | 8560 | 0.23x |
| q11 | 319 | 3277 | 0.10x | | q22 | 740 | 4710 | 0.16x |

**Mean ratio 0.16x**, and arneb is lower on all 22.

### What the two columns mean

Warming is worth a median 57% to Trino (43–67% across the 22) and 4% to arneb
(−7% to +24%). A restarted JVM re-JITs and re-reads from MinIO; arneb has no
comparable start-up cost. So most of the gap between the cold and warm columns
is Trino's start-up, not query execution — **the warm column is the one to
quote.** The cold column is kept because it is what an operator sees on a
cold cluster, and because dropping it after seeing the warm numbers would be
choosing the flattering protocol after the fact.

### q21

Warm, arneb loses q21: 8262 ms and 9852 ms across two runs, against Trino's
6442 ms and 6739 ms — 0.78x and 0.68x. It reproduces. arneb still uses 0.23x
the peak memory on it. q21 is the four-way correlated `EXISTS` / `NOT EXISTS`
self-join over `lineitem`, the deepest join pipeline in the suite; the
`ARNEB_COMPACT_SEMI_NE` path that makes its memory cheap is not making its
latency competitive once Trino is warm.

### Correctness

Separate gate, and it passes outright: all 22 queries deterministic run-to-run
and cell-identical to Trino, compared as an order-independent multiset at 6
significant figures.

```bash
python3 benchmarks/tpch/scripts/blast_radius_oracle.py --runs 2
# → 22/22 clean
```

Row-count agreement is not this guarantee — a query can return the right number
of wrong rows, which is exactly the failure this gate was written to catch.

## Manual path (Docker Compose — arneb vs Trino, dual-axis)

The scenario entry point above wraps this. Use it directly when you want to
drive the steps yourself.

The full benchmark environment — MinIO + Hive Metastore + Trino (coordinator +
2 workers) **and** arneb (coordinator + 2 workers) — runs entirely from two
compose files, so both engines execute under identical container isolation and
read the same Parquet data. The scenario entry point drives exactly these steps;
run them by hand when you want to vary something a scenario does not express.

**Prerequisites**

- **Docker Engine + Compose v2** (`docker compose`, the plugin subcommand — not
  the legacy `docker-compose` binary).
- **Host tools:** `psql` (PostgreSQL client, used to query arneb) and `python3`
  (the report + oracle scripts use only the standard library — no `pip install`).
- **Docker VM resources:** the benchmark runs six engine containers (arneb and
  Trino each as a coordinator + 2 workers) plus MinIO/HMS. Give the Docker VM at
  least **16 GB RAM** for SF10 — 15.66 GiB is the smallest allocation a full
  22-query suite has completed on. Disk: the seeded SF10 dataset is **2.0 GiB**
  of Snappy Parquet (not the ~10 GB the raw TPC-H text size suggests), but a
  first run also builds the engine images, which costs ~13.5 GB of images plus
  ~18 GB of BuildKit cache — so budget **40 GB** for a first run and far less
  for re-runs. Raise the Docker Desktop / OrbStack memory allocation if workers
  OOM; scale RAM/disk up for SF30. `run_benchmark.sh --scenario=...` checks all
  three of these before it starts anything.
- **First run compiles arneb from source** — the `--build` below runs a Rust
  release build (~10–15 min the first time). It is cached afterwards; drop
  `--build` on later runs.

```bash
# 0. Clone and enter the repo (all commands below run from the repo root).
git clone https://github.com/formosa-ventures/arneb.git
cd arneb

# 1. Bring up the whole env (infra + Trino + arneb). --build compiles the arneb
#    image from the current source; you can drop --build on later runs.
docker compose -f docker-compose.yml \
               -f docker/arneb-bench/docker-compose.bench.yml \
               up -d --build

# 2. Seed TPC-H data into MinIO via Trino CTAS. TPCH_SF: tiny | sf1 | sf10.
TPCH_SF=sf10 docker compose run --rm tpch-seed

# 3. Sanity-check both engines see the data. NOTE the different catalog names:
#    arneb's Hive catalog is `datalake`, Trino's is `hive`.
PGPASSWORD=x psql -h 127.0.0.1 -p 5432 -U arneb -d arneb \
  -c "SELECT COUNT(*) FROM datalake.tpch.lineitem"
docker exec arneb-trino-1 trino --catalog hive --schema tpch \
  --execute "SELECT COUNT(*) FROM lineitem"

# 4. Run the dual-axis benchmark (latency + sim_peak memory, all 22 queries,
#    both engines). QUERIES="q01 q06" restricts the set. Writes a raw CSV to
#    benchmarks/tpch/results/memory_total_<timestamp>.csv (the persisted artifact).
./benchmarks/tpch/scripts/run_memory_bench.sh

# 5. Print the comparison table — per-query latency and peak memory for both
#    engines, the latency speedup (Trino / arneb) and arneb's memory as a
#    fraction of Trino's, plus a geomean speedup. Goes to stdout (not saved);
#    redirect (> report.md) to keep it.
python3 benchmarks/tpch/scripts/bench_report.py \
  "$(ls -t benchmarks/tpch/results/memory_total_*.csv | head -1)"

# (optional) correctness gate — every query is deterministic AND cell-identical
# to Trino. Run with the cluster up.
python3 benchmarks/tpch/scripts/blast_radius_oracle.py --runs 2
```

**Notes**

- **Never run `docker compose down`** — it removes the MinIO volume and you lose
  the seeded data. Use `docker compose ... stop` to pause; the seed survives.
- `run_memory_bench.sh` brings the arneb + Trino services up/down itself, so you
  can re-run it repeatedly without disturbing MinIO/HMS.
- The optimization gates the measured numbers rest on are **engine defaults**
  since #74; `docker/arneb-bench/docker-compose.bench.yml` no longer has to set
  them, and a run without any `ARNEB_*` environment behaves identically. The
  results those gates produce are in *Results* above: cell-identical to Trino on
  all 22, lower peak memory on all 22, and faster on 21 of 22 warm — q21 is the
  exception.
- macOS: `nproc`/`timeout` are absent; the scripts print a warning but fall back
  safely (each arneb node defaults to 2 CPUs — override with `BENCH_NODE_CPUS`).

## Quick Start (single-node)

```bash
# 1. Start infrastructure (MinIO + HMS + Trino)
docker compose up -d

# 2. Seed TPC-H data (SF1, ~1GB, takes ~2 minutes)
docker compose run --rm tpch-seed

# 3. Start arneb with Hive config
cargo run --release --bin arneb -- --config benchmarks/tpch/tpch-hive.toml

# 4. Run benchmark
cd benchmarks/tpch
cargo run --release -- --host 127.0.0.1 --port 5432

# 5. Generate report
python3 scripts/report.py results/arneb_*.json
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│              docker compose up -d                │
│                                                  │
│  MinIO (:9000)   HMS (:9083)   Trino (:8080)    │
│       │               │              │           │
│       └───────┬───────┘              │           │
│               │                      │           │
│    s3://warehouse/tpch/              │           │
│    ├── lineitem/ (Parquet)           │           │
│    ├── orders/                       │           │
│    └── ... (8 tables)                │           │
└─────────────────────────────────────────────────┘
                │                      │
          ┌─────┴─────┐          ┌─────┴─────┐
          │   Arneb    │          │   Trino    │
          │ (pgwire)   │          │ (REST API) │
          │ :5432      │          │ :8080      │
          └────────────┘          └────────────┘
               Same Parquet data → fair comparison
```

## Data Generation

### Via Docker Compose (recommended)

```bash
# Start services
docker compose up -d

# Seed SF1 data (~1GB, ~2 minutes)
docker compose run --rm tpch-seed

# Seed with different scale factor
TPCH_SF=tiny docker compose run --rm tpch-seed   # ~10MB, quick
TPCH_SF=sf10 docker compose run --rm tpch-seed   # ~10GB, slower

# Verify data
docker compose exec trino trino --execute "SELECT COUNT(*) FROM hive.tpch.lineitem"
```

### Local Parquet files (alternative)

For quick local development without the full Hive stack. Parquet files
are generated via a single `docker run` that uses DuckDB's built-in
`tpch` extension and DuckDB's `dbgen`. The script CASTs money columns
to `DOUBLE` so the resulting Parquet schema matches what Trino writes
during the Hive CTAS seed — queries in `queries/` therefore run
identically against both local Parquet and Hive-backed data.

```bash
# 1. Generate SF 0.01 Parquet files (one docker run, ~5s on first run)
./benchmarks/tpch/scripts/generate_sf001.sh

# Larger scale factors:
TPCH_SF=0.1 ./benchmarks/tpch/scripts/generate_sf001.sh
TPCH_SF=1   ./benchmarks/tpch/scripts/generate_sf001.sh

# 2. Start arneb against the local files
cargo run --release --bin arneb -- --config benchmarks/tpch/tpch-sf001.toml
```

> `tpch-sf001.toml` uses repo-relative paths. Run `arneb` with the
> repository root as the working directory, or edit the config to use
> absolute paths.

### TPC-H Tables

| Table | Description | SF1 Rows |
|-------|------------|----------|
| lineitem | Line items | ~6M |
| orders | Orders | ~1.5M |
| customer | Customers | ~150K |
| part | Parts | ~200K |
| partsupp | Part suppliers | ~800K |
| supplier | Suppliers | ~10K |
| nation | Nations | 25 |
| region | Regions | 5 |

## Benchmark Runner

```bash
cd benchmarks/tpch

# Run all queries against arneb
cargo run --release -- --host 127.0.0.1 --port 5432

# Run against Trino (reads from same Hive tables)
cargo run --release -- --engine trino --host 127.0.0.1 --port 8080 --catalog hive --schema tpch

# Run specific queries
cargo run --release -- --queries 1,3,6

# Custom number of runs
cargo run --release -- --num-runs 10 --warm-up 3
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--engine` | arneb | Engine to benchmark (arneb\|trino) |
| `--host` | 127.0.0.1 | Database host |
| `--port` | 5432 | Database port |
| `--catalog` | tpch | Trino catalog name |
| `--schema` | sf1 | Trino schema name |
| `--queries-dir` | benchmarks/tpch/queries | Query SQL files |
| `--num-runs` | 5 | Total runs per query |
| `--warm-up` | 2 | Warm-up runs to discard |
| `--output-dir` | benchmarks/tpch/results | JSON output directory |
| `--queries` | (all) | Comma-separated query numbers |

## Full Benchmark (Arneb vs Trino)

```bash
# Automated: runs both engines and generates report
./benchmarks/tpch/scripts/run_benchmark.sh

# Skip Trino baseline
./benchmarks/tpch/scripts/run_benchmark.sh --skip-trino
```

## Report Generation

```bash
# arneb only
python3 scripts/report.py results/arneb_*.json

# Comparison with Trino
python3 scripts/report.py results/arneb_*.json results/trino_*.json
```

Output format:

```
| Query    | arneb (ms)  |  Trino (ms)  |  Speedup |
|----------|-----------------|--------------|----------|
| q01      |           45.2  |       120.5  |    2.67x |
| q03      |           82.1  |       195.3  |    2.38x |
...

Geometric mean speedup: 2.15x
Queries tested: 7
```

## Queries

Adapted TPC-H queries for arneb's SQL dialect:

| Query | Description | Complexity |
|-------|------------|------------|
| Q1 | Pricing Summary Report | Aggregation + filter |
| Q3 | Shipping Priority | 3-way join + group by |
| Q4 | Order Priority Checking | Aggregation + filter |
| Q5 | Local Supplier Volume | 6-way join + group by |
| Q6 | Forecasting Revenue | Single table aggregate |
| Q10 | Returned Item Reporting | 4-way join + group by |
| Q12 | Shipping Modes | 2-way join + group by |

## Configuration Files

| File | Description |
|------|-------------|
| `tpch-hive.toml` | Arneb config reading from Hive/MinIO (for benchmarks) |
| `tpch-config.toml` | Arneb config reading local Parquet files (for dev) |
