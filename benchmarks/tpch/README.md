# TPC-H Benchmark

Performance comparison of arneb against Trino using TPC-H queries.
Both engines read the same Parquet data from RustFS via Hive Metastore.

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
| `--mode=cold\|warm\|both` (or `BENCH_MODE`) | Default `both`. **cold**: fresh cluster, RustFS restarted, container-VM page cache dropped — every byte re-read from disk. **warm**: fresh cluster plus one untimed run of the same query first. **both**: cold then warm per query into one CSV; the report puts them side by side. Identical for both engines |
| `BENCH_WARMUP=1` | Older spelling of `--mode=warm` |
| `SKIP_SEED=1` | Measure the resident dataset; refused if it is not this scale's data |
| `SKIP_PREFLIGHT=1` | Proceed although a host floor is unmet; the run is recorded as bypassed |

Precedence, highest first: **explicit CLI flag > environment variable > scenario
> built-in default.**

Each run writes three things into `results/`: the raw
`memory_total_<timestamp>.csv`, a `memory_total_<timestamp>.provenance.json`
recording the scale, scenario, mode and bypass marker, and the rendered
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

## Results — SF10, 2026-10-14

Host: 10 CPUs, 15.66 GiB container-runtime memory (OrbStack), 2.7 CPUs per
engine node. Both engines as coordinator + 2 workers, rotated so only the
measured engine is resident, cluster recreated before every measurement.
Dataset: `lineitem` 59,986,052 rows, 2.0 GiB Snappy Parquet on RustFS via HMS.
arneb built from `main` at 1a12cd8 plus #127 and #129 (hash-partitioned
semi/anti joins with probe-side filters, both default on). Produced by
`run_benchmark.sh --scenario=sf10 --mode=both`, all 22 queries in one run.

Latency, cold vs warm, both applied identically to both engines:

- **cold** — cluster recreated, RustFS restarted, container-VM page cache
  dropped, so every byte is re-read from disk.
- **warm** — cluster recreated, then one untimed run of the same query
  before the timed one.

| Query | cold arneb | cold Trino | cold | warm arneb | warm Trino | warm |
|-------|----------:|----------:|-----:|----------:|----------:|-----:|
| q01 | 2043 | 6657 | 3.3x | 1927 | 2899 | 1.5x |
| q02 | 673 | 6256 | 9.3x | 553 | 2241 | 4.0x |
| q03 | 2594 | 8620 | 3.3x | 2472 | 4395 | 1.8x |
| q04 | 1443 | 7541 | 5.2x | 1334 | 3495 | 2.6x |
| q05 | 2284 | 8912 | 3.9x | 2150 | 4178 | 1.9x |
| q06 | 806 | 5214 | 6.5x | 731 | 1906 | 2.6x |
| q07 | 2062 | 8101 | 3.9x | 2091 | 3981 | 1.9x |
| q08 | 3079 | 9791 | 3.2x | 2825 | 5413 | 1.9x |
| q09 | 4947 | 11734 | 2.4x | 4813 | 6638 | 1.4x |
| q10 | 2061 | 9775 | 4.7x | 2034 | 4283 | 2.1x |
| q11 | 770 | 5296 | 6.9x | 668 | 2305 | 3.5x |
| q12 | 1208 | 6339 | 5.2x | 1137 | 2473 | 2.2x |
| q13 | 1648 | 7596 | 4.6x | 1492 | 4125 | 2.8x |
| q14 | 1310 | 6903 | 5.3x | 1185 | 2671 | 2.3x |
| q15 | 1136 | 6910 | 6.1x | 1011 | 3304 | 3.3x |
| q16 | 1109 | 6991 | 6.3x | 989 | 2438 | 2.5x |
| q17 | 3100 | 10280 | 3.3x | 2939 | 5282 | 1.8x |
| q18 | 4154 | 10080 | 2.4x | 4001 | 5272 | 1.3x |
| q19 | 1478 | 7052 | 4.8x | 1344 | 3139 | 2.3x |
| q20 | 1365 | 8568 | 6.3x | 1326 | 4130 | 3.1x |
| q21 | 3346 | 15156 | 4.5x | 3264 | 6894 | 2.1x |
| q22 | 835 | 5517 | 6.6x | 789 | 2137 | 2.7x |

**Geomean: cold 4.6x, warm 2.3x.** All times in milliseconds, one timed run per cell.

Peak memory (simultaneous cluster RSS, warm run):

| Query | arneb MB | Trino MB | ratio | | Query | arneb MB | Trino MB | ratio |
|---|--:|--:|--:|---|---|--:|--:|--:|
| q01 | 382 | 4972 | 0.08x | | q12 | 418 | 4261 | 0.10x |
| q02 | 336 | 3899 | 0.09x | | q13 | 457 | 6053 | 0.08x |
| q03 | 1953 | 6646 | 0.29x | | q14 | 907 | 4795 | 0.19x |
| q04 | 606 | 6591 | 0.09x | | q15 | 515 | 5461 | 0.09x |
| q05 | 1055 | 6563 | 0.16x | | q16 | 548 | 4690 | 0.12x |
| q06 | 354 | 4863 | 0.07x | | q17 | 1692 | 8796 | 0.19x |
| q07 | 1107 | 5444 | 0.20x | | q18 | 2103 | 9273 | 0.23x |
| q08 | 1517 | 7570 | 0.20x | | q19 | 807 | 5347 | 0.15x |
| q09 | 1706 | 9097 | 0.19x | | q20 | 855 | 6697 | 0.13x |
| q10 | 846 | 5292 | 0.16x | | q21 | 1157 | 7629 | 0.15x |
| q11 | 522 | 3380 | 0.15x | | q22 | 327 | 4138 | 0.08x |

**Mean ratio 0.15x**, and arneb is lower on all 22.

### Run-to-run variance

Two full runs on 2026-10-13 (on 142205f) and one on the morning of 2026-10-14
(on 7afe347), all under identical settings, give a first measure of noise.
Per-query speedups moved by a median of 4–6% between consecutive runs (worst:
q16 cold, 29%); the geomeans moved by at most 0.1x (4.3x / 1.9x / 0.17x,
4.2x / 2.0x / 0.18x and 4.3x / 2.0x / 0.16x cold / warm / memory). The one
unstable memory figure is arneb's q18 peak: 2.0, 3.0 and 4.7 GB across those
three, 2.1 GB in this one
([#126](https://github.com/formosa-ventures/arneb/issues/126)). A single-query difference under ~30% in
these tables is therefore not a result.

The exception is q20, which is a real change: 7afe347 (#123) pre-filters the
decorrelated aggregate by the outer semi-join key set, and arneb's q20 went
from 3004 / 3088 ms cold / warm to 1340 / 1257 ms (warm 1.2x → 3.0x).

The semi/anti-join queries are the other real change, from #127 and #129,
against the 7afe347 run: q21 warm 8011 → 3264 ms (0.8x → 2.1x) at 1794 → 1157
MB, q04 warm 1958 → 1334 ms (1.5x → 2.6x) at 1727 → 606 MB, and q22 warm
914 → 789 ms. The other 19 moved within the noise above.

### What the two columns mean

Warming is worth a median 54% to Trino (43–65% across the 22) and 6% to arneb
(−1% to +18%). A recreated
JVM re-JITs; arneb has no comparable start-up cost. So most of the gap between
the cold and warm columns is Trino's start-up, not query execution — **the
warm column is the one to quote.** The cold column is kept because it is what
an operator sees on a cold cluster, and because dropping it after seeing the
warm numbers would be choosing the flattering protocol after the fact.

Storage-side cold barely moves either engine at this scale. An earlier run of
the same build recreated the engines but left RustFS and the page cache warm,
and got geomeans of 4.7x cold / 2.2x warm against this run's 4.6x / 2.3x: on this
host, re-reading 2 GiB of Parquet from local NVMe is cheap next to JVM
start-up. Cold does not reach below the container VM — on macOS the VM's disk
image can still sit in the host's file cache, which only `sudo purge` drops.

### q21

q21 is the four-way correlated `EXISTS` / `NOT EXISTS` self-join over
`lineitem`, the deepest join pipeline in the suite. Warm, arneb lost it in all
five runs through the morning of 2026-10-14 (0.78x and 0.68x on 2026-09-20,
0.80x and 0.84x on 2026-10-13, 0.84x on 7afe347). It now takes 3264 ms against
Trino's 6894 ms (2.1x) at 0.15x the peak memory.

Two changes did it. #127 hash-partitions semi and anti joins on the join key
(`ARNEB_PARTITIONED_SEMI_JOIN`), so the ~98M rows of `l2` and `l3` no longer
land on a single worker. #129 then has each join task drain its small outer
side first and publish that key set to the `lineitem` scans under `EXISTS` and
`NOT EXISTS` (`ARNEB_SEMI_PROBE_FILTER`), so about 1.7M matching rows are
shuffled per worker instead of all 60M and 38M.

### q11

Until 2026-10-13, `q11.sql` hard-coded the threshold fraction `0.0001`. The
spec's fraction is `0.0001 / SF`, so at every scale above SF1 the `HAVING`
filtered out every row and both engines returned zero rows — agreeing on an
empty answer, which is why no gate caught it. The query now divides by
`COUNT(*) FROM supplier` (= 10000 × SF), which is exactly the old `0.0001` at
SF1 and correct at every other scale; at SF10 it returns 8,685 rows on both
engines. q11 numbers from SF10 runs before this date measured the empty
query.

### Correctness

Separate gate, and it passes outright: all 22 queries deterministic run-to-run
and cell-identical to Trino, compared as an order-independent multiset at 6
significant figures. Last run 2026-10-14 at SF10 on the same build as the
numbers above, with the fixed q11 (8,685 rows on both engines).

```bash
python3 benchmarks/tpch/scripts/blast_radius_oracle.py --runs 2
# → 22/22 clean
```

Row-count agreement is not this guarantee — a query can return the right number
of wrong rows, which is exactly the failure this gate was written to catch.

## Manual path (Docker Compose — arneb vs Trino, dual-axis)

The scenario entry point above wraps this. Use it directly when you want to
drive the steps yourself.

The full benchmark environment — RustFS + Hive Metastore + Trino (coordinator +
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
  Trino each as a coordinator + 2 workers) plus RustFS/HMS. Give the Docker VM at
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

# 2. Seed TPC-H data into RustFS via Trino CTAS. TPCH_SF: tiny | sf1 | sf10.
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

- **Never run `docker compose down`** — it removes the RustFS volume and you lose
  the seeded data. Use `docker compose ... stop` to pause; the seed survives.
- `run_memory_bench.sh` brings the arneb + Trino services up/down itself, so you
  can re-run it repeatedly without disturbing RustFS/HMS.
- The optimization gates the measured numbers rest on are **engine defaults**
  since #74; `docker/arneb-bench/docker-compose.bench.yml` no longer has to set
  them, and a run without any `ARNEB_*` environment behaves identically. The
  results those gates produce are in *Results* above: cell-identical to Trino on
  all 22, lower peak memory on all 22, and faster on all 22 warm.
- macOS: `nproc`/`timeout` are absent; the scripts print a warning but fall back
  safely (each arneb node defaults to 2 CPUs — override with `BENCH_NODE_CPUS`).

## Quick Start (single-node)

```bash
# 1. Start infrastructure (RustFS + HMS + Trino)
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
│  RustFS (:9000)  HMS (:9083)   Trino (:8080)     │
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
| `tpch-hive.toml` | Arneb config reading from Hive/RustFS (for benchmarks) |
| `tpch-config.toml` | Arneb config reading local Parquet files (for dev) |
