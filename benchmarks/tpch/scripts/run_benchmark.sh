#!/usr/bin/env bash
set -euo pipefail

# TPC-H comparison: arneb vs Trino vs DataFusion
# ==============================================
# Every engine — and the runner itself — executes inside the container stack.
# That is deliberate: the DataFusion adapter runs in-process inside the runner
# binary, so a native runner would mean a native DataFusion no matter what
# isolation the other engines got. Containerizing the runner is what puts all
# three under the same CPU and memory limits.
#
# arneb runs in its DEFAULT configuration here — no ARNEB_* tuning options. This
# is the arrangement official release numbers come from, so the published figure
# matches what a stock build does. For tuning experiments use
# docker/arneb-bench/docker-compose.bench.yml instead.
#
# Usage:
#   ./benchmarks/tpch/scripts/run_benchmark.sh
#   ./benchmarks/tpch/scripts/run_benchmark.sh --engines=arneb,trino
#   ./benchmarks/tpch/scripts/run_benchmark.sh --queries=1,6 --runs=4
#
# Environment:
#   TPCH_SF           scale factor to seed (default sf1)
#   NUM_RUNS          total runs per query, warm-up included (default 8)
#   WARM_UP           leading runs discarded (default 3)
#   BENCH_NODE_CPUS   CPUs per engine node (default 2; 3 nodes per engine)
#   BENCH_RUNNER_CPUS CPUs for the runner, which hosts DataFusion (default 6)
#   SKIP_SEED         set to 1 to reuse already-seeded data

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_DIR="$(cd "$BENCH_DIR/../.." && pwd)"

RESULTS_DIR="$BENCH_DIR/results"
ENGINES="${ENGINES:-arneb,trino,datafusion}"
NUM_RUNS="${NUM_RUNS:-8}"
WARM_UP="${WARM_UP:-3}"
TPCH_SF="${TPCH_SF:-sf1}"
QUERIES=""

for arg in "$@"; do
    case $arg in
        --engines=*) ENGINES="${arg#*=}" ;;
        --queries=*) QUERIES="${arg#*=}" ;;
        --runs=*)    NUM_RUNS="${arg#*=}" ;;
        --warm-up=*) WARM_UP="${arg#*=}" ;;
        -h|--help)   sed -n '2,30p' "$0"; exit 0 ;;
        *) echo "Unknown arg: $arg" >&2; exit 1 ;;
    esac
done

cd "$PROJECT_DIR"

COMPOSE=(docker compose
         -f docker-compose.yml
         -f docker/tpch-bench/docker-compose.official.yml)

ENGINE_SERVICES=(arneb arneb-worker-1 arneb-worker-2 trino trino-worker-1 trino-worker-2)

echo "============================================"
echo "TPC-H comparison (containerized, stock config)"
echo "============================================"
echo "Engines:  $ENGINES"
echo "Scale:    $TPCH_SF"
echo "Runs:     $NUM_RUNS ($WARM_UP warm-up)"
echo ""

# ---------------------------------------------------------------------------
# Step 1: bring up the engine stack
# ---------------------------------------------------------------------------
echo ">>> Step 1: Starting engines (building images as needed)..."
# minio-init creates the `warehouse` bucket and exits. It has to run before the
# seed, and naming only the engine services would skip it — leaving Trino to
# fail later with "Invalid location URI" against a bucket that does not exist.
"${COMPOSE[@]}" up -d --wait minio
"${COMPOSE[@]}" up minio-init
"${COMPOSE[@]}" up -d --build --wait "${ENGINE_SERVICES[@]}"
echo ""

# ---------------------------------------------------------------------------
# Step 2: seed TPC-H data
# ---------------------------------------------------------------------------
if [ "${SKIP_SEED:-0}" = "1" ]; then
    echo ">>> Step 2: Skipping seed (SKIP_SEED=1)"
else
    echo ">>> Step 2: Seeding TPC-H $TPCH_SF into MinIO via Trino..."
    TPCH_SF="$TPCH_SF" "${COMPOSE[@]}" run --rm tpch-seed
fi
echo ""

# ---------------------------------------------------------------------------
# Step 3: run the benchmark from inside the runner container
# ---------------------------------------------------------------------------
echo ">>> Step 3: Running benchmark..."
mkdir -p "$RESULTS_DIR"

RUN_ARGS=(--engines "$ENGINES"
          --arneb-host arneb --arneb-port 5432
          --trino-host trino --trino-port 8080
          --catalog hive --schema tpch
          --minio-endpoint http://minio:9000
          --queries-dir /queries
          --output-dir /results
          --num-runs "$NUM_RUNS"
          --warm-up "$WARM_UP")
[ -n "$QUERIES" ] && RUN_ARGS+=(--queries "$QUERIES")

"${COMPOSE[@]}" run --rm tpch-bench "${RUN_ARGS[@]}"
echo ""

# ---------------------------------------------------------------------------
# Step 4: render the comparison report
# ---------------------------------------------------------------------------
# Rendered by the same Rust binary — the harness has no Python dependency.
echo ">>> Step 4: Generating comparison report..."
"${COMPOSE[@]}" run --rm tpch-bench report --dir /results --output /results/comparison.md

echo ""
echo "Results:  $RESULTS_DIR"
echo "Report:   $RESULTS_DIR/comparison.md"
