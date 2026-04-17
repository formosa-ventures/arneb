#!/usr/bin/env bash
set -euo pipefail

# TPC-H Benchmark: Arneb vs Trino (Hive/MinIO)
# ================================================
# Both engines read the same Parquet data from MinIO via HMS.
#
# Prerequisites:
#   docker compose up -d          # start MinIO + HMS + Trino
#   docker compose run tpch-seed  # seed TPC-H SF1 data
#
# Usage:
#   ./scripts/run_benchmark.sh [--skip-trino] [--runs=N]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_DIR="$(cd "$BENCH_DIR/../.." && pwd)"

RESULTS_DIR="$BENCH_DIR/results"
NUM_RUNS="${NUM_RUNS:-5}"
WARM_UP="${WARM_UP:-2}"
SKIP_TRINO="${SKIP_TRINO:-false}"
ARNEB_PORT=5432
TRINO_PORT=8080

# Parse args
for arg in "$@"; do
    case $arg in
        --skip-trino) SKIP_TRINO=true ;;
        --runs=*) NUM_RUNS="${arg#*=}" ;;
        *) echo "Unknown arg: $arg"; exit 1 ;;
    esac
done

echo "============================================"
echo "TPC-H Benchmark: Arneb vs Trino"
echo "============================================"
echo "Data source: Hive/MinIO (hive.tpch.*)"
echo "Runs:        $NUM_RUNS (warm-up: $WARM_UP)"
echo ""

# ------------------------------------------------------------------
# Step 1: Verify Docker Compose services are running
# ------------------------------------------------------------------
echo ">>> Step 1: Checking Docker Compose services..."
cd "$PROJECT_DIR"

if ! docker compose ps --status running 2>/dev/null | grep -q trino; then
    echo "Trino is not running. Start services first:"
    echo "  docker compose up -d"
    echo "  docker compose run --rm tpch-seed"
    exit 1
fi

# Quick check: verify seeded data exists
ROW_COUNT=$(docker compose exec -T trino trino --execute "SELECT COUNT(*) FROM hive.tpch.nation" 2>/dev/null | tr -d '"' || echo "0")
if [ "$ROW_COUNT" = "0" ]; then
    echo "TPC-H data not seeded. Run: docker compose run --rm tpch-seed"
    exit 1
fi
echo "Docker Compose services running. TPC-H data verified."
echo ""

# ------------------------------------------------------------------
# Step 2: Build Arneb and benchmark runner
# ------------------------------------------------------------------
echo ">>> Step 2: Building Arneb and benchmark runner..."
cd "$PROJECT_DIR"
cargo build --release --bin arneb 2>&1 | tail -1
cd "$BENCH_DIR"
cargo build --release 2>&1 | tail -1
echo ""

# ------------------------------------------------------------------
# Step 3: Start Arneb (Hive config) and run benchmark
# ------------------------------------------------------------------
echo ">>> Step 3: Running benchmark against Arneb..."

cd "$PROJECT_DIR"
./target/release/arneb --config "$BENCH_DIR/tpch-hive.toml" &
ARNEB_PID=$!
sleep 3

cd "$BENCH_DIR"
./target/release/tpch-bench \
    --engine arneb \
    --host 127.0.0.1 \
    --port "$ARNEB_PORT" \
    --queries-dir "$BENCH_DIR/queries" \
    --num-runs "$NUM_RUNS" \
    --warm-up "$WARM_UP" \
    --output-dir "$RESULTS_DIR" || true

kill $ARNEB_PID 2>/dev/null || true
wait $ARNEB_PID 2>/dev/null || true
echo ""

# ------------------------------------------------------------------
# Step 4: Run benchmark against Trino (Docker Compose)
# ------------------------------------------------------------------
if [ "$SKIP_TRINO" = "true" ]; then
    echo ">>> Step 4: Skipping Trino benchmark (--skip-trino)"
else
    echo ">>> Step 4: Running benchmark against Trino..."

    cd "$BENCH_DIR"
    ./target/release/tpch-bench \
        --engine trino \
        --host 127.0.0.1 \
        --port "$TRINO_PORT" \
        --catalog hive \
        --schema tpch \
        --queries-dir "$BENCH_DIR/queries" \
        --num-runs "$NUM_RUNS" \
        --warm-up "$WARM_UP" \
        --output-dir "$RESULTS_DIR" || true
    echo ""
fi

# ------------------------------------------------------------------
# Step 5: Generate comparison report
# ------------------------------------------------------------------
echo ">>> Step 5: Generating comparison report..."
cd "$BENCH_DIR"

ARNEB_RESULT=$(ls -t "$RESULTS_DIR"/arneb_*.json 2>/dev/null | head -1)
TRINO_RESULT=$(ls -t "$RESULTS_DIR"/trino_*.json 2>/dev/null | grep -v arneb | head -1)

if [ -n "$ARNEB_RESULT" ] && [ -n "$TRINO_RESULT" ]; then
    python3 "$SCRIPT_DIR/report.py" "$ARNEB_RESULT" "$TRINO_RESULT" | tee "$RESULTS_DIR/comparison.md"
elif [ -n "$ARNEB_RESULT" ]; then
    python3 "$SCRIPT_DIR/report.py" "$ARNEB_RESULT" | tee "$RESULTS_DIR/comparison.md"
else
    echo "No results found to generate report."
fi

echo ""
echo "============================================"
echo "Benchmark complete!"
echo "Results in: $RESULTS_DIR/"
echo "============================================"
