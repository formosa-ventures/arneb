#!/usr/bin/env bash
set -euo pipefail

# TPC-H Benchmark: trino-alt vs Trino
# =====================================
# Prerequisites: Docker, Python 3 (with pyarrow, requests)
#
# Usage: ./scripts/run_benchmark.sh [--scale-factor sf1] [--skip-trino]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_DIR="$(cd "$BENCH_DIR/../.." && pwd)"

SCALE_FACTOR="${SCALE_FACTOR:-sf1}"
DATA_DIR="$BENCH_DIR/data/$SCALE_FACTOR"
RESULTS_DIR="$BENCH_DIR/results"
NUM_RUNS="${NUM_RUNS:-5}"
WARM_UP="${WARM_UP:-2}"
SKIP_TRINO="${SKIP_TRINO:-false}"
TRINO_CONTAINER="tpch-bench-trino"
TRINO_PORT=18080

# Parse args
for arg in "$@"; do
    case $arg in
        --scale-factor=*) SCALE_FACTOR="${arg#*=}"; DATA_DIR="$BENCH_DIR/data/$SCALE_FACTOR" ;;
        --skip-trino) SKIP_TRINO=true ;;
        --runs=*) NUM_RUNS="${arg#*=}" ;;
        *) echo "Unknown arg: $arg"; exit 1 ;;
    esac
done

echo "============================================"
echo "TPC-H Benchmark: trino-alt vs Trino"
echo "============================================"
echo "Scale factor: $SCALE_FACTOR"
echo "Data dir:     $DATA_DIR"
echo "Runs:         $NUM_RUNS (warm-up: $WARM_UP)"
echo ""

# ------------------------------------------------------------------
# Step 1: Generate Parquet data if not present
# ------------------------------------------------------------------
if [ ! -f "$DATA_DIR/lineitem.parquet" ]; then
    echo ">>> Step 1: Generating TPC-H Parquet data..."
    python3 "$SCRIPT_DIR/generate_parquet.py" \
        --scale-factor "$SCALE_FACTOR" \
        --output-dir "$DATA_DIR" \
        --keep-container
    echo ""
else
    echo ">>> Step 1: Data already exists in $DATA_DIR (skipping generation)"
    echo ""
fi

# ------------------------------------------------------------------
# Step 2: Build trino-alt and benchmark runner
# ------------------------------------------------------------------
echo ">>> Step 2: Building trino-alt and benchmark runner..."
cd "$PROJECT_DIR"
cargo build --release --bin trino-alt 2>&1 | tail -1
cd "$BENCH_DIR"
cargo build --release 2>&1 | tail -1
echo ""

# ------------------------------------------------------------------
# Step 3: Start trino-alt and run benchmark
# ------------------------------------------------------------------
echo ">>> Step 3: Running benchmark against trino-alt..."

# Start trino-alt in background
cd "$PROJECT_DIR"
./target/release/trino-alt --config "$BENCH_DIR/tpch-config.toml" &
TRINO_ALT_PID=$!
sleep 2

# Run benchmark
cd "$BENCH_DIR"
./target/release/tpch-bench \
    --engine trino-alt \
    --host 127.0.0.1 \
    --port 5432 \
    --queries-dir "$BENCH_DIR/queries" \
    --num-runs "$NUM_RUNS" \
    --warm-up "$WARM_UP" \
    --output-dir "$RESULTS_DIR" || true

# Stop trino-alt
kill $TRINO_ALT_PID 2>/dev/null || true
wait $TRINO_ALT_PID 2>/dev/null || true
echo ""

# ------------------------------------------------------------------
# Step 4: Run benchmark against Trino (Docker)
# ------------------------------------------------------------------
if [ "$SKIP_TRINO" = "true" ]; then
    echo ">>> Step 4: Skipping Trino benchmark (--skip-trino)"
else
    echo ">>> Step 4: Running benchmark against Trino..."

    # Ensure Trino container is running
    if ! docker ps --format '{{.Names}}' | grep -q "$TRINO_CONTAINER"; then
        echo "Starting Trino Docker container on port $TRINO_PORT..."
        docker rm -f "$TRINO_CONTAINER" 2>/dev/null || true

        # Create Trino config to read from Parquet files via Hive connector
        TRINO_ETC=$(mktemp -d)
        mkdir -p "$TRINO_ETC/catalog"

        # Use tpch connector pointing to the same scale factor
        cat > "$TRINO_ETC/catalog/tpch.properties" <<EOF
connector.name=tpch
tpch.splits-per-node=1
EOF

        docker run -d \
            --name "$TRINO_CONTAINER" \
            -p "$TRINO_PORT:8080" \
            trinodb/trino:latest

        echo -n "Waiting for Trino to start..."
        for i in $(seq 1 30); do
            if curl -sf "http://localhost:$TRINO_PORT/v1/info" | grep -q '"starting":false'; then
                echo " ready!"
                break
            fi
            echo -n "."
            sleep 2
        done
        echo ""
        rm -rf "$TRINO_ETC"
    fi

    cd "$BENCH_DIR"
    ./target/release/tpch-bench \
        --engine trino \
        --host 127.0.0.1 \
        --port "$TRINO_PORT" \
        --catalog tpch \
        --schema "$SCALE_FACTOR" \
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

TRINO_ALT_RESULT=$(ls -t "$RESULTS_DIR"/trino_alt_*.json 2>/dev/null | head -1)
TRINO_RESULT=$(ls -t "$RESULTS_DIR"/trino_*.json 2>/dev/null | grep -v trino_alt | head -1)

if [ -n "$TRINO_ALT_RESULT" ] && [ -n "$TRINO_RESULT" ]; then
    python3 "$SCRIPT_DIR/report.py" "$TRINO_ALT_RESULT" "$TRINO_RESULT" | tee "$RESULTS_DIR/comparison.md"
elif [ -n "$TRINO_ALT_RESULT" ]; then
    python3 "$SCRIPT_DIR/report.py" "$TRINO_ALT_RESULT" | tee "$RESULTS_DIR/comparison.md"
else
    echo "No results found to generate report."
fi

echo ""
echo "============================================"
echo "Benchmark complete!"
echo "Results in: $RESULTS_DIR/"
echo "============================================"
