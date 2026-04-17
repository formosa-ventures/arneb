#!/usr/bin/env bash
#
# Run TPC-H queries against Trino for baseline comparison.
#
# By default, connects to the Docker Compose Trino instance reading
# from Hive tables on MinIO (same data as arneb).
#
# Prerequisites:
#   docker compose up -d          # start MinIO + HMS + Trino
#   docker compose run tpch-seed  # seed TPC-H data
#
# Usage:
#   ./benchmarks/tpch/scripts/run_trino.sh [host] [port] [catalog]
#

set -euo pipefail

HOST="${1:-localhost}"
PORT="${2:-8080}"
CATALOG="${3:-hive}"
SCHEMA="tpch"
QUERIES_DIR="benchmarks/tpch/queries"
OUTPUT_DIR="benchmarks/tpch/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="${OUTPUT_DIR}/trino_${TIMESTAMP}.json"

NUM_RUNS=5
WARM_UP=2

if ! command -v trino &>/dev/null; then
    echo "ERROR: trino CLI not found. Install with: brew install trino"
    exit 1
fi

echo "TPC-H Trino Baseline"
echo "===================="
echo "Target: ${HOST}:${PORT} catalog=${CATALOG} schema=${SCHEMA}"
echo "Runs: ${NUM_RUNS} (warm-up: ${WARM_UP})"
echo ""

mkdir -p "$OUTPUT_DIR"

# Start JSON output.
echo '{"engine":"trino","host":"'"$HOST"'","port":'"$PORT"',"timestamp":"'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'","queries":[' > "$OUTPUT_FILE"

FIRST=true
for query_file in "$QUERIES_DIR"/q*.sql; do
    query_id=$(basename "$query_file" .sql)
    sql=$(cat "$query_file")

    if [ -z "$sql" ]; then
        continue
    fi

    printf "%-8s" "$query_id:"

    if [ "$FIRST" = false ]; then
        echo ',' >> "$OUTPUT_FILE"
    fi
    FIRST=false

    echo '{"query_id":"'"$query_id"'","query_file":"'"$query_file"'","runs":[' >> "$OUTPUT_FILE"

    RUN_FIRST=true
    for run in $(seq 1 $NUM_RUNS); do
        is_warmup="false"
        if [ "$run" -le "$WARM_UP" ]; then
            is_warmup="true"
        fi

        start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
        rows=$(trino --server "${HOST}:${PORT}" --catalog "$CATALOG" --schema "$SCHEMA" \
            --execute "$sql" 2>/dev/null | wc -l | tr -d ' ')
        end_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
        elapsed=$((end_ms - start_ms))

        if [ "$RUN_FIRST" = false ]; then
            echo ',' >> "$OUTPUT_FILE"
        fi
        RUN_FIRST=false

        echo '{"run_number":'"$run"',"wall_clock_ms":'"$elapsed"',"rows_returned":'"$rows"',"is_warmup":'"$is_warmup"'}' >> "$OUTPUT_FILE"

        if [ "$is_warmup" = "true" ]; then
            printf "w"
        else
            printf "."
        fi
    done

    echo '],"status":"ok"}' >> "$OUTPUT_FILE"
    echo " ${elapsed}ms"
done

echo ']}' >> "$OUTPUT_FILE"

echo ""
echo "Results written to ${OUTPUT_FILE}"
