#!/usr/bin/env bash
#
# Seed TPC-H data into Hive tables on RustFS via Trino CTAS.
#
# Environment:
#   TPCH_SF       - Scale factor (default: sf1). Options: tiny, sf1, sf10
#   TRINO_SERVER  - Trino server address (default: trino:8080)
#   TPCH_DECIMAL  - 1 = seed hive.tpch_decimal with the spec's DECIMAL(15,2)
#                   money/quantity columns instead of hive.tpch with Trino's
#                   tpch-connector DOUBLEs (default: 0)
#
# Usage:
#   docker compose run --rm tpch-seed
#   TPCH_SF=tiny docker compose run --rm tpch-seed
#   TPCH_DECIMAL=1 docker compose run --rm tpch-seed

set -euo pipefail

SF="${TPCH_SF:-sf1}"
SERVER="${TRINO_SERVER:-trino:8080}"
if [[ "${TPCH_DECIMAL:-0}" == "1" ]]; then
    SCHEMA=tpch_decimal
else
    SCHEMA=tpch
fi

# Column-renaming SELECTs shared with docker/postgres-tpch-seed.
# shellcheck source=tables.sh
source "$(dirname "$0")/tables.sh"

TABLES=(lineitem orders customer part partsupp supplier nation region)

echo "=== TPC-H Seed (${SF}) ==="
echo "Trino server: ${SERVER}"
echo ""

run_sql() {
    trino --server "${SERVER}" --execute "$1"
}

# Fixed writer layout so every seed produces the same file split: with writer
# scaling on (Trino's default) the file count and sizes depend on runtime
# load, and a skewed layout skews arneb's per-worker split assignment — the
# bimodal q18 peak memory in #126 tracked a re-seed, not a code change.
# 2 Trino workers × 4 writers = 8 near-equal files per table.
WRITER_SESSION=(
    --session scale_writers=false
    --session task_scale_writers_enabled=false
    --session task_min_writer_count=4
    --session task_max_writer_count=4
)

run_ctas() {
    trino --server "${SERVER}" "${WRITER_SESSION[@]}" --execute "$1"
}

# Drop and recreate schema for idempotency.
echo "[1/3] Dropping existing hive.${SCHEMA} schema (if any)..."
run_sql "DROP SCHEMA IF EXISTS hive.${SCHEMA} CASCADE" || true

echo "[2/3] Creating hive.${SCHEMA} schema..."
run_sql "CREATE SCHEMA hive.${SCHEMA} WITH (location = 's3a://warehouse/${SCHEMA}/')"

echo "[3/3] Creating tables via CTAS from tpch.${SF}..."
for table in "${TABLES[@]}"; do
    echo "  Creating ${table}..."
    select_sql=$(select_for_table "${table}")
    run_ctas "CREATE TABLE hive.${SCHEMA}.${table} WITH (format = 'PARQUET') AS ${select_sql}"
done

echo ""
echo "=== TPC-H Seed Complete ==="
echo "Tables created in hive.${SCHEMA}:"
for table in "${TABLES[@]}"; do
    count=$(trino --server "${SERVER}" --execute "SELECT COUNT(*) FROM hive.${SCHEMA}.${table}" 2>/dev/null | tr -d '"')
    echo "  ${table}: ${count} rows"
done
