#!/usr/bin/env bash
#
# Seed TPC-H data into Hive tables on MinIO via Trino CTAS.
#
# Environment:
#   TPCH_SF       - Scale factor (default: sf1). Options: tiny, sf1, sf10
#   TRINO_SERVER  - Trino server address (default: trino:8080)
#
# Usage:
#   docker compose run --rm tpch-seed
#   TPCH_SF=tiny docker compose run --rm tpch-seed

set -euo pipefail

SF="${TPCH_SF:-sf1}"
SERVER="${TRINO_SERVER:-trino:8080}"

TABLES=(lineitem orders customer part partsupp supplier nation region)

echo "=== TPC-H Seed (${SF}) ==="
echo "Trino server: ${SERVER}"
echo ""

run_sql() {
    trino --server "${SERVER}" --execute "$1"
}

# Drop and recreate schema for idempotency.
echo "[1/3] Dropping existing hive.tpch schema (if any)..."
run_sql "DROP SCHEMA IF EXISTS hive.tpch CASCADE" || true

echo "[2/3] Creating hive.tpch schema..."
run_sql "CREATE SCHEMA hive.tpch WITH (location = 's3a://warehouse/tpch/')"

echo "[3/3] Creating tables via CTAS from tpch.${SF}..."
for table in "${TABLES[@]}"; do
    echo "  Creating ${table}..."
    run_sql "CREATE TABLE hive.tpch.${table} WITH (format = 'PARQUET') AS SELECT * FROM tpch.${SF}.${table}"
done

echo ""
echo "=== TPC-H Seed Complete ==="
echo "Tables created in hive.tpch:"
for table in "${TABLES[@]}"; do
    count=$(trino --server "${SERVER}" --execute "SELECT COUNT(*) FROM hive.tpch.${table}" 2>/dev/null | tr -d '"')
    echo "  ${table}: ${count} rows"
done
