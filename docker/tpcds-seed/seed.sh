#!/usr/bin/env bash
#
# Seed TPC-DS data into Hive tables on MinIO via Trino CTAS.
#
# Environment:
#   TPCDS_SF      - Scale factor (default: sf1). Options: tiny, sf1, sf10
#   TRINO_SERVER  - Trino server address (default: trino:8080)
#
# Usage:
#   docker compose run --rm tpcds-seed
#   TPCDS_SF=tiny docker compose run --rm tpcds-seed

set -euo pipefail

SF="${TPCDS_SF:-sf1}"
SERVER="${TRINO_SERVER:-trino:8080}"

# 7 fact tables + 17 dimension tables = 24 total
TABLES=(
    # Fact tables
    store_sales catalog_sales web_sales
    store_returns catalog_returns web_returns
    inventory
    # Dimension tables
    call_center catalog_page customer customer_address customer_demographics
    date_dim household_demographics income_band item promotion reason
    ship_mode store time_dim warehouse web_page web_site
)

echo "=== TPC-DS Seed (${SF}) ==="
echo "Trino server: ${SERVER}"
echo "Tables: ${#TABLES[@]}"
echo ""

run_sql() {
    trino --server "${SERVER}" --execute "$1"
}

# Drop and recreate schema for idempotency.
echo "[1/3] Dropping existing hive.tpcds schema (if any)..."
run_sql "DROP SCHEMA IF EXISTS hive.tpcds CASCADE" || true

echo "[2/3] Creating hive.tpcds schema..."
run_sql "CREATE SCHEMA hive.tpcds WITH (location = 's3a://warehouse/tpcds/')"

echo "[3/3] Creating tables via CTAS from tpcds.${SF}..."
for table in "${TABLES[@]}"; do
    echo "  Creating ${table}..."
    run_sql "CREATE TABLE hive.tpcds.${table} WITH (format = 'PARQUET') AS SELECT * FROM tpcds.${SF}.${table}"
done

echo ""
echo "=== TPC-DS Seed Complete ==="
echo "Tables created in hive.tpcds:"
for table in "${TABLES[@]}"; do
    count=$(trino --server "${SERVER}" --execute "SELECT COUNT(*) FROM hive.tpcds.${table}" 2>/dev/null | tr -d '"')
    echo "  ${table}: ${count} rows"
done
