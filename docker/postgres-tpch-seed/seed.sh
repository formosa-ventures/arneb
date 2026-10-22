#!/usr/bin/env bash
#
# Seed the TPC-H dimension tables into PostgreSQL via Trino CTAS, for the
# mixed (federated) layout: dimensions in PostgreSQL, facts (orders,
# lineitem) in Hive. See benchmarks/tpch/queries-mixed.
#
# Uses the same column definitions as the Hive seed (docker/tpch-seed/
# tables.sh), so both stores hold identical values. The PostgreSQL schema is
# `<hive schema>_mixed` — the postgres fixture already owns `tpch`.
#
# Environment:
#   TPCH_SF       - Scale factor (default: sf1). Must match the Hive seed.
#   TPCH_DECIMAL  - 1 = DECIMAL(15,2) money columns (tpch_decimal_mixed),
#                   matching TPCH_DECIMAL=1 on the Hive seed (default: 0)
#   TRINO_SERVER  - Trino server address (default: trino:8080)
#
# Usage:
#   docker compose up -d postgres trino
#   docker compose run --rm pg-tpch-seed
#   TPCH_SF=sf10 docker compose run --rm pg-tpch-seed

set -euo pipefail

SF="${TPCH_SF:-sf1}"
SERVER="${TRINO_SERVER:-trino:8080}"
if [[ "${TPCH_DECIMAL:-0}" == "1" ]]; then
    SCHEMA=tpch_decimal
else
    SCHEMA=tpch
fi
PG_SCHEMA="${SCHEMA}_mixed"

# shellcheck source=../tpch-seed/tables.sh
source /tables/tables.sh

TABLES=(region nation supplier customer part partsupp)

primary_key() {
    case "$1" in
        region) echo "r_regionkey" ;;
        nation) echo "n_nationkey" ;;
        supplier) echo "s_suppkey" ;;
        customer) echo "c_custkey" ;;
        part) echo "p_partkey" ;;
        partsupp) echo "ps_partkey, ps_suppkey" ;;
    esac
}

run_sql() {
    # Skip the staging-table copy Trino's JDBC writer does by default: the
    # seed drops and recreates every table anyway.
    trino --server "${SERVER}" --session postgresql.non_transactional_insert=true \
        --execute "$1"
}

# Runs a statement in PostgreSQL itself (DDL Trino has no syntax for).
pg_execute() {
    run_sql "CALL postgresql.system.execute(query => '$1')"
}

echo "=== PostgreSQL TPC-H dimension seed (${SF} → postgresql.${PG_SCHEMA}) ==="

echo "[1/3] Recreating schema ${PG_SCHEMA}..."
pg_execute "DROP SCHEMA IF EXISTS ${PG_SCHEMA} CASCADE"
pg_execute "CREATE SCHEMA ${PG_SCHEMA}"

echo "[2/3] Creating tables via CTAS from tpch.${SF}..."
for table in "${TABLES[@]}"; do
    echo "  ${table}..."
    run_sql "CREATE TABLE postgresql.${PG_SCHEMA}.${table} AS $(select_for_table "${table}")"
done

# Primary keys give the connector its key-range splits; ANALYZE makes
# pg_class.reltuples (the connector's row estimate) reflect the load.
echo "[3/3] Adding primary keys and analyzing..."
for table in "${TABLES[@]}"; do
    pg_execute "ALTER TABLE ${PG_SCHEMA}.${table} ADD PRIMARY KEY ($(primary_key "${table}"))"
    pg_execute "ANALYZE ${PG_SCHEMA}.${table}"
done

echo ""
echo "=== Seed complete: postgresql.${PG_SCHEMA} ==="
for table in "${TABLES[@]}"; do
    count=$(trino --server "${SERVER}" --execute "SELECT COUNT(*) FROM postgresql.${PG_SCHEMA}.${table}" 2>/dev/null | tr -d '"')
    echo "  ${table}: ${count} rows"
done
