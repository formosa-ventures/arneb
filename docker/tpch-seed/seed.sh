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

# Trino's built-in tpch connector emits unprefixed column names (e.g. `orderkey`,
# `shipdate`), but the TPC-H standard — and the query files under
# benchmarks/tpch/queries — use prefixed names (`l_orderkey`, `l_shipdate`).
# CTAS with explicit aliases so the Hive tables match the standard schema.
select_for_table() {
    case "$1" in
        region)
            echo "SELECT regionkey AS r_regionkey, name AS r_name, comment AS r_comment FROM tpch.${SF}.region"
            ;;
        nation)
            echo "SELECT nationkey AS n_nationkey, name AS n_name, regionkey AS n_regionkey, comment AS n_comment FROM tpch.${SF}.nation"
            ;;
        supplier)
            echo "SELECT suppkey AS s_suppkey, name AS s_name, address AS s_address, nationkey AS s_nationkey, phone AS s_phone, acctbal AS s_acctbal, comment AS s_comment FROM tpch.${SF}.supplier"
            ;;
        part)
            echo "SELECT partkey AS p_partkey, name AS p_name, mfgr AS p_mfgr, brand AS p_brand, type AS p_type, size AS p_size, container AS p_container, retailprice AS p_retailprice, comment AS p_comment FROM tpch.${SF}.part"
            ;;
        partsupp)
            echo "SELECT partkey AS ps_partkey, suppkey AS ps_suppkey, availqty AS ps_availqty, supplycost AS ps_supplycost, comment AS ps_comment FROM tpch.${SF}.partsupp"
            ;;
        customer)
            echo "SELECT custkey AS c_custkey, name AS c_name, address AS c_address, nationkey AS c_nationkey, phone AS c_phone, acctbal AS c_acctbal, mktsegment AS c_mktsegment, comment AS c_comment FROM tpch.${SF}.customer"
            ;;
        orders)
            echo "SELECT orderkey AS o_orderkey, custkey AS o_custkey, orderstatus AS o_orderstatus, totalprice AS o_totalprice, orderdate AS o_orderdate, orderpriority AS o_orderpriority, clerk AS o_clerk, shippriority AS o_shippriority, comment AS o_comment FROM tpch.${SF}.orders"
            ;;
        lineitem)
            echo "SELECT orderkey AS l_orderkey, partkey AS l_partkey, suppkey AS l_suppkey, linenumber AS l_linenumber, quantity AS l_quantity, extendedprice AS l_extendedprice, discount AS l_discount, tax AS l_tax, returnflag AS l_returnflag, linestatus AS l_linestatus, shipdate AS l_shipdate, commitdate AS l_commitdate, receiptdate AS l_receiptdate, shipinstruct AS l_shipinstruct, shipmode AS l_shipmode, comment AS l_comment FROM tpch.${SF}.lineitem"
            ;;
        *)
            echo "Unknown table: $1" >&2
            exit 1
            ;;
    esac
}

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
    select_sql=$(select_for_table "${table}")
    run_sql "CREATE TABLE hive.tpch.${table} WITH (format = 'PARQUET') AS ${select_sql}"
done

echo ""
echo "=== TPC-H Seed Complete ==="
echo "Tables created in hive.tpch:"
for table in "${TABLES[@]}"; do
    count=$(trino --server "${SERVER}" --execute "SELECT COUNT(*) FROM hive.tpch.${table}" 2>/dev/null | tr -d '"')
    echo "  ${table}: ${count} rows"
done
