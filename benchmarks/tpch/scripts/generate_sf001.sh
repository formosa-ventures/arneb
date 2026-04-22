#!/usr/bin/env bash
# Generate TPC-H Parquet files locally using DuckDB's tpch extension.
#
# Produces files with the same column types the Hive path receives from
# Trino's tpch connector (bigint keys, double money columns, date32 dates),
# so benchmarks/tpch/queries/* run identically against both local and
# docker-compose Hive sources.
#
# Usage:
#   ./benchmarks/tpch/scripts/generate_sf001.sh           # SF 0.01 (default)
#   TPCH_SF=0.1 ./benchmarks/tpch/scripts/generate_sf001.sh
#
# Requirements: Docker (no Python, no dbgen, no committed seed files).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/data/sf001"
SF="${TPCH_SF:-0.01}"
IMAGE="${DUCKDB_IMAGE:-duckdb/duckdb:latest}"

mkdir -p "$OUT_DIR"

# DuckDB's CALL dbgen(sf=...) materialises TPC-H tables with spec-correct types
# (DECIMAL for money, DATE for dates). The explicit CASTs align money to DOUBLE
# so the Parquet schema matches what Trino writes during the Hive CTAS seed.
docker run --rm \
    -v "$OUT_DIR:/out" \
    --entrypoint duckdb \
    "$IMAGE" \
    -c "
        INSTALL tpch;
        LOAD tpch;
        CALL dbgen(sf=$SF);

        COPY (
            SELECT
                l_orderkey, l_partkey, l_suppkey, l_linenumber,
                l_quantity::DOUBLE      AS l_quantity,
                l_extendedprice::DOUBLE AS l_extendedprice,
                l_discount::DOUBLE      AS l_discount,
                l_tax::DOUBLE           AS l_tax,
                l_returnflag, l_linestatus,
                l_shipdate, l_commitdate, l_receiptdate,
                l_shipinstruct, l_shipmode, l_comment
            FROM lineitem
        ) TO '/out/lineitem.parquet' (FORMAT PARQUET);

        COPY (
            SELECT
                o_orderkey, o_custkey, o_orderstatus,
                o_totalprice::DOUBLE    AS o_totalprice,
                o_orderdate, o_orderpriority, o_clerk,
                o_shippriority::BIGINT  AS o_shippriority,
                o_comment
            FROM orders
        ) TO '/out/orders.parquet' (FORMAT PARQUET);

        COPY (
            SELECT
                c_custkey, c_name, c_address, c_nationkey, c_phone,
                c_acctbal::DOUBLE       AS c_acctbal,
                c_mktsegment, c_comment
            FROM customer
        ) TO '/out/customer.parquet' (FORMAT PARQUET);

        COPY (
            SELECT
                p_partkey, p_name, p_mfgr, p_brand, p_type,
                p_size::BIGINT          AS p_size,
                p_container,
                p_retailprice::DOUBLE   AS p_retailprice,
                p_comment
            FROM part
        ) TO '/out/part.parquet' (FORMAT PARQUET);

        COPY (
            SELECT
                ps_partkey, ps_suppkey,
                ps_availqty::BIGINT     AS ps_availqty,
                ps_supplycost::DOUBLE   AS ps_supplycost,
                ps_comment
            FROM partsupp
        ) TO '/out/partsupp.parquet' (FORMAT PARQUET);

        COPY (
            SELECT
                s_suppkey, s_name, s_address, s_nationkey, s_phone,
                s_acctbal::DOUBLE       AS s_acctbal,
                s_comment
            FROM supplier
        ) TO '/out/supplier.parquet' (FORMAT PARQUET);

        COPY nation TO '/out/nation.parquet' (FORMAT PARQUET);
        COPY region TO '/out/region.parquet' (FORMAT PARQUET);
    "

echo ""
echo "Generated TPC-H SF=$SF Parquet files in $OUT_DIR"
ls -lh "$OUT_DIR"/*.parquet | awk '{print "  " $NF "  " $5}'
