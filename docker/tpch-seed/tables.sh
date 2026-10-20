# TPC-H table definitions shared by the Hive seed (docker/tpch-seed) and the
# PostgreSQL seed (docker/postgres-tpch-seed), so both stores hold the same
# columns, types and values.
#
# Expects SF (tpch connector schema, e.g. sf1) and SCHEMA (tpch or
# tpch_decimal) to be set by the sourcing script.

# Money/quantity column `$1`, as DECIMAL(15,2) in decimal mode.
num() {
    if [[ "${SCHEMA}" == "tpch_decimal" ]]; then
        echo "CAST($1 AS DECIMAL(15,2))"
    else
        echo "$1"
    fi
}

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
            echo "SELECT suppkey AS s_suppkey, name AS s_name, address AS s_address, nationkey AS s_nationkey, phone AS s_phone, $(num acctbal) AS s_acctbal, comment AS s_comment FROM tpch.${SF}.supplier"
            ;;
        part)
            echo "SELECT partkey AS p_partkey, name AS p_name, mfgr AS p_mfgr, brand AS p_brand, type AS p_type, size AS p_size, container AS p_container, $(num retailprice) AS p_retailprice, comment AS p_comment FROM tpch.${SF}.part"
            ;;
        partsupp)
            echo "SELECT partkey AS ps_partkey, suppkey AS ps_suppkey, availqty AS ps_availqty, $(num supplycost) AS ps_supplycost, comment AS ps_comment FROM tpch.${SF}.partsupp"
            ;;
        customer)
            echo "SELECT custkey AS c_custkey, name AS c_name, address AS c_address, nationkey AS c_nationkey, phone AS c_phone, $(num acctbal) AS c_acctbal, mktsegment AS c_mktsegment, comment AS c_comment FROM tpch.${SF}.customer"
            ;;
        orders)
            echo "SELECT orderkey AS o_orderkey, custkey AS o_custkey, orderstatus AS o_orderstatus, $(num totalprice) AS o_totalprice, orderdate AS o_orderdate, orderpriority AS o_orderpriority, clerk AS o_clerk, shippriority AS o_shippriority, comment AS o_comment FROM tpch.${SF}.orders"
            ;;
        lineitem)
            echo "SELECT orderkey AS l_orderkey, partkey AS l_partkey, suppkey AS l_suppkey, linenumber AS l_linenumber, $(num quantity) AS l_quantity, $(num extendedprice) AS l_extendedprice, $(num discount) AS l_discount, $(num tax) AS l_tax, returnflag AS l_returnflag, linestatus AS l_linestatus, shipdate AS l_shipdate, commitdate AS l_commitdate, receiptdate AS l_receiptdate, shipinstruct AS l_shipinstruct, shipmode AS l_shipmode, comment AS l_comment FROM tpch.${SF}.lineitem"
            ;;
        *)
            echo "Unknown table: $1" >&2
            exit 1
            ;;
    esac
}
