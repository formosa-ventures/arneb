#!/usr/bin/env bash
#
# Seed Iceberg tables (HMS-backed) on MinIO via Trino, for exercising
# Arneb's Iceberg reader (reached through a `hive` catalog via table
# redirection). Covers the shapes the reader must get right:
#
#   ice.nation          plain CTAS (format v2, Parquet)
#   ice.orders          identity-partitioned on o_orderpriority
#   ice.nation_evolved  schema evolution: int -> bigint promotion, column
#                       rename, added column, multiple snapshots
#   ice.orders_deletes  row-level DELETE -> position delete files
#                       (Arneb must reject it, not return deleted rows)
#
# Environment:
#   TPCH_SF       - Scale factor for the orders tables (default: tiny)
#   TRINO_SERVER  - Trino server address (default: trino:8080)
#
# Usage:
#   docker compose run --rm iceberg-seed
#   TPCH_SF=sf1 docker compose run --rm iceberg-seed

set -euo pipefail

SF="${TPCH_SF:-tiny}"
SERVER="${TRINO_SERVER:-trino:8080}"

run_sql() {
    echo "  > $1"
    trino --server "${SERVER}" --execute "$1"
}

echo "=== Iceberg Seed (orders from tpch.${SF}) ==="

for t in nation orders nation_evolved orders_deletes; do
    run_sql "DROP TABLE IF EXISTS iceberg.ice.${t}"
done
run_sql "DROP SCHEMA IF EXISTS iceberg.ice"
run_sql "CREATE SCHEMA iceberg.ice WITH (location = 's3a://warehouse/ice/')"

ORDERS_SELECT="SELECT orderkey AS o_orderkey, custkey AS o_custkey, orderstatus AS o_orderstatus, totalprice AS o_totalprice, orderdate AS o_orderdate, orderpriority AS o_orderpriority, clerk AS o_clerk, shippriority AS o_shippriority, comment AS o_comment FROM tpch.${SF}.orders"

run_sql "CREATE TABLE iceberg.ice.nation AS SELECT nationkey AS n_nationkey, name AS n_name, regionkey AS n_regionkey, comment AS n_comment FROM tpch.tiny.nation"

run_sql "CREATE TABLE iceberg.ice.orders WITH (partitioning = ARRAY['o_orderpriority']) AS ${ORDERS_SELECT}"

# Schema evolution. Snapshot 1 is written with an INTEGER key column and
# the original column names; later snapshots see the evolved schema.
run_sql "CREATE TABLE iceberg.ice.nation_evolved AS SELECT CAST(nationkey AS INTEGER) AS n_nationkey, name AS n_name, regionkey AS n_regionkey FROM tpch.tiny.nation"
run_sql "ALTER TABLE iceberg.ice.nation_evolved ALTER COLUMN n_nationkey SET DATA TYPE BIGINT"
run_sql "ALTER TABLE iceberg.ice.nation_evolved RENAME COLUMN n_name TO nation_name"
run_sql "ALTER TABLE iceberg.ice.nation_evolved ADD COLUMN note VARCHAR"
run_sql "INSERT INTO iceberg.ice.nation_evolved VALUES (100, 'ATLANTIS', 9, 'added after evolution'), (5000000000, 'BIGKEY', 9, 'needs bigint')"

# Row-level delete -> position delete files (merge-on-read).
run_sql "CREATE TABLE iceberg.ice.orders_deletes AS ${ORDERS_SELECT}"
run_sql "DELETE FROM iceberg.ice.orders_deletes WHERE o_orderkey % 7 = 0"

echo ""
echo "=== Iceberg Seed Complete ==="
for t in nation orders nation_evolved orders_deletes; do
    count=$(trino --server "${SERVER}" --execute "SELECT COUNT(*) FROM iceberg.ice.${t}" 2>/dev/null | tr -d '"')
    echo "  ice.${t}: ${count} rows"
done
