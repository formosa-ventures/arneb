-- TPC-H Q2: Minimum Cost Supplier (rewritten as JOIN)
-- Original uses correlated subquery; rewritten for trino-alt compatibility
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone
FROM part
JOIN partsupp ON p_partkey = ps_partkey
JOIN supplier ON s_suppkey = ps_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
JOIN (
    SELECT ps_partkey AS min_partkey, MIN(ps_supplycost) AS min_cost
    FROM partsupp
    JOIN supplier ON s_suppkey = ps_suppkey
    JOIN nation ON s_nationkey = n_nationkey
    JOIN region ON n_regionkey = r_regionkey
    WHERE r_name = 'EUROPE'
    GROUP BY ps_partkey
) AS min_costs ON p_partkey = min_partkey AND ps_supplycost = min_cost
WHERE p_size = 15
    AND p_type LIKE '%BRASS'
    AND r_name = 'EUROPE'
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100
