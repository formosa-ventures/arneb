-- TPC-H Q9: Product Type Profit Measure (simplified)
-- Uses multi-way join, LIKE, GROUP BY
SELECT
    n_name AS nation,
    SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit
FROM part
JOIN lineitem ON p_partkey = l_partkey
JOIN partsupp ON l_suppkey = ps_suppkey AND l_partkey = ps_partkey
JOIN supplier ON s_suppkey = l_suppkey
JOIN orders ON o_orderkey = l_orderkey
JOIN nation ON s_nationkey = n_nationkey
WHERE p_name LIKE '%green%'
GROUP BY n_name
ORDER BY n_name
