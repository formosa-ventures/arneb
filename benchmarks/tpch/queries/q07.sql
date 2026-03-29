-- TPC-H Q7: Volume Shipping (simplified)
-- Uses CASE, multi-way join, GROUP BY
SELECT
    n1.n_name AS supp_nation,
    n2.n_name AS cust_nation,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM supplier
JOIN lineitem ON s_suppkey = l_suppkey
JOIN orders ON o_orderkey = l_orderkey
JOIN customer ON c_custkey = o_custkey
JOIN nation n1 ON s_nationkey = n1.n_nationkey
JOIN nation n2 ON c_nationkey = n2.n_nationkey
WHERE (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
   OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
GROUP BY n1.n_name, n2.n_name
ORDER BY supp_nation, cust_nation
