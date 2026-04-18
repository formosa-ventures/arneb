-- TPC-H Q5: Local Supplier Volume
SELECT
    n_name,
    SUM(l_extendedprice)
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
JOIN supplier ON l_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
WHERE r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS DATE)
    AND o_orderdate < CAST('1995-01-01' AS DATE)
GROUP BY n_name
ORDER BY SUM(l_extendedprice) DESC
