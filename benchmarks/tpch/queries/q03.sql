-- TPC-H Q3: Shipping Priority
SELECT
    l_orderkey,
    SUM(l_extendedprice),
    o_orderdate,
    o_shippriority
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
WHERE c_mktsegment = 'BUILDING'
    AND o_orderdate < '1995-03-15'
    AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY SUM(l_extendedprice) DESC
LIMIT 10
