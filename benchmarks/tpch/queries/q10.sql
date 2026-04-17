-- TPC-H Q10: Returned Item Reporting
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice),
    c_acctbal,
    n_name,
    c_address,
    c_phone
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON l_orderkey = o_orderkey
JOIN nation ON c_nationkey = n_nationkey
WHERE o_orderdate >= DATE '1993-10-01'
    AND o_orderdate < DATE '1994-01-01'
    AND l_returnflag = 'R'
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address
ORDER BY SUM(l_extendedprice) DESC
LIMIT 20
