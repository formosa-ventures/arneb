-- TPC-H Q4: Order Priority Checking (simplified)
SELECT
    o_orderpriority,
    COUNT(*) AS order_count
FROM orders
WHERE o_orderdate >= '1993-07-01'
    AND o_orderdate < '1993-10-01'
GROUP BY o_orderpriority
ORDER BY o_orderpriority
