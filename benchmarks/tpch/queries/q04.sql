-- TPC-H Q4: Order Priority Checking (simplified)
SELECT
    o_orderpriority,
    COUNT(*) AS order_count
FROM orders
WHERE o_orderdate >= CAST('1993-07-01' AS DATE)
    AND o_orderdate < CAST('1993-10-01' AS DATE)
GROUP BY o_orderpriority
ORDER BY o_orderpriority
