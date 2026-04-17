-- TPC-H Q12: Shipping Modes and Order Priority
SELECT
    l_shipmode,
    COUNT(*) AS order_count
FROM orders
JOIN lineitem ON o_orderkey = l_orderkey
WHERE l_shipmode IN ('MAIL', 'SHIP')
    AND l_receiptdate >= CAST('1994-01-01' AS DATE)
    AND l_receiptdate < CAST('1995-01-01' AS DATE)
GROUP BY l_shipmode
ORDER BY l_shipmode
