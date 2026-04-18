-- TPC-H Q14: Promotion Effect
-- Uses CASE, SUM
SELECT
    100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
        / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM lineitem
JOIN part ON l_partkey = p_partkey
WHERE l_shipdate >= CAST('1995-09-01' AS DATE)
    AND l_shipdate < CAST('1995-10-01' AS DATE)
