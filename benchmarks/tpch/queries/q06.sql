-- TPC-H Q6: Forecasting Revenue Change
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= '1994-01-01'
    AND l_shipdate < '1995-01-01'
    AND l_discount >= 0.05
    AND l_discount <= 0.07
    AND l_quantity < 24
