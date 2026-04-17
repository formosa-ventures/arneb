-- TPC-H Q1: Pricing Summary Report
-- Simplified for trino-alt SQL parser
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity),
    SUM(l_extendedprice),
    SUM(l_discount),
    COUNT(*)
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
