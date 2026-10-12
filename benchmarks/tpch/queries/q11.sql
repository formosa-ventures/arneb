-- TPC-H Q11: Important Stock Identification (simplified)
-- Uses HAVING, subquery
--
-- The spec's FRACTION is 0.0001 / SF. SF = COUNT(supplier) / 10000, so the
-- threshold is SUM(...) / COUNT(supplier): the query scales itself, and at
-- SF1 it is exactly the 0.0001 it used to hard-code. With the hard-coded
-- 0.0001 every scale above SF1 returned zero rows.
SELECT
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) AS value
FROM partsupp
JOIN supplier ON ps_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
WHERE n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING SUM(ps_supplycost * ps_availqty) > (
    SELECT SUM(ps_supplycost * ps_availqty) / (SELECT COUNT(*) FROM supplier)
    FROM partsupp
    JOIN supplier ON ps_suppkey = s_suppkey
    JOIN nation ON s_nationkey = n_nationkey
    WHERE n_name = 'GERMANY'
)
ORDER BY value DESC
